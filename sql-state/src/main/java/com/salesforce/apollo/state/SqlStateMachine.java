/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import static com.salesforce.apollo.state.Mutator.NULL_HANDLER;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.zip.GZIPOutputStream;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;

import org.h2.api.ErrorCode;
import org.h2.engine.Session;
import org.h2.jdbc.JdbcConnection;
import org.h2.jdbc.JdbcSQLNonTransientConnectionException;
import org.h2.jdbc.JdbcSQLNonTransientException;
import org.h2.message.DbException;
import org.h2.store.Data;
import org.h2.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.state.proto.Arguments;
import com.salesfoce.apollo.state.proto.Batch;
import com.salesfoce.apollo.state.proto.BatchUpdate;
import com.salesfoce.apollo.state.proto.BatchedTransaction;
import com.salesfoce.apollo.state.proto.Call;
import com.salesfoce.apollo.state.proto.Script;
import com.salesfoce.apollo.state.proto.Statement;
import com.salesfoce.apollo.state.proto.Txn;
import com.salesforce.apollo.choam.CHOAM.TransactionExecutor;
import com.salesforce.apollo.choam.support.CheckpointState;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.QualifiedBase64;
import com.salesforce.apollo.utils.bloomFilters.Hash.DigestHasher;

/**
 * This is ye Jesus Nut of sql state via distribute linear logs. We use H2 as a
 * the local materialized view that is constructed by SQL DML and DDL embedded
 * in the log as SQL statements. Checkpointing is accomplished by SCRIPT command
 * that will output SQL to recreate the state of the DB at the block height
 * (i.e. the checkpoint). Mutation is interactive with the submitter of the
 * transaction statements - i.e. result sets n' multiple result sets n' out
 * params (in calls). This provides a "reasonable" asynchronous interaction with
 * this log based mutation (through consensus).
 * <p>
 * Batch oriented, but low enough latency to make it worth the wait (with the
 * right system wide consensus/distribution, 'natch).
 * 
 * @author hal.hildebrand
 *
 */
public class SqlStateMachine {

    public static class CallResult {
        public final List<Object>    outValues;
        public final List<ResultSet> results;

        public CallResult(List<Object> out, List<ResultSet> results) {
            this.outValues = out;
            this.results = results;
        }

        public <ValueType> ValueType get(int index) {
            @SuppressWarnings("unchecked")
            ValueType v = (ValueType) outValues.get(index);
            return v;
        }
    }

    public record CurrentBlock(Digest hash, long height) {}

    public static class Event {
        public final JsonNode body;
        public final String   discriminator;

        public Event(String discriminator, JsonNode body) {
            this.discriminator = discriminator;
            this.body = body;
        }
    }

    public class TxnExec implements TransactionExecutor {
        @Override
        public void beginBlock(long height, Digest hash) {
            SqlStateMachine.this.beginBlock(height, hash);
        }

        @Override
        public void execute(Transaction tx, @SuppressWarnings("rawtypes") CompletableFuture onComplete) {
            boolean closed;
            try {
                closed = connection.isClosed();
            } catch (SQLException e) {
                return;
            }
            if (closed) {
                return;
            }
            Txn txn;
            try {
                txn = Txn.parseFrom(tx.getContent());
            } catch (InvalidProtocolBufferException e) {
                log.warn("invalid txn", e);
                return;
            }
            SqlStateMachine.this.execute(txn, onComplete);

        }

        @Override
        public void genesis(long height, Digest hash, List<Transaction> initialization) {
            begin(height, hash);
            initializeState();
            updateCurrentBlock(height, hash);
            for (Transaction txn : initialization) {
                execute(txn, null);
            }
        }
    }

    private static class EventTrampoline {
        private List<Event> pending = new CopyOnWriteArrayList<>();

        public void publish(Event event) {
            pending.add(event);
        }

        @SuppressWarnings("unused")
        private void evaluate(Map<String, BiConsumer<String, JsonNode>> handlers) {
            try {
                for (Event event : pending) {
                    BiConsumer<String, JsonNode> handler = handlers.get(event.discriminator);
                    if (handler != null) {
                        try {
                            handler.accept(event.discriminator, event.body);
                        } catch (Throwable e) {
                            log.trace("handler failed for {}", e);
                        }
                    }
                }
            } finally {
                pending.clear();
            }
        }
    }

    private static final String                    CREATE_ALIAS_APOLLO_INTERNAL_PUBLISH       = String.format("CREATE ALIAS __APOLLO_INTERNAL__.PUBLISH FOR \"%s.publish\"",
                                                                                                              SqlStateMachine.class.getCanonicalName());
    private static final String                    CREATE_SCHEMA_APOLLO_INTERNAL              = "CREATE SCHEMA __APOLLO_INTERNAL__";
    private static final String                    CREATE_TABLE_APOLLO_INTERNAL_CURRENT_BLOCK = "CREATE TABLE __APOLLO_INTERNAL__.CURRENT_BLOCK(_U INT, HEIGHT INT8, HASH VARCHAR(256))";
    private static final String                    CREATE_TABLE_APOLLO_INTERNAL_TRAMPOLINE    = "CREATE TABLE __APOLLO_INTERNAL__.TRAMPOLINE(ID INT AUTO_INCREMENT, CHANNEL VARCHAR(255), BODY JSON)";
    private static final String                    DELETE_FROM_APOLLO_INTERNAL_TRAMPOLINE     = "DELETE FROM __APOLLO_INTERNAL__.TRAMPOLINE";
    private static final RowSetFactory             factory;
    private static final Logger                    log                                        = LoggerFactory.getLogger(SqlStateMachine.class);
    private static final ObjectMapper              MAPPER                                     = new ObjectMapper();
    private static final String                    PUBLISH_INSERT                             = "INSERT INTO __APOLLO_INTERNAL__.TRAMPOLINE(CHANNEL, BODY) VALUES(?1, ?2)";
    private static final ThreadLocal<SecureRandom> secureRandom                               = new ThreadLocal<>();
    private static final String                    SELECT_FROM_APOLLO_INTERNAL_TRAMPOLINE     = "select * from __APOLLO_INTERNAL__.TRAMPOLINE";
    private static final String                    UPDATE_CURRENT_BLOCK                       = "MERGE INTO __APOLLO_INTERNAL__.CURRENT_BLOCK(_U, HEIGHT, HASH) KEY(_U) VALUES(1, ?1, ?2)";

    static {
        try {
            factory = RowSetProvider.newFactory();
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot create row set factory", e);
        }
    }

    public static SecureRandom getSecureRandom() {
        return secureRandom.get();
    }

    public static boolean publish(Connection connection, String channel, String jsonBody) {
        try (PreparedStatement statement = connection.prepareStatement(PUBLISH_INSERT)) {
            statement.setString(1, channel);
            statement.setString(2, jsonBody);
            statement.execute();
        } catch (SQLException e) {
            throw new IllegalStateException("Unable to publish: " + channel, e);
        }
        return true;
    }

    private final LoadingCache<String, CallableStatement> callCache;
    private final File                                    checkpointDirectory;
    private final ScriptCompiler                          compiler     = new ScriptCompiler();
    private final JdbcConnection                          connection;
    private final AtomicReference<CurrentBlock>           currentBlock = new AtomicReference<>();
    private final AtomicReference<SecureRandom>           entropy      = new AtomicReference<>();
    private final TxnExec                                 executor     = new TxnExec();
    private final Properties                              info;
    private final LoadingCache<String, PreparedStatement> psCache;
    private final EventTrampoline                         trampoline   = new EventTrampoline();
    private final String                                  url;

    public SqlStateMachine(String url, Properties info, File cpDir) {
        this.url = url;
        this.info = info;
        this.checkpointDirectory = cpDir;
        if (checkpointDirectory.exists()) {
            if (!checkpointDirectory.isDirectory()) {
                throw new IllegalArgumentException("Must be a directory: " + checkpointDirectory.getAbsolutePath());
            }
        } else {
            if (!checkpointDirectory.mkdirs()) {
                throw new IllegalArgumentException("Cannot create checkpoint directory: "
                + checkpointDirectory.getAbsolutePath());
            }
        }
        try {
            connection = new JdbcConnection(url, info);
        } catch (SQLException e) {
            throw new IllegalStateException("Unable to create connection using " + url, e);
        }
        try {
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            log.error("Unable to set autocommit to false", e);
        }
        callCache = constructCallCache();
        psCache = constructPsCache();
    }

    public void close() {
        psCache.invalidateAll();
        callCache.invalidateAll();
        try {
            connection.rollback();
        } catch (SQLException e1) {
        }
        try {
            connection.close();
        } catch (SQLException e) {
        }
    }

    public BiConsumer<Long, CheckpointState> getBootstrapper() {
        return (height, state) -> {
            String rndm = UUID.randomUUID().toString();
            try (java.sql.Statement statement = connection.createStatement()) {
                File temp = new File(checkpointDirectory, String.format("checkpoint-%s--%s.sql", height, rndm));
                try {
                    state.assemble(temp);
                } catch (IOException e) {
                    log.error("unable to assemble checkpoint: {} into: {}", height, temp, e);
                    return;
                }
                try {
                    log.error("Restoring checkpoint: {} ", height);
                    statement.execute(String.format("RUNSCRIPT FROM '%s'", temp.getAbsolutePath()));
                    log.error("Restored from checkpoint: {}", height);
                    statement.close();
                } catch (SQLException e) {
                    log.error("unable to restore checkpoint: {}", height, e);
                    return;
                }
            } catch (SQLException e) {
                log.error("unable to restore from checkpoint: {}", height, e);
                return;
            }
        };
    }

    public Function<Long, File> getCheckpointer() {
        return height -> {
            String rndm = UUID.randomUUID().toString();
            try (java.sql.Statement statement = connection.createStatement()) {
                File temp = new File(checkpointDirectory, String.format("checkpoint-%s--%s.sql", height, rndm));
                try {
                    statement.execute(String.format("SCRIPT DROP TO '%s'", temp.getAbsolutePath()));
                    statement.close();
                } catch (SQLException e) {
                    log.error("unable to checkpoint: {}", height, e);
                    return null;
                }
                if (!temp.exists()) {
                    log.error("Written file does not exist: {}", temp.getAbsolutePath());
                    return null;
                }
                File checkpoint = new File(checkpointDirectory, String.format("checkpoint-%s--%s.gzip", height, rndm));
                try (FileInputStream fis = new FileInputStream(temp);
                FileOutputStream fos = new FileOutputStream(checkpoint);
                GZIPOutputStream gzos = new GZIPOutputStream(fos);) {

                    byte[] buffer = new byte[6 * 1024];
                    for (int read = fis.read(buffer); read > 0; read = fis.read(buffer)) {
                        gzos.write(buffer, 0, read);
                    }
                    gzos.finish();
                    gzos.flush();
                    fos.flush();
                } catch (IOException e) {
                    log.error("unable to checkpoint: {}", height, e);
                } finally {
                    temp.delete();
                }
                assert checkpoint.exists() : "Written file does not exist: " + checkpoint.getAbsolutePath();
                return checkpoint;
            } catch (SQLException e) {
                log.error("unable to checkpoint: {}", height, e);
                return null;
            }
        };
    }

    public CurrentBlock getCurrentBlock() {
        return currentBlock.get();
    }

    public TxnExec getExecutor() {
        return executor;
    }

    public Connection newConnection() {
        try {
            return new JdbcConnection(url, info);
        } catch (SQLException e) {
            throw new IllegalStateException("cannot get JDBC connection", e);
        }
    }

    // Test accessible
    void initializeState() {
        java.sql.Statement statement = null;

        SecureRandom prev = secureRandom.get();
        secureRandom.set(entropy.get());
        try {
            statement = connection.createStatement();
            statement.execute(CREATE_SCHEMA_APOLLO_INTERNAL);
            statement.execute(CREATE_TABLE_APOLLO_INTERNAL_CURRENT_BLOCK);
            statement.execute(CREATE_TABLE_APOLLO_INTERNAL_TRAMPOLINE);
            statement.execute(CREATE_ALIAS_APOLLO_INTERNAL_PUBLISH);
        } catch (SQLException e) {
            throw new IllegalStateException("unable to initialize db state", e);
        } finally {
            secureRandom.set(prev);
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                }
            }
        }
    }

    private int[] acceptBatch(Batch batch) throws SQLException {
        try (java.sql.Statement exec = connection.createStatement()) {
            for (String sql : batch.getStatementsList()) {
                exec.addBatch(sql);
            }
            return exec.executeBatch();
        }
    }

    private List<Object> acceptBatchTransaction(BatchedTransaction txns) throws Exception {
        List<Object> results = new ArrayList<Object>();
        for (int i = 0; i < txns.getTransactionsCount(); i++) {
            try {
                results.add(SqlStateMachine.this.execute(txns.getTransactions(i)));
            } catch (Throwable e) {
                throw new Mutator.BatchedTransactionException(i, e);
            }
        }
        return results;
    }

    private int[] acceptBatchUpdate(BatchUpdate batchUpdate) throws SQLException {
        PreparedStatement exec;
        try {
            exec = psCache.get(batchUpdate.getSql());
        } catch (ExecutionException e) {
            if (e.getCause() instanceof SQLException) {
                throw (SQLException) e.getCause();
            }
            throw new IllegalStateException("Unable to get prepared update statement: " + batchUpdate.getSql(), e);
        }
        try {
            for (Arguments args : batchUpdate.getBatchList()) {
                int i = 0;
                for (ByteString argument : args.getArgsList()) {
                    Data data = Data.create(NULL_HANDLER, argument.toByteArray(), false);
                    setArgument(exec, i++, data.readValue());
                }
                exec.addBatch();
            }
            return exec.executeBatch();
        } finally {
            try {
                exec.clearBatch();
                exec.clearParameters();
            } catch (JdbcSQLNonTransientException | JdbcSQLNonTransientConnectionException e) {
                // ignore
            }
        }
    }

    private CallResult acceptCall(Call call) throws SQLException {
        List<ResultSet> results = new ArrayList<>();
        CallableStatement exec;
        try {
            exec = callCache.get(call.getSql());
        } catch (ExecutionException e) {
            if (e.getCause() instanceof SQLException) {
                throw (SQLException) e.getCause();
            }
            throw new IllegalStateException("Unable to get callable statement: " + call.getSql(), e);
        }
        try {
            int i = 0;
            for (ByteString argument : call.getArgsList()) {
                Data data = Data.create(NULL_HANDLER, argument.toByteArray(), false);
                setArgument(exec, i++, data.readValue());
            }
            for (int p = 0; p < call.getOutParametersCount(); p++) {
                exec.registerOutParameter(p, call.getOutParameters(p));
            }
            List<Object> out = new ArrayList<>();

            switch (call.getExecution()) {
            case EXECUTE:
                exec.execute();
                break;
            case QUERY:
                exec.executeQuery();
                break;
            case UPDATE:
                exec.executeUpdate();
                break;
            default:
                log.debug("Invalid statement execution enum: {}", call.getExecution());
                return new CallResult(out, results);
            }
            CachedRowSet rowset = factory.createCachedRowSet();

            rowset.populate(exec.getResultSet());
            results.add(rowset);

            while (exec.getMoreResults()) {
                rowset = factory.createCachedRowSet();
                rowset.populate(exec.getResultSet());
                results.add(rowset);
            }
            for (int j = 0; j < call.getOutParametersCount(); j++) {
                out.add(exec.getObject(j));
            }
            return new CallResult(out, results);
        } finally {
            try {
                exec.clearBatch();
                exec.clearParameters();
            } catch (JdbcSQLNonTransientException | JdbcSQLNonTransientConnectionException e) {
                // ignore
            }
        }
    }

    private List<ResultSet> acceptPreparedStatement(Statement statement) throws SQLException {
        List<ResultSet> results = new ArrayList<>();
        PreparedStatement exec;
        try {
            exec = psCache.get(statement.getSql());
        } catch (ExecutionException e) {
            if (e.getCause() instanceof SQLException) {
                throw (SQLException) e.getCause();
            }
            throw new IllegalStateException("Unable to create prepared statement: " + statement.getSql(), e);
        }
        try {
            switch (statement.getExecution()) {
            case EXECUTE:
                exec.execute();
                break;
            case QUERY:
                exec.executeQuery();
                break;
            case UPDATE:
                exec.executeUpdate();
                break;
            default:
                log.debug("Invalid statement execution enum: {}", statement.getExecution());
                return Collections.emptyList();
            }
            CachedRowSet rowset = factory.createCachedRowSet();

            rowset.populate(exec.getResultSet());
            results.add(rowset);

            while (exec.getMoreResults()) {
                rowset = factory.createCachedRowSet();
                rowset.populate(exec.getResultSet());
                results.add(rowset);
            }
            return results;
        } finally {
            try {
                exec.clearBatch();
                exec.clearParameters();
            } catch (JdbcSQLNonTransientException | JdbcSQLNonTransientConnectionException e) {
                // ignore
            }
        }
    }

    private Object acceptScript(Script script) throws SQLException {

        Object instance;

        Value[] args = new Value[script.getArgsCount()];
        int i = 1;
        for (ByteString argument : script.getArgsList()) {
            Data data = Data.create(NULL_HANDLER, argument.toByteArray(), false);
            args[i++] = data.readValue();
        }

        try {
            Class<?> clazz = compiler.getClass(script.getClassName(), script.getSource(), getClass().getClassLoader());
            instance = clazz.getConstructor().newInstance();
        } catch (DbException e) {
            throw e;
        } catch (Exception e) {
            throw DbException.get(ErrorCode.SYNTAX_ERROR_1, e, script.getSource());
        }

        Method call = null;
        String callName = script.getMethod();
        for (Method m : instance.getClass().getDeclaredMethods()) {
            if (callName.equals(m.getName())) {
                call = m;
                break;
            }
        }

        if (call == null) {
            throw DbException.get(ErrorCode.SYNTAX_ERROR_1,
                                  new IllegalArgumentException("Must contain invocation method named: " + callName
                                  + "(...)"), script.getSource());
        }

        Object returnValue = new JavaMethod(call).getValue(instance, getSession(), args);
        if (returnValue instanceof ResultSet) {
            CachedRowSet rowset = factory.createCachedRowSet();

            rowset.populate((ResultSet) returnValue);
            return rowset;
        }
        return returnValue;
    }

    private void begin(long height, Digest hash) {
        getSession().getRandom().setSeed(new DigestHasher(hash, height).identityHash());
        try {
            SecureRandom secureEntropy = SecureRandom.getInstance("SHA1PRNG");
            secureEntropy.setSeed(hash.getBytes());
            entropy.set(secureEntropy);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("No SHA1PRNG available", e);
        }
        currentBlock.set(new CurrentBlock(hash, height));
    }

    private void beginBlock(long height, Digest hash) {
        begin(height, hash);
        updateCurrentBlock(height, hash);
    }

    private void commit() {
        try {
            publishEvents();
            connection.commit();
        } catch (SQLException e) {
            log.trace("unable to commit connection", e);
        }
    }

    @SuppressWarnings("unchecked")
    private void complete(@SuppressWarnings("rawtypes") CompletableFuture onCompletion, Object results) {
        if (onCompletion == null) {
            return;
        }
        onCompletion.complete(results);
    }

    private LoadingCache<String, CallableStatement> constructCallCache() {
        return CacheBuilder.newBuilder().removalListener(new RemovalListener<String, CallableStatement>() {
            @Override
            public void onRemoval(RemovalNotification<String, CallableStatement> notification) {
                try {
                    notification.getValue().close();
                } catch (JdbcSQLNonTransientException | JdbcSQLNonTransientConnectionException e) {
                    // ignore
                } catch (SQLException e) {
                    log.error("Error closing cached statment: {}", notification.getKey(), e);
                }
            }
        }).build(new CacheLoader<String, CallableStatement>() {
            @Override
            public CallableStatement load(String sql) throws Exception {
                return connection.prepareCall(sql);
            }
        });
    }

    private LoadingCache<String, PreparedStatement> constructPsCache() {
        return CacheBuilder.newBuilder().removalListener(new RemovalListener<String, PreparedStatement>() {
            @Override
            public void onRemoval(RemovalNotification<String, PreparedStatement> notification) {
                try {
                    notification.getValue().close();
                } catch (JdbcSQLNonTransientException | JdbcSQLNonTransientConnectionException e) {
                    // ignore
                } catch (SQLException e) {
                    log.error("Error closing cached statment: {}", notification.getKey(), e);
                }
            }
        }).build(new CacheLoader<String, PreparedStatement>() {

            @Override
            public PreparedStatement load(String sql) throws Exception {
                return connection.prepareStatement(sql);
            }

        });
    }

    private void exception(@SuppressWarnings("rawtypes") CompletableFuture onCompletion, Throwable e) {
        if (onCompletion != null) {
            onCompletion.completeExceptionally(e);
        }
    }

    private Object execute(Txn txn) throws Exception {
        try {
            return switch (txn.getExecutionCase()) {
            case BATCH -> acceptBatch(txn.getBatch());
            case BATCHUPDATE -> acceptBatchUpdate(txn.getBatchUpdate());
            case CALL -> acceptCall(txn.getCall());
            case SCRIPT -> acceptScript(txn.getScript());
            case STATEMENT -> acceptPreparedStatement(txn.getStatement());
            case BATCHED -> acceptBatchTransaction(txn.getBatched());
            default -> null;
            };
        } catch (Throwable th) {
            return th;
        }
    }

    private void execute(Txn tx, @SuppressWarnings("rawtypes") CompletableFuture onCompletion) {
        SecureRandom prev = SqlStateMachine.secureRandom.get();
        SqlStateMachine.secureRandom.set(SqlStateMachine.this.entropy.get());
        try {
            Object results = switch (tx.getExecutionCase()) {
            case BATCH -> SqlStateMachine.this.acceptBatch(tx.getBatch());
            case CALL -> acceptCall(tx.getCall());
            case BATCHUPDATE -> SqlStateMachine.this.acceptBatchUpdate(tx.getBatchUpdate());
            case STATEMENT -> SqlStateMachine.this.acceptPreparedStatement(tx.getStatement());
            case SCRIPT -> SqlStateMachine.this.acceptScript(tx.getScript());
            case BATCHED -> acceptBatchTransaction(tx.getBatched());
            default -> null;
            };
            this.complete(onCompletion, results);
        } catch (JdbcSQLNonTransientConnectionException e) {
            // ignore
        } catch (Exception e) {
            rollback();
            exception(onCompletion, e);
        } finally {
            secureRandom.set(prev);
            commit();
        }
    }

    private Session getSession() {
        return (Session) connection.getSession();
    }

    private void publishEvents() {
        PreparedStatement exec;
        try {
            exec = psCache.get(SELECT_FROM_APOLLO_INTERNAL_TRAMPOLINE);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof JdbcSQLNonTransientException ||
                e.getCause() instanceof JdbcSQLNonTransientConnectionException) {
                return;
            }
            log.error("Error publishing events", e.getCause());
            throw new IllegalStateException("Cannot publish events", e.getCause());
        }
        try (ResultSet events = exec.executeQuery()) {
            while (events.next()) {
                String channel = events.getString(2);
                JsonNode body;
                try {
                    body = MAPPER.readTree(events.getString(3));
                } catch (JsonProcessingException e) {
                    log.warn("cannot deserialize event: {} channel: {}", events.getInt(1), channel, e);
                    continue;
                }
                trampoline.publish(new Event(channel, body));
            }
        } catch (JdbcSQLNonTransientException | JdbcSQLNonTransientConnectionException e) {
            return;
        } catch (SQLException e) {
            log.error("Error retrieving published events", e.getCause());
            throw new IllegalStateException("Cannot retrieve published events", e.getCause());
        }

        try {
            exec = psCache.get(DELETE_FROM_APOLLO_INTERNAL_TRAMPOLINE);
        } catch (ExecutionException e) {
            log.error("Error publishing events", e.getCause());
            throw new IllegalStateException("Cannot publish events", e.getCause());
        }
        try {
            exec.execute();
        } catch (JdbcSQLNonTransientException | JdbcSQLNonTransientConnectionException e) {
            return;
        } catch (SQLException e) {
            log.error("Error cleaning published events", e);
            throw new IllegalStateException("Cannot clean published events", e);
        }

    }

    private void rollback() {
        try {
            connection.rollback();
        } catch (SQLException e) {
            log.trace("unable to rollback connection", e);
        }
    }

    private void setArgument(PreparedStatement exec, int i, Value value) {
        try {
            value.set(exec, i + 1);
        } catch (SQLException e) {
            throw new IllegalArgumentException("Illegal argument value: " + value);
        }
    }

    private void updateCurrentBlock(long height, Digest hash) {
        PreparedStatement exec;
        try {
            exec = psCache.get(UPDATE_CURRENT_BLOCK);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Unable to get prepared update statement: " + UPDATE_CURRENT_BLOCK, e);
        }
        try {
            exec.setLong(1, height);
            exec.setString(2, QualifiedBase64.qb64(hash));
            exec.execute();
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot update the CURRENT BLOCK", e);
        } finally {
            try {
                exec.clearBatch();
                exec.clearParameters();
            } catch (SQLException e) {
                // ignore
            }
        }
        commit();
    }
}
