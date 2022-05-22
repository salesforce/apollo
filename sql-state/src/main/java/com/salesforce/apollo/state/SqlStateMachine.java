/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

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
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.zip.GZIPOutputStream;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;

import org.joou.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.state.proto.Arguments;
import com.salesfoce.apollo.state.proto.Batch;
import com.salesfoce.apollo.state.proto.BatchUpdate;
import com.salesfoce.apollo.state.proto.BatchedTransaction;
import com.salesfoce.apollo.state.proto.Call;
import com.salesfoce.apollo.state.proto.ChangeLog;
import com.salesfoce.apollo.state.proto.Drop;
import com.salesfoce.apollo.state.proto.Migration;
import com.salesfoce.apollo.state.proto.Script;
import com.salesfoce.apollo.state.proto.Statement;
import com.salesfoce.apollo.state.proto.Txn;
import com.salesforce.apollo.choam.CHOAM.TransactionExecutor;
import com.salesforce.apollo.choam.Session;
import com.salesforce.apollo.choam.support.CheckpointState;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.QualifiedBase64;
import com.salesforce.apollo.state.Mutator.BatchedTransactionException;
import com.salesforce.apollo.state.liquibase.LiquibaseConnection;
import com.salesforce.apollo.state.liquibase.MigrationAccessor;
import com.salesforce.apollo.state.liquibase.NullResourceAccessor;
import com.salesforce.apollo.state.liquibase.ReplicatedChangeLogHistoryService;
import com.salesforce.apollo.state.liquibase.ThreadLocalScopeManager;
import com.salesforce.apollo.utils.DelegatingJdbcConnector;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.bloomFilters.Hash.DigestHasher;

import deterministic.org.h2.api.ErrorCode;
import deterministic.org.h2.engine.SessionLocal;
import deterministic.org.h2.jdbc.JdbcConnection;
import deterministic.org.h2.jdbc.JdbcSQLNonTransientConnectionException;
import deterministic.org.h2.jdbc.JdbcSQLNonTransientException;
import deterministic.org.h2.message.DbException;
import deterministic.org.h2.util.BlockClock;
import deterministic.org.h2.util.CloseWatcher;
import deterministic.org.h2.util.DateTimeUtils;
import deterministic.org.h2.util.JdbcUtils;
import deterministic.org.h2.util.MathUtils;
import deterministic.org.h2.value.Value;
import liquibase.CatalogAndSchema;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.changelog.ChangeLogHistoryServiceFactory;
import liquibase.database.core.H2Database;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.util.StringUtil;

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

    public record Current(ULong height, Digest blkHash) {}

    public record Event(String discriminator, JsonNode body) {}

    public static class ReadOnlyConnector extends DelegatingJdbcConnector {

        private final CloseWatcher watcher;

        public ReadOnlyConnector(Connection wrapped, deterministic.org.h2.engine.Session session) throws SQLException {
            super(wrapped);
            wrapped.setReadOnly(true);
            wrapped.setAutoCommit(false);
            this.watcher = CloseWatcher.register(this, session, false);
        }

        @Override
        public void close() throws SQLException {
            CloseWatcher.unregister(watcher);
        }

        @Override
        public boolean getAutoCommit() throws SQLException {
            return false;
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return false;
        }

        @Override
        public void setAutoCommit(boolean autoCommit) throws SQLException {
            if (autoCommit) {
                throw new SQLException("Cannot set autocommit on this connection");
            }
        }

        @Override
        public void setReadOnly(boolean readOnly) throws SQLException {
            if (!readOnly) {
                throw new SQLException("This is a read only connection");
            }
        }

        @Override
        public void setTransactionIsolation(int level) throws SQLException {
            throw new SQLException("Cannot set transaction isolation level on this connection");
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new SQLException("Cannot unwrap: " + iface.getCanonicalName() + "on th connection");
        }
    }

    public class TxnExec implements TransactionExecutor {
        @Override
        public void beginBlock(ULong height, Digest hash) {
            SqlStateMachine.this.beginBlock(height, hash);
        }

        @Override
        public void endBlock(ULong height, Digest hash) {
            SqlStateMachine.this.endBlock(height, hash);
        }

        @Override
        public void execute(int index, Digest txnHash, Transaction tx,
                            @SuppressWarnings("rawtypes") CompletableFuture onComplete) {
            boolean closed;
            try {
                closed = connection().isClosed();
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
                log.warn("invalid txn: {}", tx, e);
                onComplete.completeExceptionally(e);
                return;
            }
            withContext(() -> {
                SqlStateMachine.this.execute(index, txnHash, txn, onComplete);
            });

        }

        @Override
        public void genesis(Digest hash, List<Transaction> initialization) {
            begin(ULong.valueOf(0), hash);
            withContext(() -> {
                initializeState();
                updateCurrent(ULong.valueOf(0), hash, -1, Digest.NONE);
            });
            int i = 0;
            for (Transaction txn : initialization) {
                execute(i, Digest.NONE, txn, null);
            }
            log.debug("Genesis executed on: {}", url);
        }
    }

    @FunctionalInterface
    private interface CheckedFunction<A, B> {
        B apply(A a) throws SQLException;
    }

    private static class EventTrampoline {
        private volatile Consumer<List<Event>> handler;
        private volatile List<Event>           pending = new ArrayList<>();

        public void deregister() {
            handler = null;
        }

        private void evaluate() {
            try {
                if (handler != null) {
                    try {
                        handler.accept(pending);
                    } catch (Throwable e) {
                        log.trace("handler failed for {}", e);
                    }
                }
            } finally {
                pending = new ArrayList<>();
            }
        }

        private void publish(Event event) {
            pending.add(event);
        }

        private void register(Consumer<List<Event>> handler) {
            this.handler = handler;
        }
    }

    private record baseAndAccessor(Liquibase liquibase, MigrationAccessor ra) implements AutoCloseable {
        @Override
        public void close() throws LiquibaseException {
            ra.clone();
            liquibase.close();
        }
    }

    private static final String        CREATE_ALIAS_APOLLO_INTERNAL_PUBLISH   = String.format("CREATE ALIAS apollo_internal.publish FOR \"%s.publish\"",
                                                                                              SqlStateMachine.class.getCanonicalName());
    private static final String        DELETE_FROM_APOLLO_INTERNAL_TRAMPOLINE = "DELETE FROM apollo_internal.trampoline";
    private static final RowSetFactory factory;
    private static final Logger        log                                    = LoggerFactory.getLogger(SqlStateMachine.class);
    private static final ObjectMapper  MAPPER                                 = new ObjectMapper();
    private static final String        PUBLISH_INSERT                         = "INSERT INTO apollo_internal.trampoline(channel, body) VALUES(?1, ?2 FORMAT JSON)";
    private static final String        SELECT_FROM_APOLLO_INTERNAL_TRAMPOLINE = "SELECT * FROM apollo_internal.trampoline";
    private static final String        SQL_STATE_INTERNAL                     = "/sql-state/internal.xml";
    private static final String        UPDATE_CURRENT                         = "MERGE INTO apollo_internal.current(_u, height, block_hash, transaction, transaction_hash) KEY(_U) VALUES(1, ?1, ?2, ?3, ?4)";

    static {
        ThreadLocalScopeManager.initialize();
    }
    static {
        try {
            factory = RowSetProvider.newFactory();
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot create row set factory", e);
        }
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

    private final File                          checkpointDirectory;
    private final BlockClock                    clock          = new BlockClock();
    private final ScriptCompiler                compiler       = new ScriptCompiler();
    private final JdbcConnection                connection;
    private final AtomicReference<Current>      currentBlock   = new AtomicReference<>();
    private PreparedStatement                   deleteEvents;
    private final AtomicReference<SecureRandom> entropy        = new AtomicReference<>();
    private final AtomicReference<Current>      executingBlock = new AtomicReference<>();
    private final TxnExec                       executor       = new TxnExec();
    private PreparedStatement                   getEvents;
    private final EventTrampoline               trampoline     = new EventTrampoline();
    private PreparedStatement                   updateCurrent;
    private final String                        url;

    public SqlStateMachine(String url, Properties info, File cpDir) {
        this.url = url;
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
        connection = withContext(() -> {
            JdbcConnection c;
            try {
                c = new JdbcConnection(url, info, "", "", false);
            } catch (SQLException e) {
                throw new IllegalStateException("Unable to create connection using " + url, e);
            }
            try {
                c.setAutoCommit(false);
            } catch (SQLException e) {
                log.error("Unable to set autocommit to false", e);
            }
            return c;
        });
    }

    public void close() {
        try {
            connection().rollback();
        } catch (SQLException e1) {
        }
        try {
            connection().close();
        } catch (SQLException e) {
        }
    }

    public void deregisterHandler() {
        trampoline.deregister();
    }

    public BiConsumer<ULong, CheckpointState> getBootstrapper() {
        return (height, state) -> {
            String rndm = UUID.randomUUID().toString();
            try (java.sql.Statement statement = connection().createStatement()) {
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

    public Function<ULong, File> getCheckpointer() {
        return height -> {
            String rndm = Long.toString(Entropy.nextBitsStreamLong());
            try (java.sql.Statement statement = connection().createStatement()) {
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

    public Current getCurrentBlock() {
        return currentBlock.get();
    }

    public TxnExec getExecutor() {
        return executor;
    }

    public Mutator getMutator(Session session) {
        return new Mutator(session, getSession());
    }

    public Connection newConnection() {
        try {
            return new ReadOnlyConnector(new JdbcConnection(getSession(), "", url), getSession());
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    public void register(Consumer<List<Event>> handler) {
        trampoline.register(handler);
    }

    // Test accessible
    void commit() {
        try {
            publishEvents();
            connection().commit();
            trampoline.evaluate();
        } catch (SQLException e) {
            log.trace("unable to commit connection", e);
        }
    }

    // Test accessible
    JdbcConnection connection() {
        return connection;
    }

    SessionLocal getSession() {
        return (SessionLocal) connection().getSession();
    }

    // Test accessible
    void initializeState() {
        java.sql.Statement statement = null;

        final var database = new H2Database();
        database.setConnection(new liquibase.database.jvm.JdbcConnection(new LiquibaseConnection(connection())));
        try (Liquibase liquibase = new Liquibase(SQL_STATE_INTERNAL, new ClassLoaderResourceAccessor(), database)) {
            liquibase.update((String) null);
            statement = connection().createStatement();
            statement.execute(CREATE_ALIAS_APOLLO_INTERNAL_PUBLISH);
            deleteEvents = connection.prepareStatement(DELETE_FROM_APOLLO_INTERNAL_TRAMPOLINE);
            getEvents = connection.prepareStatement(SELECT_FROM_APOLLO_INTERNAL_TRAMPOLINE);
            updateCurrent = connection.prepareStatement(UPDATE_CURRENT);
        } catch (SQLException e) {
            throw new IllegalStateException("unable to initialize db state", e);
        } catch (LiquibaseException e) {
            throw new IllegalStateException("unable to initialize db state", e);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                }
            }
        }
        log.debug("Initialized state on: {}", url);
    }

    private int[] acceptBatch(Batch batch) throws SQLException {
        try (java.sql.Statement exec = connection().createStatement()) {
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
        return execute(batchUpdate.getSql(), exec -> {
            try {
                for (Arguments args : batchUpdate.getBatchList()) {
                    int i = 1;
                    for (Value v : new StreamTransfer(getSession()).read(args.getArgs())) {
                        setArgument(exec, i++, v);
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
        });
    }

    private CallResult acceptCall(Call call) throws SQLException {
        return call(call.getSql(), exec -> {
            List<ResultSet> results = new ArrayList<>();
            try {
                int p = 1;
                for (int t : call.getOutParametersList()) {
                    exec.registerOutParameter(p++, t);
                }
                for (Value v : new StreamTransfer(call.getArgs().getVersion(), getSession()).read(call.getArgs()
                                                                                                      .getArgs())) {
                    setArgument(exec, p++, v);
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
                for (int j = 1; j <= call.getOutParametersCount(); j++) {
                    out.add(exec.getObject(j));
                }

                CachedRowSet rowset = factory.createCachedRowSet();

                rowset.populate(exec.getResultSet());
                results.add(rowset);

                while (exec.getMoreResults()) {
                    rowset = factory.createCachedRowSet();
                    rowset.populate(exec.getResultSet());
                    results.add(rowset);
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
        });
    }

    private Boolean acceptMigration(Migration migration) throws SQLException {
        try {
            ChangeLogHistoryServiceFactory.getInstance().register(new ReplicatedChangeLogHistoryService());
            switch (migration.getCommandCase()) {
            case CHANGELOGSYNC:
                changeLogSync(migration.getChangelogSync());
                break;
            case CLEARCHECKSUMS:
                clearCheckSums();
                break;
            case DROP:
                dropAll(migration.getDrop());
                break;
            case ROLLBACK:
                rollback(migration.getRollback());
                break;
            case TAG:
                tag(migration.getTag());
                break;
            case UPDATE:
                update(migration.getUpdate());
                break;
            default:
                break;
            }
        } catch (Throwable e) {
            throw new SQLException("Exception during migration", e);
        }
        return Boolean.TRUE;
    }

    private List<ResultSet> acceptPreparedStatement(Statement statement) throws SQLException {
        return execute(statement.getSql(), exec -> {
            List<ResultSet> results = new ArrayList<>();
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
        });
    }

    private Object acceptScript(Script script) throws SQLException {

        Object instance;

        Value[] args = new StreamTransfer(script.getArgs().getVersion(), getSession()).read(script.getArgs().getArgs());
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

    private void begin(ULong height, Digest blkHash) {
        final var session = getSession();
        if (session == null) {
            return;
        }
        executingBlock.set(new Current(height, blkHash));
        session.getRandom().setSeed(new DigestHasher(blkHash, height.longValue()).identityHash());
        try {
            SecureRandom secureEntropy = SecureRandom.getInstance("SHA1PRNG");
            secureEntropy.setSeed(blkHash.getBytes());
            entropy.set(secureEntropy);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("No SHA1PRNG available", e);
        }
        clock.incrementHeight();
    }

    private void beginBlock(ULong height, Digest hash) {
        begin(height, hash);
        withContext(() -> {
            updateCurrent(height, hash, -1, Digest.NONE);
            log.debug("Begin block: {} hash: {} on: {}", height, hash, url);
        });
    }

    private <T> T call(String sql, CheckedFunction<CallableStatement, T> execution) throws SQLException {
        CallableStatement cs = null;
        try {
            cs = connection().prepareCall(sql);
            return execution.apply(cs);
        } finally {
            if (cs != null) {
                try {
                    cs.close();
                } catch (SQLException e) {
                }
            }
        }
    }

    private void changeLogSync(ChangeLog changeLog) throws LiquibaseException {
        try (var l = liquibase(changeLog)) {
            l.liquibase.changeLogSync(changeLog.getTag(), new Contexts(changeLog.getContext()),
                                      new LabelExpression(changeLog.getLabels()));
        } catch (IOException e) {
            throw new LiquibaseException("unable to sync", e);
        }
    }

    private void clearCheckSums() throws LiquibaseException {
        final var database = new H2Database();
        database.setConnection(new liquibase.database.jvm.JdbcConnection(new LiquibaseConnection(connection())));
        try (Liquibase liquibase = new Liquibase((String) null, new NullResourceAccessor(), database)) {
            liquibase.clearCheckSums();
        }
    }

    @SuppressWarnings("unchecked")
    private void complete(@SuppressWarnings("rawtypes") CompletableFuture onCompletion, Object results) {
        if (onCompletion == null) {
            return;
        }
        onCompletion.complete(results);
    }

    private void dropAll(Drop drop) throws LiquibaseException {
        final var database = new H2Database();
        database.setConnection(new liquibase.database.jvm.JdbcConnection(new LiquibaseConnection(connection())));
        if (StringUtil.trimToNull(drop.getSchemas()) != null) {
            List<String> schemaNames = StringUtil.splitAndTrim(drop.getSchemas(), ",");
            List<CatalogAndSchema> schemas = new ArrayList<>();
            for (String name : schemaNames) {
                schemas.add(new CatalogAndSchema(drop.getCatalog(), name));
            }
            try (Liquibase liquibase = new Liquibase((String) null, new NullResourceAccessor(), database)) {
                liquibase.dropAll(schemas.toArray(new CatalogAndSchema[schemas.size()]));
            }
        }
    }

    private void endBlock(ULong height, Digest blkHash) {
        currentBlock.set(new Current(height, blkHash));
    }

    private void exception(@SuppressWarnings("rawtypes") CompletableFuture onCompletion, Throwable e) {
        if (onCompletion != null) {
            var completed = onCompletion.completeExceptionally(e);
            assert completed : "Invalid state";
        }
    }

    private void execute(int index, Digest txnHash, Txn tx,
                         @SuppressWarnings("rawtypes") CompletableFuture onCompletion) {
        log.debug("executing: {}", tx.getExecutionCase());
        var executing = executingBlock.get();
        updateCurrent(executing.height, executing.blkHash, index, txnHash);

        clock.incrementTxn();

        try {
            Object results = switch (tx.getExecutionCase()) {
            case BATCH -> SqlStateMachine.this.acceptBatch(tx.getBatch());
            case CALL -> acceptCall(tx.getCall());
            case BATCHUPDATE -> SqlStateMachine.this.acceptBatchUpdate(tx.getBatchUpdate());
            case STATEMENT -> SqlStateMachine.this.acceptPreparedStatement(tx.getStatement());
            case SCRIPT -> SqlStateMachine.this.acceptScript(tx.getScript());
            case BATCHED -> acceptBatchTransaction(tx.getBatched());
            case MIGRATION -> acceptMigration(tx.getMigration());
            default -> null;
            };
            this.complete(onCompletion, results);
        } catch (JdbcSQLNonTransientConnectionException e) {
            // ignore
        } catch (Exception e) {
            rollback();
            exception(onCompletion, e);
            if (e instanceof BatchedTransactionException bte) {
                log.error("error executing: {}: {}", tx.getExecutionCase(), bte.getCause().getMessage());
            } else {
                log.error("error executing: {}", tx.getExecutionCase(), e);
            }
        } finally {
            commit();
        }
    }

    private <T> T execute(String sql, CheckedFunction<PreparedStatement, T> execution) throws SQLException {
        PreparedStatement ps = null;
        try {
            ps = connection().prepareStatement(sql);
            return execution.apply(ps);
        } catch (JdbcSQLNonTransientException e) {
            return null;
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                }
            }
        }
    }

    private Object execute(Txn txn) throws Exception {
        return switch (txn.getExecutionCase()) {
        case BATCH -> acceptBatch(txn.getBatch());
        case BATCHUPDATE -> acceptBatchUpdate(txn.getBatchUpdate());
        case CALL -> acceptCall(txn.getCall());
        case SCRIPT -> acceptScript(txn.getScript());
        case STATEMENT -> acceptPreparedStatement(txn.getStatement());
        case BATCHED -> acceptBatchTransaction(txn.getBatched());
        case MIGRATION -> acceptMigration(txn.getMigration());
        default -> null;
        };
    }

    private baseAndAccessor liquibase(ChangeLog changeLog) throws IOException {
        final var database = new H2Database();
        database.setConnection(new liquibase.database.jvm.JdbcConnection(new LiquibaseConnection(connection())));
        var ra = new MigrationAccessor(changeLog.getResources());
        return new baseAndAccessor(new Liquibase(changeLog.getRoot(), ra, database), ra);
    }

    private void publishEvents() {
        try (ResultSet events = getEvents.executeQuery()) {
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
        } catch (SQLException e) {
            log.error("Error retrieving published events", e.getCause());
            throw new IllegalStateException("Cannot retrieve published events", e.getCause());
        }

        try {
            deleteEvents.execute();
        } catch (JdbcSQLNonTransientException | JdbcSQLNonTransientConnectionException e) {
        } catch (SQLException e) {
            log.error("Error cleaning published events", e);
            throw new IllegalStateException("Cannot clean published events", e);
        }

    }

    private void rollback() {
        try {
            if (connection().isClosed()) {
                return;
            }
        } catch (SQLException e1) {
            return; // I'm done with this shit
        }
        try {
            connection().rollback();
        } catch (SQLException e) {
            log.trace("unable to rollback connection", e);
        }
    }

    private void rollback(ChangeLog changeLog) throws LiquibaseException {
        try (var l = liquibase(changeLog)) {
            if (changeLog.hasCount()) {
                l.liquibase.rollback(changeLog.getCount(), new Contexts(changeLog.getContext()),
                                     new LabelExpression(changeLog.getLabels()));
            } else {
                l.liquibase.rollback(changeLog.getTag(), new Contexts(changeLog.getContext()),
                                     new LabelExpression(changeLog.getLabels()));
            }
        } catch (IOException e) {
            throw new LiquibaseException("unable to update", e);
        }
    }

    private void setArgument(PreparedStatement exec, int i, Value value) {
        try {
            JdbcUtils.set(exec, i, value, connection());
        } catch (SQLException e) {
            throw new IllegalArgumentException("Illegal argument value: " + value, e);
        }
    }

    private void tag(String tag) throws LiquibaseException {
        final var database = new H2Database();
        database.setConnection(new liquibase.database.jvm.JdbcConnection(new LiquibaseConnection(connection())));
        try (Liquibase liquibase = new Liquibase((String) null, new NullResourceAccessor(), database)) {
            liquibase.tag(tag);
        }
    }

    private void update(ChangeLog changeLog) throws LiquibaseException {
        try (var l = liquibase(changeLog)) {
            if (changeLog.hasCount()) {
                l.liquibase.update(changeLog.getCount(), new Contexts(changeLog.getContext()),
                                   new LabelExpression(changeLog.getLabels()));
            } else {
                l.liquibase.update(changeLog.getTag(), new Contexts(changeLog.getContext()),
                                   new LabelExpression(changeLog.getLabels()));
            }
        } catch (IOException e) {
            throw new LiquibaseException("unable to update", e);
        }
    }

    private void updateCurrent(ULong height, Digest blkHash, int txn, Digest txnHash) {
        try {
            updateCurrent.setLong(1, height.longValue());
            updateCurrent.setString(2, QualifiedBase64.qb64(blkHash));
            updateCurrent.setLong(3, txn);
            updateCurrent.setString(4, QualifiedBase64.qb64(txnHash));
            updateCurrent.execute();
        } catch (SQLException e) {
            log.debug("Failure to update current block: {} hash: {} txn: {} hash: {} on: {}", height, blkHash, txn,
                      txnHash, url);
            throw new IllegalStateException("Cannot update the CURRENT BLOCK on: " + url, e);
        }
        commit();
    }

    private <T> T withContext(Callable<T> action) {
        SecureRandom prev = MathUtils.SECURE_RANDOM.get();
        MathUtils.SECURE_RANDOM.set(entropy.get());
        BlockClock prevClock = DateTimeUtils.CLOCK.get();
        DateTimeUtils.CLOCK.set(clock);
        try {
            return action.call();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            MathUtils.SECURE_RANDOM.set(prev);
            DateTimeUtils.CLOCK.set(prevClock);
        }
    }

    private void withContext(Runnable action) {
        withContext(() -> {
            action.run();
            return null;
        });
    }
}
