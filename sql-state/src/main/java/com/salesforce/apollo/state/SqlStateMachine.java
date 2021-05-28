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
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;

import org.h2.api.ErrorCode;
import org.h2.api.Interval;
import org.h2.api.TimestampWithTimeZone;
import org.h2.engine.Session;
import org.h2.jdbc.JdbcConnection;
import org.h2.jdbc.JdbcSQLNonTransientConnectionException;
import org.h2.jdbc.JdbcSQLNonTransientException;
import org.h2.message.DbException;
import org.h2.store.Data;
import org.h2.store.DataHandler;
import org.h2.util.JSR310;
import org.h2.util.JSR310Utils;
import org.h2.value.Value;
import org.h2.value.ValueArray;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueByte;
import org.h2.value.ValueBytes;
import org.h2.value.ValueDate;
import org.h2.value.ValueDecimal;
import org.h2.value.ValueDouble;
import org.h2.value.ValueFloat;
import org.h2.value.ValueInt;
import org.h2.value.ValueInterval;
import org.h2.value.ValueLong;
import org.h2.value.ValueNull;
import org.h2.value.ValueShort;
import org.h2.value.ValueString;
import org.h2.value.ValueStringFixed;
import org.h2.value.ValueTime;
import org.h2.value.ValueTimestamp;
import org.h2.value.ValueTimestampTimeZone;
import org.h2.value.ValueUuid;
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
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.salesfoce.apollo.consortium.proto.ExecutedTransaction;
import com.salesfoce.apollo.state.proto.Arguments;
import com.salesfoce.apollo.state.proto.Batch;
import com.salesfoce.apollo.state.proto.BatchUpdate;
import com.salesfoce.apollo.state.proto.BatchedTransaction;
import com.salesfoce.apollo.state.proto.Call;
import com.salesfoce.apollo.state.proto.EXECUTION;
import com.salesfoce.apollo.state.proto.Script;
import com.salesfoce.apollo.state.proto.Statement;
import com.salesfoce.apollo.state.proto.Txn;
import com.salesforce.apollo.consortium.TransactionExecutor;
import com.salesforce.apollo.consortium.support.CheckpointState;
import com.salesforce.apollo.protocols.Hash.HkHasher;
import com.salesforce.apollo.protocols.HashKey;

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

    public static class Event {
        public final JsonNode body;
        public final String   discriminator;

        public Event(String discriminator, JsonNode body) {
            this.discriminator = discriminator;
            this.body = body;
        }
    }

    public interface Registry {
        void deregister(String discriminator);

        void register(String discriminator, BiConsumer<String, Any> handler);
    }

    public class TxnExec implements TransactionExecutor {

        @Override
        public void beginBlock(long height, HashKey hash) {
            SqlStateMachine.this.beginBlock(height, hash);
        }

        @Override
        public void execute(HashKey blockHash, ExecutedTransaction t, BiConsumer<Object, Throwable> completion) {
            boolean closed;
            try {
                closed = connection.isClosed();
            } catch (SQLException e) {
                return;
            }
            if (closed) {
                return;
            }
            Any tx = t.getTransaction().getTxn();
            HashKey txnHash = new HashKey(t.getHash());
            SecureRandom prev = secureRandom.get();
            secureRandom.set(entropy.get());
            try {
                Object results;
                if (tx.is(BatchedTransaction.class)) {
                    results = executeBatchTransaction(t, tx, txnHash);
                } else if (tx.is(Batch.class)) {
                    results = executeBatch(blockHash, tx, txnHash);
                } else if (tx.is(BatchUpdate.class)) {
                    results = executeBatchUpdate(blockHash, tx, txnHash);
                } else if (tx.is(Statement.class)) {
                    results = executeStatement(blockHash, tx, txnHash);
                } else if (tx.is(Script.class)) {
                    results = executeScript(blockHash, tx, txnHash);
                } else {
                    throw new IllegalStateException("unknown statement type");
                }
                complete(completion, results);
            } catch (JdbcSQLNonTransientConnectionException e) {
                // ignore
            } catch (Exception e) {
                rollback();
                exception(completion, e);
            } finally {
                secureRandom.set(prev);
                commit();
            }

        }

        @Override
        public void processGenesis(Any genesisData) {
            initializeEvents();
            if (!genesisData.is(BatchedTransaction.class)) {
                log.info("Unknown genesis data type: {}", genesisData.getTypeUrl());
                return;
            }
            BatchedTransaction txns;
            try {
                txns = genesisData.unpack(BatchedTransaction.class);
            } catch (InvalidProtocolBufferException e1) {
                log.info("Error unpacking batched transaction genesis");
                return;
            }
            HashKey hash = HashKey.ORIGIN;
            for (Txn txn : txns.getTransactionsList()) {
                try {
                    if (txn.hasStatement()) {
                        acceptPreparedStatement(hash, txn.getStatement());
                    } else if (txn.hasBatch()) {
                        acceptBatch(hash, txn.getBatch());
                    } else if (txn.hasCall()) {
                        acceptCall(hash, txn.getCall());
                    } else if (txn.hasBatchUpdate()) {
                        acceptBatchUpdate(hash, txn.getBatchUpdate());
                    } else {
                        log.error("Unknown transaction type");
                        return;
                    }
                } catch (SQLException e) {
                    log.error("Unable to process transaction", e);
                    rollback();
                    return;
                }
            }
            commit();
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

    public static final DataHandler NULL_HANDLER = (DataHandler) Proxy.newProxyInstance(DataHandler.class.getClassLoader(),
                                                                                        new Class<?>[] { DataHandler.class },
                                                                                        new InvocationHandler() {

                                                                                            @Override
                                                                                            public Object invoke(Object proxy,
                                                                                                                 Method method,
                                                                                                                 Object[] args) throws Throwable {
                                                                                                return null;
                                                                                            }
                                                                                        });

    private static final String                    CREATE_ALIAS_APOLLO_INTERNAL_PUBLISH    = String.format("CREATE ALIAS __APOLLO_INTERNAL__.PUBLISH FOR \"%s.publish\"",
                                                                                                           SqlStateMachine.class.getCanonicalName());
    private static final String                    CREATE_SCHEMA_APOLLO_INTERNAL           = "CREATE SCHEMA __APOLLO_INTERNAL__";
    private static final String                    CREATE_TABLE_APOLLO_INTERNAL_TRAMPOLINE = "CREATE TABLE __APOLLO_INTERNAL__.TRAMPOLINE(ID INT AUTO_INCREMENT, CHANNEL VARCHAR(255), BODY JSON)";
    private static final String                    DELETE_FROM_APOLLO_INTERNAL_TRAMPOLINE  = "DELETE FROM __APOLLO_INTERNAL__.TRAMPOLINE";
    private static final RowSetFactory             factory;
    private static final Logger                    log                                     = LoggerFactory.getLogger(SqlStateMachine.class);
    private static final ObjectMapper              MAPPER                                  = new ObjectMapper();
    private static final String                    PUBLISH_INSERT                          = "INSERT INTO __APOLLO_INTERNAL__.TRAMPOLINE(CHANNEL, BODY) VALUES(?1, ?2)";
    private static final ThreadLocal<SecureRandom> secureRandom                            = new ThreadLocal<>();
    private static final String                    SELECT_FROM_APOLLO_INTERNAL_TRAMPOLINE  = "select * from __APOLLO_INTERNAL__.TRAMPOLINE";

    static {
        try {
            factory = RowSetProvider.newFactory();
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot create row set factory", e);
        }
    }

    public static BatchedTransaction batch(Message... messages) {
        BatchedTransaction.Builder builder = BatchedTransaction.newBuilder();
        for (Message message : messages) {
            if (message instanceof Call) {
                builder.addTransactions(Txn.newBuilder().setCall((Call) message));
            } else if (message instanceof Batch) {
                builder.addTransactions(Txn.newBuilder().setBatch((Batch) message));
            } else if (message instanceof BatchUpdate) {
                builder.addTransactions(Txn.newBuilder().setBatchUpdate((BatchUpdate) message));
            } else if (message instanceof Statement) {
                builder.addTransactions(Txn.newBuilder().setStatement((Statement) message));
            } else {
                throw new IllegalArgumentException("Unknown transaction batch element type: " + message.getClass());
            }
        }
        return builder.build();
    }

    public static Batch batch(String... statements) {
        Batch.Builder builder = Batch.newBuilder();
        for (String sql : statements) {
            builder.addStatements(sql);
        }
        return builder.build();
    }

    public static BatchUpdate batch(String sql, List<List<Value>> batch) {
        BatchUpdate.Builder builder = BatchUpdate.newBuilder().setSql(sql);
        Data data = Data.create(NULL_HANDLER, 1024, false);
        for (List<Value> arguments : batch) {
            Arguments.Builder args = Arguments.newBuilder();
            for (Value argument : arguments) {
                args.addArgs(serialized(data, argument));
            }
            builder.addBatch(args);
        }

        return builder.build();
    }

    public static BatchUpdate batchOf(String sql, List<List<Object>> batch) {
        return batch(sql,
                     batch.stream()
                          .map(args -> args.stream().map(o -> convert(o)).collect(Collectors.toList()))
                          .collect(Collectors.toList()));
    }

    public static Call call(EXECUTION execution, String sql, List<SQLType> outParameters, Object... arguments) {
        Value[] argValues = new Value[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            argValues[i] = convert(arguments[i]);
        }
        return call(execution, sql, outParameters, argValues);
    }

    public static Call call(EXECUTION execution, String sql, List<SQLType> outParameters, Value... arguments) {
        Call.Builder builder = Call.newBuilder().setSql(sql);
        Data data = Data.create(NULL_HANDLER, 1024, false);
        for (Value argument : arguments) {
            builder.addArgs(serialized(data, argument));
        }

        return builder.build();
    }

    public static Call call(String sql, List<SQLType> outParameters, Object... arguments) {
        return call(EXECUTION.EXECUTE, sql, outParameters, arguments);
    }

    public static Script callScript(String className, String method, String source, Value... args) {
        Data data = Data.create(NULL_HANDLER, 1024, false);
        return Script.newBuilder()
                     .setClassName(className)
                     .setMethod(method)
                     .setSource(source)
                     .addAllArgs(Arrays.asList(args)
                                       .stream()
                                       .map(v -> serialized(data, v))
                                       .collect(Collectors.toList()))
                     .build();
    }

    public static Value convert(Object x) {
        if (x == null) {
            return ValueNull.INSTANCE;
        }
        if (x instanceof String) {
            return ValueString.get((String) x);
        } else if (x instanceof Value) {
            return (Value) x;
        } else if (x instanceof Long) {
            return ValueLong.get((Long) x);
        } else if (x instanceof Integer) {
            return ValueInt.get((Integer) x);
        } else if (x instanceof BigInteger) {
            return ValueDecimal.get((BigInteger) x);
        } else if (x instanceof BigDecimal) {
            return ValueDecimal.get((BigDecimal) x);
        } else if (x instanceof Boolean) {
            return ValueBoolean.get((Boolean) x);
        } else if (x instanceof Byte) {
            return ValueByte.get((Byte) x);
        } else if (x instanceof Short) {
            return ValueShort.get((Short) x);
        } else if (x instanceof Float) {
            return ValueFloat.get((Float) x);
        } else if (x instanceof Double) {
            return ValueDouble.get((Double) x);
        } else if (x instanceof byte[]) {
            return ValueBytes.get((byte[]) x);
        } else if (x instanceof Date) {
            return ValueDate.get(null, (Date) x);
        } else if (x instanceof Time) {
            return ValueTime.get(null, (Time) x);
        } else if (x instanceof Timestamp) {
            return ValueTimestamp.get(null, (Timestamp) x);
        } else if (x instanceof java.util.Date) {
            return ValueTimestamp.fromMillis(((java.util.Date) x).getTime(), 0);
        } else if (x instanceof java.sql.Array) {
            java.sql.Array array = (java.sql.Array) x;
            try {
                return convert(array.getArray());
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
        } else if (x instanceof UUID) {
            return ValueUuid.get((UUID) x);
        }
        Class<?> clazz = x.getClass();
        if (x instanceof Object[]) {
            // (a.getClass().isArray());
            // (a.getClass().getComponentType().isPrimitive());
            Object[] o = (Object[]) x;
            int len = o.length;
            Value[] v = new Value[len];
            for (int i = 0; i < len; i++) {
                v[i] = convert(o[i]);
            }
            return ValueArray.get(clazz.getComponentType(), v);
        } else if (x instanceof Character) {
            return ValueStringFixed.get(((Character) x).toString());
        } else if (clazz == JSR310.LOCAL_DATE) {
            return JSR310Utils.localDateToValue(x);
        } else if (clazz == JSR310.LOCAL_TIME) {
            return JSR310Utils.localTimeToValue(x);
        } else if (clazz == JSR310.LOCAL_DATE_TIME) {
            return JSR310Utils.localDateTimeToValue(x);
        } else if (clazz == JSR310.INSTANT) {
            return JSR310Utils.instantToValue(x);
        } else if (clazz == JSR310.OFFSET_TIME) {
            return JSR310Utils.offsetTimeToValue(x);
        } else if (clazz == JSR310.OFFSET_DATE_TIME) {
            return JSR310Utils.offsetDateTimeToValue(x);
        } else if (clazz == JSR310.ZONED_DATE_TIME) {
            return JSR310Utils.zonedDateTimeToValue(x);
        } else if (x instanceof TimestampWithTimeZone) {
            return ValueTimestampTimeZone.get((TimestampWithTimeZone) x);
        } else if (x instanceof Interval) {
            Interval i = (Interval) x;
            return ValueInterval.from(i.getQualifier(), i.isNegative(), i.getLeading(), i.getRemaining());
        } else if (clazz == JSR310.PERIOD) {
            return JSR310Utils.periodToValue(x);
        } else if (clazz == JSR310.DURATION) {
            return JSR310Utils.durationToValue(x);
        } else {
            throw new IllegalArgumentException("Unknown value type: " + x.getClass());
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

    public static ByteString serialized(Data data, Value arg) {
        int valueLen = Data.getValueLen(arg, false);
        data.checkCapacity(valueLen);
        data.writeValue(arg);
        byte[] serialized = data.getBytes();
        ByteString bs = ByteString.copyFrom(serialized, 0, valueLen);
        data.reset();
        return bs;
    }

    public static void setSecureRandom(SecureRandom random) {
        secureRandom.set(random);
    }

    public static Statement statement(EXECUTION execution, String sql, Object... args) {
        Value[] paramValues = new Value[args.length];
        for (int i = 0; i < args.length; i++) {
            paramValues[i] = convert(args[i]);
        }

        return statement(execution, sql, paramValues);
    }

    public static Statement statement(EXECUTION execution, String sql, Value... args) {
        Statement.Builder builder = Statement.newBuilder().setSql(sql);
        Data data = Data.create(NULL_HANDLER, 1024, false);
        for (Value arg : args) {
            builder.addArgs(serialized(data, arg));
        }

        return builder.build();
    }

    public static Statement statement(String sql, Object... args) {
        return statement(EXECUTION.EXECUTE, sql, args);
    }

    private final LoadingCache<String, CallableStatement> callCache;

    private final File checkpointDirectory;

    private final ScriptCompiler compiler = new ScriptCompiler();

    private final JdbcConnection connection;

    private final AtomicReference<SecureRandom> entropy = new AtomicReference<>();

    private final TxnExec executor = new TxnExec();

    private final ForkJoinPool fjPool;

    private final Properties info;

    private final LoadingCache<String, PreparedStatement> psCache;

    private final EventTrampoline trampoline = new EventTrampoline();

    private final String url;

    public SqlStateMachine(String url, Properties info, File checkpointDirectory) {
        this(url, info, checkpointDirectory, ForkJoinPool.commonPool());
    }

    public SqlStateMachine(String url, Properties info, File cpDir, ForkJoinPool fjPool) {
        this.url = url;
        this.info = info;
        this.fjPool = fjPool;
        this.checkpointDirectory = cpDir;
        if (checkpointDirectory.exists()) {
            if (!checkpointDirectory.isDirectory()) {
                throw new IllegalArgumentException("Must be a directory: " + checkpointDirectory.getAbsolutePath());
            }
        } else {
            if (!checkpointDirectory.mkdirs()) {
                throw new IllegalArgumentException(
                        "Cannot create checkpoint directory: " + checkpointDirectory.getAbsolutePath());
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
    void initializeEvents() {
        java.sql.Statement statement = null;

        SecureRandom prev = secureRandom.get();
        secureRandom.set(entropy.get());
        try {
            statement = connection.createStatement();
            statement.execute(CREATE_SCHEMA_APOLLO_INTERNAL);
            statement.execute(CREATE_TABLE_APOLLO_INTERNAL_TRAMPOLINE);
            statement.execute(CREATE_ALIAS_APOLLO_INTERNAL_PUBLISH);
        } catch (SQLException e) {
            throw new IllegalStateException("unable to create event publish function", e);
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

    private int[] acceptBatch(HashKey hash, Batch batch) throws SQLException {
        try (java.sql.Statement exec = connection.createStatement()) {
            for (String sql : batch.getStatementsList()) {
                exec.addBatch(sql);
            }
            return exec.executeBatch();
        }
    }

    private int[] acceptBatchUpdate(HashKey hash, BatchUpdate batchUpdate) throws SQLException {
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
                    Data data = Data.create(SqlStateMachine.NULL_HANDLER, argument.toByteArray(), false);
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

    private CallResult acceptCall(HashKey hash, Call call) throws SQLException {
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
                Data data = Data.create(SqlStateMachine.NULL_HANDLER, argument.toByteArray(), false);
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

    private List<ResultSet> acceptPreparedStatement(HashKey hash, Statement statement) throws SQLException {
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

    private Object acceptScript(HashKey hash, Script script) throws SQLException {

        Object instance;

        Value[] args = new Value[script.getArgsCount()];
        int i = 1;
        for (ByteString argument : script.getArgsList()) {
            Data data = Data.create(SqlStateMachine.NULL_HANDLER, argument.toByteArray(), false);
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
                                  new IllegalArgumentException(
                                          "Must contain invocation method named: " + callName + "(...)"),
                                  script.getSource());
        }

        Object returnValue = new JavaMethod(call).getValue(instance, getSession(), args);
        if (returnValue instanceof ResultSet) {
            CachedRowSet rowset = factory.createCachedRowSet();

            rowset.populate((ResultSet) returnValue);
            return rowset;
        }
        return returnValue;
    }

    private void beginBlock(long height, HashKey hash) {
        HkHasher hasher = new HkHasher(hash, height);
        getSession().getRandom().setSeed(hasher.getH1());
        try {
            SecureRandom secureEntropy = SecureRandom.getInstance("SHA1PRNG");
            secureEntropy.setSeed(hash.bytes());
            entropy.set(secureEntropy);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("No SHA1PRNG available", e);
        }
    }

    private void commit() {
        try {
            publishEvents();
            connection.commit();
        } catch (SQLException e) {
            log.trace("unable to commit connection", e);
        }
    }

    private void complete(BiConsumer<Object, Throwable> completion, Object results) {
        if (completion == null) {
            return;
        }
        fjPool.execute(() -> completion.accept(results, null));
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

    private void exception(BiConsumer<Object, Throwable> completion, Throwable e) {
        if (completion != null) {
            completion.accept(null, e);
        }
    }

    private Object execute(ExecutedTransaction t, Txn txn, HashKey hash) throws Exception {
        try {
            if (txn.hasStatement()) {
                return acceptPreparedStatement(hash, txn.getStatement());
            } else if (txn.hasBatch()) {
                return acceptBatch(hash, txn.getBatch());
            } else if (txn.hasCall()) {
                return acceptCall(hash, txn.getCall());
            } else if (txn.hasBatchUpdate()) {
                return acceptBatchUpdate(hash, txn.getBatchUpdate());
            } else {
                log.error("Unknown transaction type: {}", t.getHash());
                return null;
            }
        } catch (Throwable th) {
            return th;
        }
    }

    private int[] executeBatch(HashKey blockHash, Any tx, HashKey txnHash) throws Exception {
        Batch batch;
        try {
            batch = tx.unpack(Batch.class);
        } catch (InvalidProtocolBufferException e) {
            log.warn("Cannot deserialize batch: {} of block: {}", txnHash, blockHash);
            throw e;
        }
        return acceptBatch(txnHash, batch);
    }

    private List<Object> executeBatchTransaction(ExecutedTransaction t, Any tx, HashKey txnHash) throws Exception {
        BatchedTransaction txns;
        try {
            txns = tx.unpack(BatchedTransaction.class);
        } catch (InvalidProtocolBufferException e) {
            log.error("Error deserializing transaction: {}  ", t.getHash());
            throw e;
        }
        List<Object> results = new ArrayList<Object>();
        for (int i = 0; i < txns.getTransactionsCount(); i++) {
            try {
                results.add(SqlStateMachine.this.execute(t, txns.getTransactions(i), txnHash));
            } catch (Throwable e) {
                throw new Mutator.BatchedTransactionException(i, e);
            }
        }
        return results;
    }

    private int[] executeBatchUpdate(HashKey blockHash, Any tx, HashKey txnHash) throws Exception {
        BatchUpdate batch;
        try {
            batch = tx.unpack(BatchUpdate.class);
        } catch (InvalidProtocolBufferException e) {
            log.warn("Cannot deserialize batch update: {} of block: {}", txnHash, blockHash);
            throw e;
        }
        return acceptBatchUpdate(txnHash, batch);
    }

    private Object executeScript(HashKey blockHash, Any tx, HashKey txnHash) throws Exception {
        Script script;
        try {
            script = tx.unpack(Script.class);
        } catch (InvalidProtocolBufferException e) {
            log.warn("Cannot deserialize Script {} of block: {}", txnHash, blockHash);
            throw e;
        }
        return acceptScript(txnHash, script);
    }

    private List<ResultSet> executeStatement(HashKey blockHash, Any tx, HashKey txnHash) throws Exception {
        Statement statement;
        try {
            statement = tx.unpack(Statement.class);
        } catch (InvalidProtocolBufferException e) {
            log.warn("Cannot deserialize Statement {} of block: {}", txnHash, blockHash);
            throw e;
        }
        return acceptPreparedStatement(txnHash, statement);
    }

    private Session getSession() {
        return (Session) connection.getSession();
    }

    private void publishEvents() {
        PreparedStatement exec;
        try {
            exec = psCache.get(SELECT_FROM_APOLLO_INTERNAL_TRAMPOLINE);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof JdbcSQLNonTransientException
                    || e.getCause() instanceof JdbcSQLNonTransientConnectionException) {
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
}
