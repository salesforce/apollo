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
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.zip.GZIPOutputStream;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;

import org.h2.engine.Session;
import org.h2.jdbc.JdbcConnection;
import org.h2.store.Data;
import org.h2.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.consortium.proto.ExecutedTransaction;
import com.salesfoce.apollo.state.proto.Arguments;
import com.salesfoce.apollo.state.proto.Batch;
import com.salesfoce.apollo.state.proto.BatchUpdate;
import com.salesfoce.apollo.state.proto.BatchedTransaction;
import com.salesfoce.apollo.state.proto.Call;
import com.salesfoce.apollo.state.proto.Statement;
import com.salesfoce.apollo.state.proto.Txn;
import com.salesforce.apollo.consortium.TransactionExecutor;
import com.salesforce.apollo.protocols.HashKey;

/**
 * This is ye Jesus Nut of sql state via distribute linear logs. We use H2 as a
 * the local materialized view that is constructed by SQL DML and DDL embedded
 * in the log as SQL statements. Checkpointing is accomplished by a *new* H2
 * BLOCKSCRIPT command that will output SQL to recreate the state of the DB at a
 * given block height (i.e. the checkpoint). Mutation is interactive with the
 * submitter of the transaction statements - i.e. result sets n' multiple result
 * sets n' out params (in calls). This provides a "reasonable" asynchronous
 * interaction with this log based mutation (through consensus).
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

    public class TxnExec implements TransactionExecutor {

        @Override
        public void execute(HashKey blockHash, long blockHeight, ExecutedTransaction t,
                            BiConsumer<Object, Throwable> completion) {
            Any tx = t.getTransaction().getTxn();
            HashKey txnHash = new HashKey(t.getHash());

            if (tx.is(BatchedTransaction.class)) {
                executeBatchTransaction(blockHeight, t, completion, tx, txnHash);
            } else if (tx.is(Batch.class)) {
                executeBatch(blockHash, blockHeight, completion, tx, txnHash);
            } else if (tx.is(BatchUpdate.class)) {
                executeBatchUpdate(blockHash, blockHeight, completion, tx, txnHash);
            } else if (tx.is(Statement.class)) {
                executeStatement(blockHash, blockHeight, completion, tx, txnHash);
            }

        }

        @Override
        public void processGenesis(Any genesisData) {
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
                        acceptPreparedStatement(hash, 0, txn.getStatement());
                    } else if (txn.hasBatch()) {
                        acceptBatch(hash, 0, txn.getBatch());
                    } else if (txn.hasCall()) {
                        acceptCall(hash, 0, txn.getCall());
                    } else if (txn.hasBatchUpdate()) {
                        acceptBatchUpdate(hash, 0, txn.getBatchUpdate());
                    } else {
                        log.error("Unknown transaction type");
                        return;
                    }
                } catch (SQLException e) {
                    log.error("Unable to process transaction", e);
                    return;
                }
            }

            try {
                connection.commit();
            } catch (SQLException e) {
                log.error("Error committing genesis transaction");
            }
        }
    }

    private static final RowSetFactory factory;
    private static final Logger        log = LoggerFactory.getLogger(SqlStateMachine.class);

    static {
        try {
            factory = RowSetProvider.newFactory();
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot create row set factory", e);
        }
    }

    private final LoadingCache<String, CallableStatement> callCache;
    private final File                                    checkpointDirectory;
    private final JdbcConnection                          connection;
    private final TxnExec                                 executor = new TxnExec();
    private final ForkJoinPool                            fjPool;
    private final Properties                              info;
    private final LoadingCache<String, PreparedStatement> psCache;
    private final String                                  url;

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

    public Function<Long, File> getCheckpointer() {
        return height -> {
            String rndm = UUID.randomUUID().toString();
            java.sql.Statement statement;
            File temp = new File(checkpointDirectory, String.format("checkpoint-%s--%s.sql", height, rndm));
            try {
                statement = connection.createStatement();
                statement.execute(String.format("BLOCKSCRIPT BLOCKHEIGHT 1 DROP TO '%s'", temp.getAbsolutePath()));
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

    private int[] acceptBatch(HashKey hash, long blockHeight, Batch batch) throws SQLException {
        connection.setAutoCommit(false);
        getSession().setBlockHeight(blockHeight);

        int[] updated = new int[0];
        try (java.sql.Statement exec = connection.createStatement()) {
            for (String sql : batch.getStatementsList()) {
                exec.addBatch(sql);
            }
            updated = exec.executeBatch();
            connection.commit();
        }
        return updated;
    }

    private int[] acceptBatchUpdate(HashKey hash, long blockHeight, BatchUpdate batchUpdate) throws SQLException {
        AtomicInteger i = new AtomicInteger();
        try (PreparedStatement exec = connection.prepareStatement(batchUpdate.getSql())) {
            for (Arguments args : batchUpdate.getBatchList()) {
                for (ByteString argument : args.getArgsList()) {
                    Data data = Data.create(Helper.NULL_HANDLER, argument.toByteArray(), false);
                    setArgument(exec, i.getAndIncrement(), data.readValue());
                    exec.addBatch();
                }
            }
            return exec.executeBatch();
        }
    }

    private CallResult acceptCall(HashKey hash, long blockHeight, Call call) throws SQLException {
        List<ResultSet> results = new ArrayList<>();
        AtomicInteger i = new AtomicInteger();
        CallableStatement exec;
        try {
            exec = callCache.get(call.getSql());
        } catch (ExecutionException e) {
            if (e.getCause() instanceof SQLException) {
                throw (SQLException) e.getCause();
            }
            throw new IllegalStateException("Unable to get callable statement: " + call.getSql(), e);
        }
        for (ByteString argument : call.getArguments().getArgsList()) {
            Data data = Data.create(Helper.NULL_HANDLER, argument.toByteArray(), false);
            setArgument(exec, i.getAndIncrement(), data.readValue());
            exec.addBatch();
        }
        for (int p = 0; p < call.getOutParametersCount(); p++) {
            exec.registerOutParameter(p, call.getOutParameters(p));
        }
        List<Object> out = new ArrayList<>();
        if (exec.execute()) {
            CachedRowSet rowset = factory.createCachedRowSet();

            rowset.populate(exec.getResultSet());
            results.add(rowset);

            while (exec.getMoreResults()) {
                rowset = factory.createCachedRowSet();
                rowset.populate(exec.getResultSet());
                results.add(rowset);
            }
        }
        for (int j = 0; j < call.getOutParametersCount(); j++) {
            out.add(exec.getObject(j));
        }
        return new CallResult(out, results);
    }

    private List<ResultSet> acceptPreparedStatement(HashKey hash, long blockHeight,
                                                    Statement statement) throws SQLException {
        List<ResultSet> results = new ArrayList<>();
        connection.setAutoCommit(false);
        getSession().setBlockHeight(blockHeight);
        AtomicInteger i = new AtomicInteger();
        PreparedStatement exec;
        try {
            exec = psCache.get(statement.getSql());
        } catch (ExecutionException e) {
            if (e.getCause() instanceof SQLException) {
                throw (SQLException) e.getCause();
            }
            throw new IllegalStateException("Unable to create prepared statement: " + statement.getSql(), e);
        }
        for (ByteString argument : statement.getArguments().getArgsList()) {
            Data data = Data.create(Helper.NULL_HANDLER, argument.toByteArray(), false);
            setArgument(exec, i.getAndIncrement(), data.readValue());
        }
        if (exec.execute()) {
            CachedRowSet rowset = factory.createCachedRowSet();

            rowset.populate(exec.getResultSet());
            results.add(rowset);

            while (exec.getMoreResults()) {
                rowset = factory.createCachedRowSet();
                rowset.populate(exec.getResultSet());
                results.add(rowset);
            }
        }
        return results;
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

    private Object execute(long blockHeight, ExecutedTransaction t, BiConsumer<Object, Throwable> completion, Txn txn,
                           HashKey hash) {
        try {
            if (txn.hasStatement()) {
                return acceptPreparedStatement(hash, blockHeight, txn.getStatement());
            } else if (txn.hasBatch()) {
                return acceptBatch(hash, blockHeight, txn.getBatch());
            } else if (txn.hasCall()) {
                return acceptCall(hash, blockHeight, txn.getCall());
            } else if (txn.hasBatchUpdate()) {
                return acceptBatchUpdate(hash, blockHeight, txn.getBatchUpdate());
            } else {
                log.error("Unknown transaction type: {}", t.getHash());
                return null;
            }
        } catch (Throwable th) {
            return th;
        }
    }

    private void executeBatch(HashKey blockHash, long blockHeight, BiConsumer<Object, Throwable> completion, Any tx,
                              HashKey txnHash) {
        Batch batch;
        try {
            batch = tx.unpack(Batch.class);
        } catch (InvalidProtocolBufferException e) {
            log.warn("Cannot deserialize batch: {} of block: {}", txnHash, blockHash);
            return;
        }
        int[] result;
        try {
            result = acceptBatch(txnHash, blockHeight, batch);
        } catch (SQLException e) {
            log.info("Exception processing batch: {} of block: {}", txnHash, blockHash, e);
            exception(completion, e);
            return;
        }
        try {
            connection.commit();
        } catch (SQLException e) {
            log.info("Exception committing batch: {} of block: {}", txnHash, blockHash, e);
            exception(completion, e);
            return;
        }
        complete(completion, result);
    }

    private void executeBatchTransaction(long blockHeight, ExecutedTransaction t,
                                         BiConsumer<Object, Throwable> completion, Any tx, HashKey txnHash) {
        BatchedTransaction txns;
        try {
            txns = tx.unpack(BatchedTransaction.class);
        } catch (InvalidProtocolBufferException e) {
            log.error("Error deserializing transaction: {}  ", t.getHash());
            return;
        }
        List<Object> results = new ArrayList<Object>();
        for (Txn txn : txns.getTransactionsList()) {

            Object result = SqlStateMachine.this.execute(blockHeight, t, completion, txn, txnHash);
            if (result instanceof Throwable) {
                complete(completion, (Throwable) result);
                return;
            } else {
                results.add(result);
            }
        }

        try {
            connection.commit();
            complete(completion, results);
        } catch (SQLException e) {
            log.info("Error executing txn: {} : {}", txnHash, e.toString());
            exception(completion, e);
        }
    }

    private void executeBatchUpdate(HashKey blockHash, long blockHeight, BiConsumer<Object, Throwable> completion,
                                    Any tx, HashKey txnHash) {
        BatchUpdate batch;
        try {
            batch = tx.unpack(BatchUpdate.class);
        } catch (InvalidProtocolBufferException e) {
            log.warn("Cannot deserialize batch update: {} of block: {}", txnHash, blockHash);
            return;
        }
        int[] result;
        try {
            result = acceptBatchUpdate(txnHash, blockHeight, batch);
        } catch (SQLException e) {
            log.info("Exception processing batch update: {} of block: {}", txnHash, blockHash, e);
            exception(completion, e);
            return;
        }
        try {
            connection.commit();
        } catch (SQLException e) {
            log.info("Exception committing batch update: {} of block: {}", txnHash, blockHash, e);
            exception(completion, e);
            return;
        }
        complete(completion, result);
    }

    private void executeStatement(HashKey blockHash, long blockHeight, BiConsumer<Object, Throwable> completion, Any tx,
                                  HashKey txnHash) {
        Statement statement;
        try {
            statement = tx.unpack(Statement.class);
        } catch (InvalidProtocolBufferException e) {
            log.warn("Cannot deserialize Statement {} of block: {}", txnHash, blockHash);
            return;
        }
        List<ResultSet> result;
        try {
            result = acceptPreparedStatement(txnHash, blockHeight, statement);
        } catch (SQLException e) {
            log.info("Exception processing Statement: {} of block: {}", txnHash, blockHash, e);
            exception(completion, e);
            return;
        }
        try {
            connection.commit();
        } catch (SQLException e) {
            log.info("Exception committing Statement: {} of block: {}", txnHash, blockHash, e);
            exception(completion, e);
            return;
        }
        complete(completion, result);
    }

    private Session getSession() {
        return (Session) connection.getSession();
    }

    private void setArgument(PreparedStatement exec, int i, Value value) {
        try {
            value.set(exec, i);
        } catch (SQLException e) {
            throw new IllegalArgumentException("Illegal argument value: " + value);
        }
    }
}
