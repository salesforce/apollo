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
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.zip.GZIPOutputStream;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;

import org.h2.engine.Session;
import org.h2.jdbc.JdbcConnection;
import org.h2.jdbc.JdbcSQLNonTransientConnectionException;
import org.h2.jdbc.JdbcSQLNonTransientException;
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
                commit();
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

        @Override
        public void setBlockHeight(long height) {
            Session session = getSession();
            if (session != null) {
                session.setBlockHeight(height);
            }
        }
    }

    private static final RowSetFactory factory;

    private static final Logger log = LoggerFactory.getLogger(SqlStateMachine.class);

    static {
        try {
            factory = RowSetProvider.newFactory();
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot create row set factory", e);
        }
    }
    private final LoadingCache<String, CallableStatement> callCache;

    private final File checkpointDirectory;

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

    public Function<Long, File> getCheckpointer() {
        return height -> {
            String rndm = UUID.randomUUID().toString();
            try (java.sql.Statement statement = connection.createStatement()) {
                File temp = new File(checkpointDirectory, String.format("checkpoint-%s--%s.sql", height, rndm));
                try {
                    statement.execute(String.format("BLOCKSCRIPT BLOCKHEIGHT %s DROP TO '%s'", height,
                                                    temp.getAbsolutePath()));
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
                    Data data = Data.create(Helper.NULL_HANDLER, argument.toByteArray(), false);
                    setArgument(exec, i++, data.readValue());
                }
                exec.addBatch();
            }
            return exec.executeBatch();
        } finally {
            try {
                exec.clearBatch();
                exec.clearParameters();
            } catch (JdbcSQLNonTransientException e) {
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
            for (ByteString argument : call.getArguments().getArgsList()) {
                Data data = Data.create(Helper.NULL_HANDLER, argument.toByteArray(), false);
                setArgument(exec, i++, data.readValue());
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
        } finally {
            try {
                exec.clearBatch();
                exec.clearParameters();
            } catch (JdbcSQLNonTransientException e) {
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
            int i = 0;
            for (ByteString argument : statement.getArguments().getArgsList()) {
                Data data = Data.create(Helper.NULL_HANDLER, argument.toByteArray(), false);
                setArgument(exec, i++, data.readValue());
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
        } finally {
            try {
                exec.clearBatch();
                exec.clearParameters();
            } catch (JdbcSQLNonTransientException e) {
                // ignore
            }
        }
    }

    private void commit() {
        try {
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
                } catch (JdbcSQLNonTransientException e) {
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
                } catch (JdbcSQLNonTransientException e) {
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
        for (Txn txn : txns.getTransactionsList()) {
            results.add(SqlStateMachine.this.execute(t, txn, txnHash));
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
