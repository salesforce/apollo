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
import com.salesforce.apollo.protocols.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class Updater {
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
    }

    private static final RowSetFactory factory;
    private static final Logger        log = LoggerFactory.getLogger(Updater.class);

    static {
        try {
            factory = RowSetProvider.newFactory();
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot create row set factory", e);
        }
    }

    private final File           checkpointDirectory;
    private final JdbcConnection connection;
    private final TxnExec        executor = new TxnExec();
    private final ForkJoinPool   fjPool;
    private final Properties     info;
    private File                 temp;
    private final String         url;

    public Updater(String url, Properties info, File checkpointDirectory) {
        this(url, info, checkpointDirectory, ForkJoinPool.commonPool());
    }

    public Updater(String url, Properties info, File cpDir, ForkJoinPool fjPool) {
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
    }

    public void close() {
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
            java.sql.Statement statement;
            try {
                temp = File.createTempFile("checkpoint-" + height, "sql");
            } catch (IOException e) {
                log.error("Unable to create temporary checkpoint file: {}", height, e);
                return null;
            }
            temp.deleteOnExit();
            try {
                statement = connection.createStatement();
                statement.execute(String.format("BLOCKSCRIPT BLOCKHEIGHT 1 TO '%s'", temp.getAbsolutePath()));
            } catch (SQLException e) {
                log.error("unable to checkpoint: {}", height, e);
                return null;
            }
            File checkpoint = new File(checkpointDirectory, String.format("checkpoint-%s.sql.gzip", height));
            try (FileInputStream fis = new FileInputStream(temp);
                    FileOutputStream fos = new FileOutputStream(checkpoint)) {
                GZIPOutputStream gzos = new GZIPOutputStream(fos);
                Utils.copy(fis, gzos);
                gzos.flush();
            } catch (IOException e) {
                log.error("unable to checkpoint: {}", height, e);
            } finally {
                temp.delete();
            }
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
        try (CallableStatement exec = connection.prepareCall(call.getSql())) {
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
    }

    private List<ResultSet> acceptPreparedStatement(HashKey hash, long blockHeight,
                                                    Statement statement) throws SQLException {
        List<ResultSet> results = new ArrayList<>();
        connection.setAutoCommit(false);
        getSession().setBlockHeight(blockHeight);
        AtomicInteger i = new AtomicInteger();
        try (PreparedStatement exec = connection.prepareStatement(statement.getSql())) {
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
        }
        return results;
    }

    private void complete(BiConsumer<Object, Throwable> completion, Object results) {
        if (completion == null) {
            return;
        }
        fjPool.execute(() -> completion.accept(results, null));
    }

    private void exception(BiConsumer<Object, Throwable> completion, Throwable e) {
        completion.accept(null, e);
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

            Object result = Updater.this.execute(blockHeight, t, completion, txn, txnHash);
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
            exec.setObject(i, value);
        } catch (SQLException e) {
            throw new IllegalArgumentException("Illegal argument value: " + value);
        }
    }
}
