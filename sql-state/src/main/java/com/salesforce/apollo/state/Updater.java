/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiConsumer;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;

import org.h2.engine.Session;
import org.h2.jdbc.JdbcConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.consortium.proto.ExecutedTransaction;
import com.salesfoce.apollo.state.proto.BatchStatements;
import com.salesfoce.apollo.state.proto.Statement;
import com.salesforce.apollo.consortium.TransactionExecutor;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
public class Updater implements TransactionExecutor {
    private static final RowSetFactory factory;
    private static final Logger        log = LoggerFactory.getLogger(Updater.class);
    static {
        try {
            factory = RowSetProvider.newFactory();
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot create row set factory", e);
        }
    }
    private final JdbcConnection connection;
    private final ForkJoinPool   fjPool;
    private final Properties     info;
    private final String         url;

    public Updater(String url, Properties info) {
        this(url, info, ForkJoinPool.commonPool());
    }

    public Updater(String url, Properties info, ForkJoinPool fjPool) {
        this.url = url;
        this.info = info;
        this.fjPool = fjPool;
        try {
            connection = new JdbcConnection(url, info);
        } catch (SQLException e) {
            throw new IllegalStateException("Unable to create connection using " + url, e);
        }
    }

    @Override
    public void execute(HashKey blockHash, long blockHeight, ExecutedTransaction t,
                        BiConsumer<Object, Throwable> completion) {
        if (t.getTransaction().getTxn().is(Statement.class)) {
            acceptStatement(blockHeight, t, completion);
        } else if (t.getTransaction().getTxn().is(BatchStatements.class)) {
            acceptBatch(blockHeight, t, completion);
        } else {
            log.error("Unknown transaction: {} type: {}", t.getHash(), t.getTransaction().getTxn().getTypeUrl());
        }
    }

    public void close() {
        rollback();
        try {
            connection.close();
        } catch (SQLException e) {
        }
    }

    public Connection newConnection() {
        try {
            return new JdbcConnection(url, info);
        } catch (SQLException e) {
            throw new IllegalStateException("cannot get JDBC connection", e);
        }
    }

    private void acceptBatch(long blockHeight, ExecutedTransaction t, BiConsumer<Object, Throwable> completion) {
        try {
            BatchStatements batch;
            try {
                batch = t.getTransaction().getTxn().unpack(BatchStatements.class);
            } catch (InvalidProtocolBufferException e) {
                log.error("unable to deserialize BatchStatements from txn: {} : {}", new HashKey(t.getHash()),
                          e.toString());
                complete(null, e);
                return;
            }
            getSession().setBlockHeight(blockHeight);
            try (java.sql.Statement exec = connection.createStatement();) {
                batch.getStatementsList().forEach(sql -> {
                    try {
                        exec.addBatch(sql);
                    } catch (SQLException e) {
                        complete(completion, e);
                        return;
                    }
                });
                int[] count = exec.executeBatch();
                complete(completion, count);
            } catch (SQLException e) {
                exception(completion, e);
                return;
            }
            connection.commit();
        } catch (Exception e) {
            try {
                log.warn("Rolling back transaction: {}, {}", new HashKey(t.getHash()), e.toString());
                connection.rollback();
                exception(completion, e);
            } catch (SQLException e1) {
                log.error("unable to rollback: {}", new HashKey(t.getHash()), e1);
                throw new IllegalStateException("Cannot rollback txn", e1);
            }
        }
    }

    private void acceptStatement(long blockHeight, ExecutedTransaction t, BiConsumer<Object, Throwable> completion) {
        try {
            Statement statement;
            try {
                statement = t.getTransaction().getTxn().unpack(Statement.class);
            } catch (InvalidProtocolBufferException e) {
                log.error("unable to deserialize Statement from txn: {} : {}", new HashKey(t.getHash()), e.toString());
                complete(null, e);
                return;
            }
            getSession().setBlockHeight(blockHeight);
            try (java.sql.Statement exec = connection.createStatement()) {
                if (exec.execute(statement.getSql())) {
                    CachedRowSet rowset = factory.createCachedRowSet();

                    List<ResultSet> results = new ArrayList<>();
                    rowset.populate(exec.getResultSet());
                    results.add(rowset);

                    while (exec.getMoreResults()) {
                        rowset = factory.createCachedRowSet();
                        rowset.populate(exec.getResultSet());
                        results.add(rowset);
                    }
                    complete(completion, results);
                } else {
                    complete(completion, null);
                }
            } catch (SQLException e) {
                log.warn("Error executing Statement: {} from txn: {} : {}", statement.getSql(),
                         new HashKey(t.getHash()), e.toString());
                exception(completion, e);
                return;
            }
            connection.commit();
        } catch (Exception e) {
            try {
                log.warn("Rolling back transaction: {}, {}", new HashKey(t.getHash()), e.toString());
                connection.rollback();
                exception(completion, e);
            } catch (SQLException e1) {
                log.error("unable to rollback: {}", new HashKey(t.getHash()), e1);
                throw new IllegalStateException("Cannot rollback txn", e1);
            }
        }
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

    private void rollback() {
        try {
            connection.rollback();
        } catch (SQLException e) {
        }
    }
    
    private Session getSession() {
        return (Session) connection.getSession();
    }
}
