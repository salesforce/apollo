/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiConsumer;

import org.h2.jdbc.JdbcConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.consortium.proto.ExecutedTransaction;
import com.salesfoce.apollo.state.proto.Statement;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
public class Updater implements BiConsumer<ExecutedTransaction, BiConsumer<HashKey, Throwable>> {
    private static final Logger  log = LoggerFactory.getLogger(Updater.class);
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
    public void accept(ExecutedTransaction t, BiConsumer<HashKey, Throwable> completion) {
        t.getTransaction().getBatchList().forEach(txn -> {
            Statement statement;
            try {
                statement = txn.unpack(Statement.class);
            } catch (InvalidProtocolBufferException e) {
                log.error("unable to deserialize Statement from txn: {} : {}", new HashKey(t.getHash()), e.toString());
                complete(completion, e);
                return;
            }
            try {
                java.sql.Statement exec = connection.createStatement();
                exec.execute(statement.getSql());
                complete(completion, new HashKey(t.getHash()));
            } catch (SQLException e) {
                log.warn("Error executing Statement: {} from txn: {} : {}", statement.getSql(),
                         new HashKey(t.getHash()), e.toString());
                complete(completion, e);
                return;
            }
        });
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

    private void complete(BiConsumer<HashKey, Throwable> completion, HashKey hashKey) {
        if (completion == null) {
            return;
        }
        fjPool.execute(() -> completion.accept(hashKey, null));
    }

    private void complete(BiConsumer<HashKey, Throwable> completion, Throwable e) {
        if (completion == null) {
            return;
        }
        fjPool.execute(() -> completion.accept(null, e));
    }

    private void rollback() {
        try {
            connection.rollback();
        } catch (SQLException e) {
        }
    }
}
