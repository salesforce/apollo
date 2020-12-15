/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiConsumer;

import org.h2.engine.IsolationLevel;
import org.h2.engine.Session;
import org.h2.engine.UndoLogRecord;
import org.h2.jdbc.JdbcConnection;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.RowImpl;
import org.h2.store.Data;
import org.h2.table.Table;
import org.h2.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.consortium.proto.ExecutedTransaction;
import com.salesfoce.apollo.state.proto.Delete;
import com.salesfoce.apollo.state.proto.Insert;
import com.salesfoce.apollo.state.proto.Result;
import com.salesfoce.apollo.state.proto.Results;
import com.salesfoce.apollo.state.proto.Statement;
import com.salesfoce.apollo.state.proto.Update;
import com.salesforce.apollo.consortium.TransactionExecutor;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
public class Updater implements TransactionExecutor {
    private static final Logger       log    = LoggerFactory.getLogger(Updater.class);
    private JdbcConnection            connection;
    private final CdcEngine           engine;
    private Session                   session;
    private final Map<Integer, Table> tables = new HashMap<>();

    public Updater(CdcEngine engine) {
        this.engine = engine;
    }

    @SuppressWarnings("unused")
    @Override
    public void accept(ExecutedTransaction t, BiConsumer<HashKey, Throwable> completion) {
        if (false) {
            acceptSimulated(t);
        }
        t.getTransaction().getBatchList().forEach(txn -> {
            Statement statement;
            try {
                statement = txn.unpack(Statement.class);
            } catch (InvalidProtocolBufferException e) {
                log.error("unable to deserialize Statement from txn: {} : {}", t.getHash(), e.toString());
                complete(completion, e);
                return;
            }
            try {
                java.sql.Statement exec = connection.createStatement();
                if (exec.execute(statement.getSql())) {
                    complete(completion, new HashKey(t.getHash()));
                } else {
                    log.debug("Unable to execute Statement: {} from txn: {}", statement.getSql(), t.getHash());
                    complete(completion, new SQLException("unable to execute transaction"));
                }
            } catch (SQLException e) {
                log.debug("Error executing Statement: {} from txn: {} : {}", statement.getSql(), t.getHash(),
                          e.toString());
                complete(completion, e);
                return;
            }
        });
    }

    private void complete(BiConsumer<HashKey, Throwable> completion, HashKey hashKey) {
        if (completion == null) {
            return;
        }
        ForkJoinPool.commonPool().execute(() -> completion.accept(hashKey, null));
    }

    private void complete(BiConsumer<HashKey, Throwable> completion, Throwable e) {
        if (completion == null) {
            return;
        }
        ForkJoinPool.commonPool().execute(() -> completion.accept(null, e));
    }

    @Override
    public void begin() {
        if (connection != null) {
            log.error("Execution series has already been established!");
            return;
        }
        connection = engine.newConnection();
        session = (Session) connection.getSession();
        session.setIsolationLevel(IsolationLevel.SERIALIZABLE);
        session.getDatabase().getAllTablesAndViews(true).forEach(table -> tables.put(table.getId(), table));
    }

    public void close() {
        rollback();
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
            }
        }
    }

    @Override
    public void complete() {
        try {
            connection.commit();
        } catch (SQLException e) {
            rollback();
            log.error("Cannot commit execution series {}", e.toString());
        } finally {
            tables.clear();
            connection = null;
            session = null;
        }
    }

    private void acceptSimulated(ExecutedTransaction t) {
        Results results;
        try {
            results = t.getResult().unpack(Results.class);
        } catch (InvalidProtocolBufferException e) {
            rollback();
            log.error("Cannot deserialize execution results", e);
            return;
        }
        results.getResultsList().forEach(result -> process(result));
    }

    private boolean process(Delete delete) {
        return true;
    }

    private boolean process(Insert insert) {
        Table table = tables.get(insert.getTable());
        if (table == null) {
            log.error("Table {} does not exist!", insert.getTable());
            return false;
        }
        Row newRow = rowFrom(insert);
        table.lock(session, true, false);
        try {
            table.addRow(session, newRow);
        } catch (DbException de) {
            log.error("Execution series should proceed without error: {}", de.toString());
            return false;
        }
        session.log(table, UndoLogRecord.INSERT, newRow);
        return true;
    }

    private boolean process(Result result) {
        if (result.hasInsert()) {
            return process(result.getInsert());
        } else if (result.hasUpdate()) {
            return process(result.getUpdate());
        } else {
            return process(result.getDelete());
        }
    }

    private boolean process(Update update) {
        return true;
    }

    private void rollback() {
        if (connection == null) {
            return;
        }
        try {
            connection.rollback();
        } catch (SQLException e) {
        }
        connection = null;
    }

    private Row rowFrom(Insert insert) {
        Value[] data = new Value[insert.getCount()];
        Data buff = Data.create(Capture.NULL, insert.getValues().toByteArray(), true);
        for (int i = 0; i < data.length; i++) {
            data[i] = buff.readValue();
        }
        return new RowImpl(data, Row.MEMORY_CALCULATE);
    }
}
