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

import org.h2.engine.Session;
import org.h2.engine.UndoLogRecord;
import org.h2.jdbc.JdbcConnection;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.RowImpl;
import org.h2.store.Data;
import org.h2.table.Table;
import org.h2.value.Value;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.consortium.proto.ExecutedTransaction;
import com.salesfoce.apollo.state.proto.Delete;
import com.salesfoce.apollo.state.proto.Insert;
import com.salesfoce.apollo.state.proto.Result;
import com.salesfoce.apollo.state.proto.Results;
import com.salesfoce.apollo.state.proto.Update;
import com.salesforce.apollo.consortium.TransactionExecutor;

/**
 * @author hal.hildebrand
 *
 */
public class Updater implements TransactionExecutor {
    private JdbcConnection            connection;
    private final CdcEngine           engine;
    private Session                   session;
    private final Map<Integer, Table> tables = new HashMap<>();

    public Updater(CdcEngine engine) {
        this.engine = engine;
    }

    @Override
    public void accept(ExecutedTransaction t) {
        Results results;
        try {
            results = t.getResult().unpack(Results.class);
        } catch (InvalidProtocolBufferException e) {
            rollback();
            throw new IllegalStateException("Cannot deserialize execution results", e);
        }
        results.getResultsList().forEach(result -> process(result));
    }

    @Override
    public void begin() {
        if (connection != null) {
            throw new IllegalStateException("Execution series has already been established!");
        }
        connection = engine.newConnection();
        session = (Session) connection.getSession();
        session.getDatabase().getAllTablesAndViews(true).forEach(table -> tables.put(table.getId(), table));
    }

    @Override
    public void complete() {
        try {
            connection.commit();
        } catch (SQLException e) {
            rollback();
            throw new IllegalStateException("Cannot commit execution series", e);
        } finally {
            tables.clear();
            connection = null;
            session = null;
        }
    }

    private void process(Delete delete) {

    }

    private void process(Insert insert) {
        Table table = tables.get(insert.getTable());
        if (table == null) {
            throw new IllegalStateException("Table " + insert.getTable() + " does not exist!");
        }
        Row newRow = rowFrom(insert);
        table.lock(session, true, false);
        try {
            table.addRow(session, newRow);
        } catch (DbException de) {
            throw new IllegalStateException("Execution series should proceed without error", de);
        }
        session.log(table, UndoLogRecord.INSERT, newRow);
    }

    private void process(Result result) {
        if (result.hasInsert()) {
            process(result.getInsert());
        } else if (result.hasUpdate()) {
            process(result.getUpdate());
        } else if (result.hasDelete()) {
            process(result.getDelete());
        }
    }

    private void process(Update update) {

    }

    private void rollback() {
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
