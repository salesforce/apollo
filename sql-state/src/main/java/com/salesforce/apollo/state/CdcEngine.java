/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.h2.jdbc.JdbcConnection;

import com.salesforce.apollo.state.h2.CdcSession;
import com.salesforce.apollo.state.jdbc.CdcConnection;

/**
 * @author hal.hildebrand
 *
 */
public class CdcEngine {
    private final CdcSession        capture;
    private final JdbcConnection    connection;
    private final CdcSession        system;
    private final List<Transaction> transactions = new ArrayList<>();

    public CdcEngine(String url, Properties info) throws SQLException {
        connection = new JdbcConnection(url, info);
        capture = (CdcSession) connection.getSession();
        system = (CdcSession) capture.getDatabase().getSystemSession();
    }

    public Connection newTransaction() {
        Transaction t = new Transaction();
        transactions.add(t);
        capture.setCdc((table, op, row) -> t.log(table, op, row));
        system.setCdc((table, op, row) -> t.log(table, op, row));
        try {
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot set base connection to autocommit: false");
        }
        return new CdcConnection(connection);
    }
}
