/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state.h2;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.h2.jdbc.JdbcConnection;
import org.h2.result.Row;
import org.h2.table.Table;

import com.salesforce.apollo.state.h2.CdcSession.CdcEvent;

/**
 * @author hal.hildebrand
 *
 */
public class CdcEngine implements Cdc {
    private final JdbcConnection connection;
    private final List<CdcEvent> changes       = new ArrayList<>();
    private final List<CdcEvent> systemChanges = new ArrayList<>();
    private final CdcSession     systemSession;

    public CdcEngine(String url, Properties info) throws SQLException {
        connection = new JdbcConnection(url, info);
        CdcSession capture = (CdcSession) connection.getSession();
        capture.setCdc((table, operation, row) -> changes.add(new CdcEvent(table, operation, row)));

        systemSession = (CdcSession) capture.getDatabase().getSystemSession();
        systemSession.setCdc((table, operation, row) -> systemChanges.add(new CdcEvent(table, operation, row)));

    }

    @Override
    public void log(Table table, short operation, Row row) {
        changes.add(new CdcEvent(table, operation, row));
    }

    public Connection getConnection() {
        return connection;
    }
}
