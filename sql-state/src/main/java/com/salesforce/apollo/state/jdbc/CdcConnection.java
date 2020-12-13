/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;

import com.salesforce.apollo.state.CdcEngine;

/**
 * @author hal.hildebrand
 *
 */
public class CdcConnection extends DelegatingConnection {
    private final CdcEngine engine;

    public CdcConnection(CdcEngine engine, Connection cdcConnection) {
        super(cdcConnection);
        this.engine = engine;
    }

    @Override
    public void commit() throws SQLException {
        // Commit is just continuation of the generation of the block CDC state,
        // demarcated by savepoints in the session
    }

    @Override
    public void rollback() throws SQLException {
        engine.rollback();
        super.rollback();
    }

    @Override
    public void rollback(Savepoint arg0) throws SQLException {
        super.rollback(arg0);
    }

    @Override
    public void setAutoCommit(boolean a) throws SQLException {
        if (a) {
            throw new SQLException("auto commit not allowed for this connection");
        }
    }

}
