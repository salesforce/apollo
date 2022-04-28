/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state.liquibase;

import java.sql.Connection;
import java.sql.SQLException;

import com.salesforce.apollo.utils.DelegatingJdbcConnector;

/**
 * @author hal.hildebrand
 *
 */
public class LiquibaseConnection extends DelegatingJdbcConnector {

    public LiquibaseConnection(Connection wrapped) {
        super(wrapped);
    }

    @Override
    public void close() throws SQLException {
        // no op
    }

    public boolean getAutoCommit() throws SQLException {
        return false;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (autoCommit) {
            throw new SQLException("Cannot set autocommit on this connection");
        }
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        if (!readOnly) {
            throw new SQLException("This is a read only connection");
        }
    }

    public void setTransactionIsolation(int level) throws SQLException {
        throw new SQLException("Cannot set transaction isolation level on this connection");
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("Cannot unwrap: " + iface.getCanonicalName() + "on th connection");
    }
}
