/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import java.sql.Connection;
import java.sql.SQLException;

import com.salesforce.apollo.choam.CHOAM;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.state.Mutator;
import com.salesforce.apollo.state.SqlStateMachine;
import com.salesforce.apollo.utils.DelegatingJdbcConnector;

/**
 * Represents a linear ledger in the system, controls access
 * 
 * @author hal.hildebrand
 *
 */
public class Shard {
    public static class JdbcConnector extends DelegatingJdbcConnector {

        public JdbcConnector(Connection wrapped) throws SQLException {
            super(wrapped);
            wrapped.setReadOnly(true);
            wrapped.setAutoCommit(false);
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

    private final CHOAM           choam;
    private final SqlStateMachine state;

    public Shard(CHOAM consortium, SqlStateMachine state) {
        this.choam = consortium;
        this.state = state;
    }

    public Connection createConnection() throws SQLException {
        return new JdbcConnector(state.newConnection());
    }

    public Digest getId() {
        return choam.getId();
    }

    public Mutator getMutator() {
        return new Mutator(choam.getSession());
    }

    public void start() {
        choam.start();
    }

    public void stop() {
        choam.stop();
    }

    Context<? extends Member> context() {
        return choam.context();
    }
}
