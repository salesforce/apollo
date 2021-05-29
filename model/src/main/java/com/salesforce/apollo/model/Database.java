/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import java.sql.SQLException;

import com.salesforce.apollo.consortium.Consortium;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.state.Mutator;
import com.salesforce.apollo.state.SqlStateMachine;

/**
 * Represents a linear ledger in the system, controls access
 * 
 * @author hal.hildebrand
 *
 */
public class Database {

    private final Consortium      consortium;
    private final SqlStateMachine state;

    public Database(Consortium consortium, SqlStateMachine state) {
        this.consortium = consortium;
        this.state = state;
    }

    public JdbcConnector createConnector() throws SQLException {
        return new JdbcConnector(state.newConnection());
    }

    public HashKey getId() {
        return consortium.getId();
    }

    public Mutator getMutator() {
        return new Mutator(consortium);
    }

    public void start() {
        consortium.start();
    }

    public void stop() {
        consortium.stop();
    }
}
