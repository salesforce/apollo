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

/**
 * @author hal.hildebrand
 *
 */
public class CHOAMShard implements Shard {

    private final CHOAM           choam;
    private final SqlStateMachine state;

    public CHOAMShard(CHOAM consortium, SqlStateMachine state) {
        this.choam = consortium;
        this.state = state;
    }

    @Override
    public Connection createConnection() throws SQLException {
        return state.newConnection();
    }

    @Override
    public Digest getId() {
        return choam.getId();
    }

    @Override
    public Mutator getMutator() {
        return state.getMutator(choam.getSession());
    }

    @Override
    public void start() {
        choam.start();
    }

    @Override
    public void stop() {
        choam.stop();
    }

    Context<? extends Member> context() {
        return choam.context();
    }
}
