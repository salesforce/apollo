/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletionStage;

import com.salesfoce.apollo.state.proto.Batch;
import com.salesfoce.apollo.state.proto.BatchUpdate;
import com.salesfoce.apollo.state.proto.Call;
import com.salesfoce.apollo.state.proto.Script;
import com.salesfoce.apollo.state.proto.Statement;
import com.salesforce.apollo.consortium.Consortium;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.state.SqlStateMachine;
import com.salesforce.apollo.state.SqlStateMachine.CallResult;

/**
 * Represents a linear ledger in the system, controls access
 * 
 * @author hal.hildebrand
 *
 */
public class Database {
    public class DatabaseWriteConnection {
        
        public HashKey submit(Batch.Builder batch, CompletionStage<HashKey> submitted) {
            consortium.submit(null, null, null);
            return null;
        }

        public CompletionStage<int[]> execute(BatchUpdate update, Batch.Builder batch) {
            return null;
        }

        public CompletionStage<CallResult> execute(Call call, Batch.Builder batch) {
            return null;
        }

        public <T> CompletionStage<T> execute(Script script, Batch.Builder batch) {
            return null;
        }

        public CompletionStage<List<ResultSet>> execute(Statement statement, Batch.Builder batch) {
            return null;
        }
    }

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
}
