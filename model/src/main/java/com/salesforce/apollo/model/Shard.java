/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import java.sql.Connection;
import java.sql.SQLException;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.state.Mutator;

/**
 * @author hal.hildebrand
 *
 */
public interface Shard {

    Connection createConnection() throws SQLException;

    Digest getId();

    Mutator getMutator();

    void start();

    void stop();

}
