/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model.stereotomy;

import java.sql.Connection;
import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.state.Mutator;

/**
 * @author hal.hildebrand
 *
 */
public class ShardedKERLTest {

    @Test
    public void func() throws Exception {
        Connection connection = null;
        Mutator mutator = null;
        ScheduledExecutorService scheduler = null;
        Duration timeout = null;
        DigestAlgorithm algo = null;
        Executor exec = null;

        ShardedKERL kerl = new ShardedKERL(connection, mutator, scheduler, timeout, algo, exec);
    }
}
