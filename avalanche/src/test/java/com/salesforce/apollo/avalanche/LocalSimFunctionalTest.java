/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.avalanche;

import org.junit.jupiter.api.AfterEach;

import com.salesforce.apollo.comm.Communications;
import com.salesforce.apollo.comm.LocalCommSimm;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.fireflies.FireflyMetricsImpl;
import com.salesforce.apollo.fireflies.Node;

/**
 * @author hhildebrand
 */
public class LocalSimFunctionalTest extends AvalancheFunctionalTest {

    private LocalCommSimm comms;

    @AfterEach
    public void after() {
        comms = null;
    }

    protected Communications getCommunications(Node node, boolean first) {
        if (comms == null) {
            comms = new LocalCommSimm(
                    ServerConnectionCache.newBuilder().setTarget(30).setMetrics(new FireflyMetricsImpl(node0registry)));
        }
        return comms;
    }

    @Override
    protected int testCardinality() {
        return 31;
    }
}
