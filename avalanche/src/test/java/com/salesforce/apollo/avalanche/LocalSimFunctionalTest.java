/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.avalanche;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;

import com.salesforce.apollo.comm.Communications;
import com.salesforce.apollo.comm.LocalCommSimm;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.comm.ServerConnectionCache.ServerConnectionCacheBuilder;
import com.salesforce.apollo.fireflies.FireflyMetricsImpl;
import com.salesforce.apollo.fireflies.Node;

/**
 * @author hhildebrand
 */
public class LocalSimFunctionalTest extends AvalancheFunctionalTest {

    private final List<Communications>   comms   = new ArrayList<>();
    private ServerConnectionCacheBuilder builder = ServerConnectionCache.newBuilder().setTarget(30);

    @AfterEach
    public void after() {
        comms.forEach(e -> e.close());
        comms.clear();
    }

    protected Communications getCommunications(Node node, boolean first) {
        return new LocalCommSimm(builder.setMetrics(new FireflyMetricsImpl(first ? node0registry : registry)),
                node.getId());
    }

    @Override
    protected int testCardinality() {
        return 31;
    }
}
