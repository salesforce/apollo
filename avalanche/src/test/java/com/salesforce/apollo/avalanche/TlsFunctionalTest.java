/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.avalanche;

import com.salesforce.apollo.comm.MtlsRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.comm.ServerConnectionCache.Builder;
import com.salesforce.apollo.fireflies.FireflyMetricsImpl;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.fireflies.View;

/**
 * @author hhildebrand
 */
public class TlsFunctionalTest extends AvalancheFunctionalTest {

    @Override
    protected Router getCommunications(Node node, boolean first) {
        Builder builder = ServerConnectionCache.newBuilder()
                                               .setTarget(30)
                                               .setMetrics(new FireflyMetricsImpl(first ? node0registry : registry));
        return new MtlsRouter(builder, View.getStandardEpProvider(node));
    }

    @Override
    protected int testCardinality() {
        return 14;
    }
}
