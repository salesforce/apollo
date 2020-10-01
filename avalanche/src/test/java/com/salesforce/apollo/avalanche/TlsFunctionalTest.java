/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.avalanche;

import com.salesforce.apollo.comm.Communications;
import com.salesforce.apollo.comm.MtlsCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.comm.ServerConnectionCache.ServerConnectionCacheBuilder;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.fireflies.View;

/**
 * @author hhildebrand
 */
public class TlsFunctionalTest extends AvalancheFunctionalTest {

    protected Communications getCommunications(Node node) {
        ServerConnectionCacheBuilder builder = ServerConnectionCache.newBuilder().setTarget(30);
        return new MtlsCommunications(builder, View.getStandardEpProvider(node));
    }

    @Override
    protected int testCardinality() {
        return 14;
    }
}
