/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;

import com.codahale.metrics.MetricRegistry;
import com.salesforce.apollo.snow.ids.ID;
import com.salesforce.apollo.snow.ids.ShortID;

/**
 * @author hal.hildebrand
 *
 */
public class Context {
    public final int             networkID;
    public final int             subnetID;
    public final int             chainID;
    public final ShortID         nodeID;
    public final ID              xchainID;
    public final ID              avaxAssetID;
    public final Logger          log;
    private final AtomicBoolean  bootstrapped = new AtomicBoolean();
    public final String          namespace;
    public final MetricRegistry  metrics;
    public final EventDispatcher decisionDispatcher;
    public final EventDispatcher consensusDispatcher;

    public Context(int networkID, int subnetID, int chainID, ShortID nodeID, ID xchainID, ID avaxAssetID, Logger log,
            String namespace, MetricRegistry metrics, EventDispatcher decisionDispatcher,
            EventDispatcher consensusDispatcher) {
        this.networkID = networkID;
        this.subnetID = subnetID;
        this.chainID = chainID;
        this.nodeID = nodeID;
        this.xchainID = xchainID;
        this.avaxAssetID = avaxAssetID;
        this.log = log;
        this.namespace = namespace;
        this.metrics = metrics;
        this.consensusDispatcher = consensusDispatcher;
        this.decisionDispatcher = decisionDispatcher;
    }

    public boolean isBootstrapped() {
        return bootstrapped.get();
    }

    public void bootstrap() {
        bootstrapped.set(true);
    }
}
