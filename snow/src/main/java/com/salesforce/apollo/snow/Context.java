/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow;

import org.slf4j.Logger;

import com.codahale.metrics.MetricRegistry;
import com.salesforce.apollo.snow.ids.ID;
import com.salesforce.apollo.snow.ids.ShortID;

/**
 * @author hal.hildebrand
 *
 */
public class Context {
    public final int            networkID;
    public final int            subnetID;
    public final int            chainID;
    public final ShortID        nodeID;
    public final ID             xchainID;
    public final ID             avaxAssetID;
    public final Logger         log;
    public final int            bootstrapped;
    public final String         namespace;
    public final MetricRegistry metrics;

    public Context(int networkID, int subnetID, int chainID, ShortID nodeID, ID xchainID, ID avaxAssetID, Logger log,
            int bootstrapped, String namespace, MetricRegistry metrics) {
        this.networkID = networkID;
        this.subnetID = subnetID;
        this.chainID = chainID;
        this.nodeID = nodeID;
        this.xchainID = xchainID;
        this.avaxAssetID = avaxAssetID;
        this.log = log;
        this.bootstrapped = bootstrapped;
        this.namespace = namespace;
        this.metrics = metrics;
    }

}
