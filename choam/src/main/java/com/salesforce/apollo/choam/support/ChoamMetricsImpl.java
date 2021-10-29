/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.support;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.salesforce.apollo.comm.RouterMetricsImpl;
import com.salesforce.apollo.ethereal.Ethereal.PreBlock;
import com.salesforce.apollo.ethereal.PreUnit;

/**
 * @author hal.hildebrand
 *
 */
public class ChoamMetricsImpl extends RouterMetricsImpl implements ChoamMetrics {

    public ChoamMetricsImpl(MetricRegistry registry) {
        super(registry);
    }

    @Override
    public void broadcast(PreUnit preUnit) {
        // TODO Auto-generated method stub

    }

    @Override
    public void coordDeserialError() {
        // TODO Auto-generated method stub

    }

    @Override
    public void incTotalMessages() {
        // TODO Auto-generated method stub

    }

    @Override
    public void invalidSourcePid() {
        // TODO Auto-generated method stub

    }

    @Override
    public void invalidUnit() {
        // TODO Auto-generated method stub

    }

    @Override
    public void preBlockProduced(PreBlock preblock) {
        // TODO Auto-generated method stub

    }

    @Override
    public void publishedBatch(int batchSize, int byteSize) {
        // TODO Auto-generated method stub

    }

    @Override
    public void transactionComplete(Throwable t) {
        // TODO Auto-generated method stub

    }

    @Override
    public void transactionTimeout() {
        // TODO Auto-generated method stub

    }

    @Override
    public Timer transactionLatency() {
        // TODO Auto-generated method stub
        return null;
    }

}
