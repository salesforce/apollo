/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.protocols;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

/**
 * @author hal.hildebrand
 *
 */
public class BandwidthMetricsImpl implements BandwidthMetrics {
    private final Meter inboundBandwidth;
    private final Meter outboundBandwidth;

    public BandwidthMetricsImpl(MetricRegistry registry) {
        inboundBandwidth = registry.meter(INBOUND_BANDWIDTH);
        outboundBandwidth = registry.meter(OUTBOUND_BANDWIDTH);
    }

    @Override
    public Meter inboundBandwidth() {
        return inboundBandwidth;
    }

    @Override
    public Meter outboundBandwidth() {
        return outboundBandwidth;
    }
}
