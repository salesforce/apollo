/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

/**
 * @author hal.hildebrand
 *
 */
public interface GossipMetrics {

    Histogram gossipReply();

    Histogram gossipResponse();

    Timer gossipRoundDuration();

    Histogram inboundGossip();

    Meter inboundGossipRate();

    Timer inboundGossipTimer();

    Meter inboundPingRate();

    Histogram inboundUpdate();

    Meter inboundUpdateRate();

    Timer inboundUpdateTimer();

    Histogram outboundGossip();

    Meter outboundGossipRate();

    Timer outboundGossipTimer();

    Meter outboundPingRate();

    Timer outboundPingTimer();

    Histogram outboundUpdate();

    Meter outboundUpdateRate();

    Timer outboundUpdateTimer();
}
