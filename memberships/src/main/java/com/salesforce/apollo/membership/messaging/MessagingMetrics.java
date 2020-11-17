/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.salesforce.apollo.protocols.BandwidthMetrics;

/**
 * @author hal.hildebrand
 *
 */
public interface MessagingMetrics extends BandwidthMetrics {

    Histogram outboundGossip();

    Histogram gossipResponse();

    Meter outboundGossipRate();

    Histogram outboundUpdate();

    Meter outboundUpdateRate();

    Meter inboundUpdateRate();

    Histogram inboundUpdate();

    Meter inboundGossipRate();

    Histogram inboundGossip();

    Histogram gossipReply();

}
