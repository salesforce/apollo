/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;

/**
 * @author hal.hildebrand
 *
 */
public interface MessagingMetrics {

    Meter outboundBandwidth();

    Meter inboundBandwidth();

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
