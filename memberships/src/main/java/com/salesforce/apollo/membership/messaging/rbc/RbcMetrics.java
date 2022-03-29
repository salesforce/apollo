/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging.rbc;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.salesforce.apollo.protocols.EdpointMetrics;

/**
 * @author hal.hildebrand
 *
 */
public interface RbcMetrics extends EdpointMetrics {

    Meter gossipReply();

    Meter gossipResponse();

    Timer gossipRoundDuration();

    Meter inboundGossip();

    Timer inboundGossipTimer();

    Meter inboundUpdate();

    Timer inboundUpdateTimer();

    Meter outboundGossip();

    Timer outboundGossipTimer();

    Meter outboundUpdate();

    Timer outboundUpdateTimer();
}
