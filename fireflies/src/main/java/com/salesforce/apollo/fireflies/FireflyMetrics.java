/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.salesforce.apollo.protocols.EdpointMetrics;

/**
 * @author hal.hildebrand
 *
 */
public interface FireflyMetrics extends EdpointMetrics {

    Meter accusations();

    Meter gossipReply();

    Meter gossipResponse();

    Timer gossipRoundDuration();

    Meter inboundGossip();

    Timer inboundGossipTimer();

    Meter inboundUpdate();

    Timer inboundUpdateTimer();

    Meter notes();

    Meter outboundGossip();

    Meter outboundUpdate();

    Timer outboundUpdateTimer();
}
