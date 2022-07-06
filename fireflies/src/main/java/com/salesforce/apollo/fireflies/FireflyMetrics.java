/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.salesforce.apollo.protocols.EdpointMetrics;

/**
 * @author hal.hildebrand
 *
 */
public interface FireflyMetrics extends EdpointMetrics {

    Meter accusations();

    Meter filteredNotes();

    Histogram gateway();

    Histogram gossipReply();

    Histogram gossipResponse();

    Timer gossipRoundDuration();

    Histogram inboundGossip();

    Timer inboundGossipDuration();

    Histogram inboundJoin();

    Timer inboundJoinDuration();

    Histogram inboundSeed();

    Timer inboundSeedDuration();

    Histogram inboundUpdate();

    Timer inboundUpdateTimer();

    Histogram join();

    Timer joinDuration();

    Meter joins();

    Meter leaves();

    Meter notes();

    Histogram outboundGateway();

    Histogram outboundGossip();

    Histogram outboundRedirect();

    Histogram outboundUpdate();

    Timer outboundUpdateTimer();

    Histogram redirect();

    Histogram seed();

    Timer seedDuration();

    Meter shunnedGossip();
}
