/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.salesforce.apollo.protocols.BandwidthMetricsImpl;

/**
 * @author hal.hildebrand
 *
 */
public class FireflyMetricsImpl extends BandwidthMetricsImpl implements FireflyMetrics {
    private final Meter gossipReply;
    private final Meter gossipResponse;
    private final Timer gossipRoundDuration;
    private final Meter inboundGossip;
    private final Timer inboundGossipTimer;
    private final Meter inboundPingRate;
    private final Meter inboundUpdate;
    private final Timer inboundUpdateTimer;
    private final Meter outboundGossip;
    private final Timer outboundGossipTimer;
    private final Timer outboundPingRate;
    private final Meter outboundUpdate;
    private final Timer outboundUpdateTimer;

    public FireflyMetricsImpl(MetricRegistry registry) {
        super(registry);
        outboundUpdateTimer = registry.timer("ff.update.outbound.duration");
        inboundUpdateTimer = registry.timer("ff.update.inbound.duration");
        outboundUpdate = registry.meter("ff.update.outbound.bytes");
        inboundUpdate = registry.meter("ff.update.inbound.bytes");

        outboundGossipTimer = registry.timer("ff.gossip.outbound.duration");
        inboundGossipTimer = registry.timer("ff.gossip.inbound.duration");
        outboundGossip = registry.meter("ff.gossip.outbound.bytes");
        gossipResponse = registry.meter("ff.gossip.reply.inbound.bytes");
        inboundGossip = registry.meter("ff.gossip.inbound.bytes");
        gossipReply = registry.meter("ff.gossip.reply.outbound.bytes");
        gossipRoundDuration = registry.timer("ff.gossip.round.duration");

        outboundPingRate = registry.timer("ff.ping.outbound.duration");
        inboundPingRate = registry.meter("ff.ping.inbound");

    }

    @Override
    public Meter gossipReply() {
        return gossipReply;
    }

    @Override
    public Meter gossipResponse() {
        return gossipResponse;
    }

    @Override
    public Timer gossipRoundDuration() {
        return gossipRoundDuration;
    }

    @Override
    public Meter inboundGossip() {
        return inboundGossip;
    }

    @Override
    public Timer inboundGossipTimer() {
        return inboundGossipTimer;
    }

    @Override
    public Meter inboundPingRate() {
        return inboundPingRate;
    }

    @Override
    public Meter inboundUpdate() {
        return inboundUpdate;
    }

    @Override
    public Timer inboundUpdateTimer() {
        return inboundUpdateTimer;
    }

    @Override
    public Meter outboundGossip() {
        return outboundGossip;
    }

    @Override
    public Timer outboundGossipTimer() {
        return outboundGossipTimer;
    }

    @Override
    public Timer outboundPingRate() {
        return outboundPingRate;
    }

    @Override
    public Meter outboundUpdate() {
        return outboundUpdate;
    }

    @Override
    public Timer outboundUpdateTimer() {
        return outboundUpdateTimer;
    }
}
