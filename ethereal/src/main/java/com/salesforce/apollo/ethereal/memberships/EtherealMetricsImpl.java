/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.memberships;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.salesforce.apollo.protocols.BandwidthMetrics;

/**
 * @author hal.hildebrand
 *
 */
public class EtherealMetricsImpl implements EtherealMetrics, BandwidthMetrics {
    private final Meter inboundBandwidth;
    private final Meter gossipReply;
    private final Meter gossipResponse;
    private final Timer gossipRoundDuration;
    private final Meter inboundGossip;
    private final Timer inboundGossipTimer;
    private final Meter inboundUpdate;
    private final Timer inboundUpdateTimer;
    private final Meter outboundBandwidth;
    private final Meter outboundGossip;
    private final Timer outboundGossipTimer;
    private final Meter outboundUpdate;
    private final Timer outboundUpdateTimer;

    public EtherealMetricsImpl(MetricRegistry registry) {
        inboundBandwidth = registry.meter(INBOUND_BANDWIDTH);
        outboundBandwidth = registry.meter(OUTBOUND_BANDWIDTH);

        outboundUpdateTimer = registry.timer("ethereal.update.outbound.duration");
        inboundUpdateTimer = registry.timer("ethereal.update.inbound.duration");
        outboundUpdate = registry.meter("ethereal.update.outbound.bytes");
        inboundUpdate = registry.meter("ethereal.update.inbound.bytes");

        outboundGossipTimer = registry.timer("ethereal.outbound.gossip.duration");
        inboundGossipTimer = registry.timer("ethereal.inbound.gossip.duration");
        outboundGossip = registry.meter("ethereal.gossip.oubound.bytes");
        gossipResponse = registry.meter("ethereal.gossip.response.bytes");
        inboundGossip = registry.meter("ethereal.gossip.inbound.bytes");
        gossipReply = registry.meter("ethereal.gossip.reply.bytes");
        gossipRoundDuration = registry.timer("ethereal.gossip.round.durration");

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
    public Meter inboundBandwidth() {
        return inboundBandwidth;
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
    public Meter inboundUpdate() {
        return inboundUpdate;
    }

    @Override
    public Timer inboundUpdateTimer() {
        return inboundUpdateTimer;
    }

    @Override
    public Meter outboundBandwidth() {
        return outboundBandwidth;
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
    public Meter outboundUpdate() {
        return outboundUpdate;
    }

    @Override
    public Timer outboundUpdateTimer() {
        return outboundUpdateTimer;
    }
}
