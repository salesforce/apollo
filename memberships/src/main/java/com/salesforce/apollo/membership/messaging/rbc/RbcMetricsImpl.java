/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging.rbc;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.salesforce.apollo.protocols.BandwidthMetricsImpl;

/**
 * @author hal.hildebrand
 *
 */
public class RbcMetricsImpl extends BandwidthMetricsImpl implements RbcMetrics {
    private final Meter gossipReply;
    private final Meter gossipResponse;
    private final Timer gossipRoundDuration;
    private final Meter inboundGossip;
    private final Timer inboundGossipTimer;
    private final Meter inboundUpdate;
    private final Timer inboundUpdateTimer;
    private final Meter outboundGossip;
    private final Timer outboundGossipTimer;
    private final Meter outboundUpdate;
    private final Timer outboundUpdateTimer;

    public RbcMetricsImpl(MetricRegistry registry) {
        super(registry);
        outboundUpdateTimer = registry.timer("rbc.update.outbound.duration");
        inboundUpdateTimer = registry.timer("rbc.update.inbound.duration");
        outboundUpdate = registry.meter("rbc.update.outbound.bytes");
        inboundUpdate = registry.meter("rbc.update.inbound.bytes");

        outboundGossipTimer = registry.timer("rbc.gossip.outbound.duration");
        inboundGossipTimer = registry.timer("rbc.gossip.inbound.duration");
        outboundGossip = registry.meter("rbc.gossip.outbound.bytes");
        gossipResponse = registry.meter("rbc.gossip.reply.inbound.bytes");
        inboundGossip = registry.meter("rbc.gossip.inbound.bytes");
        gossipReply = registry.meter("rbc.gossip.reply.outbound.bytes");
        gossipRoundDuration = registry.timer("rbc.gossip.round.duration");

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
    public Meter outboundUpdate() {
        return outboundUpdate;
    }

    @Override
    public Timer outboundUpdateTimer() {
        return outboundUpdateTimer;
    }
}
