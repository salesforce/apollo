/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.protocols.EndpointMetricsImpl;

/**
 * @author hal.hildebrand
 *
 */
public class FireflyMetricsImpl extends EndpointMetricsImpl implements FireflyMetrics {
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

    public FireflyMetricsImpl(Digest context, MetricRegistry registry) {
        super(registry);
        outboundUpdateTimer = registry.timer(name(context.shortString(), "ff.update.outbound.duration"));
        inboundUpdateTimer = registry.timer(name(context.shortString(), "ff.update.inbound.duration"));
        outboundUpdate = registry.meter(name(context.shortString(), "ff.update.outbound.bytes"));
        inboundUpdate = registry.meter(name(context.shortString(), "ff.update.inbound.bytes"));

        outboundGossipTimer = registry.timer(name(context.shortString(), "ff.gossip.outbound.duration"));
        inboundGossipTimer = registry.timer(name(context.shortString(), "ff.gossip.inbound.duration"));
        outboundGossip = registry.meter(name(context.shortString(), "ff.gossip.outbound.bytes"));
        gossipResponse = registry.meter(name(context.shortString(), "ff.gossip.reply.inbound.bytes"));
        inboundGossip = registry.meter(name(context.shortString(), "ff.gossip.inbound.bytes"));
        gossipReply = registry.meter(name(context.shortString(), "ff.gossip.reply.outbound.bytes"));
        gossipRoundDuration = registry.timer(name(context.shortString(), "ff.gossip.round.duration"));

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
