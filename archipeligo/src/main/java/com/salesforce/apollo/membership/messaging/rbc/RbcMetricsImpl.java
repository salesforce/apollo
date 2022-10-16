/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging.rbc;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.protocols.EndpointMetricsImpl;

/**
 * @author hal.hildebrand
 *
 */
public class RbcMetricsImpl extends EndpointMetricsImpl implements RbcMetrics {
    private final Histogram gossipReply;
    private final Histogram gossipResponse;
    private final Timer     gossipRoundDuration;
    private final Histogram inboundGossip;
    private final Timer     inboundGossipTimer;
    private final Histogram inboundUpdate;
    private final Timer     inboundUpdateTimer;
    private final Histogram outboundGossip;
    private final Timer     outboundGossipTimer;
    private final Histogram outboundUpdate;
    private final Timer     outboundUpdateTimer;

    public RbcMetricsImpl(Digest context, String system, MetricRegistry registry) {
        super(registry);
        outboundUpdateTimer = registry.timer(name(context.shortString(), system, "rbc.update.outbound.duration"));
        inboundUpdateTimer = registry.timer(name(context.shortString(), system, "rbc.update.inbound.duration"));
        outboundUpdate = registry.histogram(name(context.shortString(), system, "rbc.update.outbound.bytes"));
        inboundUpdate = registry.histogram(name(context.shortString(), system, "rbc.update.inbound.bytes"));

        outboundGossipTimer = registry.timer(name(context.shortString(), system, "rbc.gossip.outbound.duration"));
        inboundGossipTimer = registry.timer(name(context.shortString(), system, "rbc.gossip.inbound.duration"));
        outboundGossip = registry.histogram(name(context.shortString(), system, "rbc.gossip.outbound.bytes"));
        gossipResponse = registry.histogram(name(context.shortString(), system, "rbc.gossip.reply.inbound.bytes"));
        inboundGossip = registry.histogram(name(context.shortString(), system, "rbc.gossip.inbound.bytes"));
        gossipReply = registry.histogram(name(context.shortString(), system, "rbc.gossip.reply.outbound.bytes"));
        gossipRoundDuration = registry.timer(name(context.shortString(), system, "rbc.gossip.round.duration"));
    }

    @Override
    public Histogram gossipReply() {
        return gossipReply;
    }

    @Override
    public Histogram gossipResponse() {
        return gossipResponse;
    }

    @Override
    public Timer gossipRoundDuration() {
        return gossipRoundDuration;
    }

    @Override
    public Histogram inboundGossip() {
        return inboundGossip;
    }

    @Override
    public Timer inboundGossipTimer() {
        return inboundGossipTimer;
    }

    @Override
    public Histogram inboundUpdate() {
        return inboundUpdate;
    }

    @Override
    public Timer inboundUpdateTimer() {
        return inboundUpdateTimer;
    }

    @Override
    public Histogram outboundGossip() {
        return outboundGossip;
    }

    @Override
    public Timer outboundGossipTimer() {
        return outboundGossipTimer;
    }

    @Override
    public Histogram outboundUpdate() {
        return outboundUpdate;
    }

    @Override
    public Timer outboundUpdateTimer() {
        return outboundUpdateTimer;
    }
}
