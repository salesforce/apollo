/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.memberships.comm;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.protocols.EdpointMetrics;
import com.salesforce.apollo.protocols.EndpointMetricsImpl;

/**
 * @author hal.hildebrand
 *
 */
public class EtherealMetricsImpl extends EndpointMetricsImpl implements EtherealMetrics, EdpointMetrics {
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

    public EtherealMetricsImpl(Digest context, String system, MetricRegistry registry) {
        super(registry);
        outboundUpdateTimer = registry.timer(name(context.shortString(), system, "ethereal.update.outbound.duration"));
        outboundUpdate = registry.meter(name(context.shortString(), system, "ethereal.update.outbound.bytes"));

        inboundUpdateTimer = registry.timer(name(context.shortString(), system, "ethereal.update.inbound.duration"));
        inboundUpdate = registry.meter(name(context.shortString(), system, "ethereal.update.inbound.bytes"));

        outboundGossipTimer = registry.timer(name(context.shortString(), system, "ethereal.gossip.outbound.duration"));
        outboundGossip = registry.meter(name(context.shortString(), system, "ethereal.gossip.oubound.bytes"));
        gossipResponse = registry.meter(name(context.shortString(), system, "ethereal.gossip.response.bytes"));

        inboundGossipTimer = registry.timer(name(context.shortString(), system, "ethereal.gossip.inbound.duration"));
        inboundGossip = registry.meter(name(context.shortString(), system, "ethereal.gossip.inbound.bytes"));
        gossipReply = registry.meter(name(context.shortString(), system, "ethereal.gossip.reply.bytes"));

        gossipRoundDuration = registry.timer(name(context.shortString(), system, "ethereal.gossip.round.duration"));

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
