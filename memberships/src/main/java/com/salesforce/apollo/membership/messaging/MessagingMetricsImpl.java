/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

/**
 * @author hal.hildebrand
 *
 */
public class MessagingMetricsImpl implements MessagingMetrics {

    private Histogram gossipReply;
    private Histogram gossipResponse;
    private Meter     inboundBandwidth;
    private Histogram inboundGossip;
    private Meter     inboundGossipRate;
    private Histogram inboundUpdate;
    private Meter     inboundUpdateRate;
    private Meter     outboundBandwidth;
    private Histogram outboundGossip;
    private Meter     outboundGossipRate;
    private Histogram outboundUpdate;
    private Meter     outboundUpdateRate;

    public MessagingMetricsImpl(MetricRegistry registry) {
        inboundBandwidth = registry.meter(INBOUND_BANDWIDTH);
        outboundBandwidth = registry.meter(OUTBOUND_BANDWIDTH);
        outboundGossip = registry.histogram("Outbound CausalMessaging Gossip Bytes");
        gossipResponse = registry.histogram("Outbound CausalMessaging Gossip Response Bytes");
        outboundGossipRate = registry.meter("Outbound CausalMessaging Gossip Rate");
        outboundUpdate = registry.histogram("Outbound CausalMessaging Update Bytes");
        outboundUpdateRate = registry.meter("Outbound CausalMessaging Update Rate");
        inboundUpdateRate = registry.meter("Inbound CausalMessaging Update Rate");
        inboundUpdate = registry.histogram("Inbound CausalMessaging Update Bytes");
        inboundGossipRate = registry.meter("Inbound CausalMessaging Gossip Rate");
        inboundGossip = registry.histogram("Inbound Messagging Gossip Bytes");
        gossipReply = registry.histogram("Inbound CausalMessaging Gossip Reply Bytes");
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
    public Meter inboundBandwidth() {
        return inboundBandwidth;
    }

    @Override
    public Histogram inboundGossip() {
        return inboundGossip;
    }

    @Override
    public Meter inboundGossipRate() {
        return inboundGossipRate;
    }

    @Override
    public Histogram inboundUpdate() {
        return inboundUpdate;
    }

    @Override
    public Meter inboundUpdateRate() {
        return inboundUpdateRate;
    }

    @Override
    public Meter outboundBandwidth() {
        return outboundBandwidth;
    }

    @Override
    public Histogram outboundGossip() {
        return outboundGossip;
    }

    @Override
    public Meter outboundGossipRate() {
        return outboundGossipRate;
    }

    @Override
    public Histogram outboundUpdate() {
        return outboundUpdate;
    }

    @Override
    public Meter outboundUpdateRate() {
        return outboundUpdateRate;
    }

}
