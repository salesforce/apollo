/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

/**
 * @author hal.hildebrand
 *
 */
public class FireflyMetricsImpl implements FireflyMetrics, BandwidthMetrics {
    private final Meter     borrowRate;
    private final Timer     channelOpenDuration;
    private final Meter     closeConnectionRate;
    private final Counter   createConnection;
    private final Meter     createConnectionRate;
    private final Meter     failedConnectionRate;
    private final Counter   failedOpenConnection;
    private final Histogram gossipReply;
    private final Histogram gossipResponse;
    private final Timer     gossipRoundDuration;
    private final Meter inboundBandwidth;
    private final Histogram inboundGossip;
    private final Meter     inboundGossipRate;
    private final Timer     inboundGossipTimer;
    private final Meter     inboundPingRate;
    private final Histogram inboundUpdate;
    private final Meter     inboundUpdateRate;
    private final Timer     inboundUpdateTimer;
    private final Counter   openConnections;
    private final Meter outboundBandwidth;
    private final Histogram outboundGossip;
    private final Meter     outboundGossipRate;
    private final Timer     outboundGossipTimer;
    private final Meter     outboundPingRate;
    private final Timer     outboundPingTimer;
    private final Histogram outboundUpdate;
    private final Meter     outboundUpdateRate;
    private final Timer     outboundUpdateTimer;
    private final Meter     releaseRate;

    public FireflyMetricsImpl(MetricRegistry registry) {
        inboundBandwidth = registry.meter(INBOUND_BANDWIDTH);
        outboundBandwidth = registry.meter(OUTBOUND_BANDWIDTH);

        outboundPingRate = registry.meter("Outbound Ping Rate");
        inboundPingRate = registry.meter("Inbound Ping Rate");
        outboundPingTimer = registry.timer("Outbound Ping Duration");

        outboundUpdateRate = registry.meter("Outbound Update Rate");
        inboundUpdateRate = registry.meter("Inbound Update Rate");
        outboundUpdateTimer = registry.timer("Inbound Update Duration");
        inboundUpdateTimer = registry.timer("Inbound Update Duration");
        outboundUpdate = registry.histogram("Outbound Update Bytes");
        inboundUpdate = registry.histogram("Inbound Update Bytes");

        outboundGossipRate = registry.meter("Outbound Gossip Rate");
        inboundGossipRate = registry.meter("Inbound Gossip Rate");
        outboundGossipTimer = registry.timer("Outbound Gossip Duration");
        inboundGossipTimer = registry.timer("Inbound Gossip Duration");
        outboundGossip = registry.histogram("Outbound Gossip Bytes");
        gossipResponse = registry.histogram("Inbound Gossip Response Bytes");
        inboundGossip = registry.histogram("Inbound Gossip Bytes");
        gossipReply = registry.histogram("Outbound Gossip Reply Bytes");

        failedOpenConnection = registry.counter("Failed Open Connections");
        createConnection = registry.counter("Connections Created");
        openConnections = registry.counter("Open Connections");
        createConnectionRate = registry.meter("Connection Create Rate");
        closeConnectionRate = registry.meter("Connection Close Rate");
        failedConnectionRate = registry.meter("Failed Connection Rate");
        borrowRate = registry.meter("Connection Borrow Rate");
        releaseRate = registry.meter("Connection Release Rate");

        channelOpenDuration = registry.timer("Outbound Channel Open Duration");
        gossipRoundDuration = registry.timer("Gossip Round Duration");

    }

    @Override
    public Meter borrowRate() {
        return borrowRate;
    }

    @Override
    public Timer channelOpenDuration() {
        return channelOpenDuration;
    }

    @Override
    public Meter closeConnectionRate() {
        return closeConnectionRate;
    }

    @Override
    public Counter createConnection() {
        return createConnection;
    }

    @Override
    public Meter createConnectionRate() {
        return createConnectionRate;
    }

    @Override
    public Meter failedConnectionRate() {
        return failedConnectionRate;
    }

    @Override
    public Counter failedOpenConnection() {
        return failedOpenConnection;
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
    public Timer inboundGossipTimer() {
        return inboundGossipTimer;
    }

    @Override
    public Meter inboundPingRate() {
        return inboundPingRate;
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
    public Timer inboundUpdateTimer() {
        return inboundUpdateTimer;
    }

    @Override
    public Counter openConnections() {
        return openConnections;
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
    public Timer outboundGossipTimer() {
        return outboundGossipTimer;
    }

    @Override
    public Meter outboundPingRate() {
        return outboundPingRate;
    }

    @Override
    public Timer outboundPingTimer() {
        return outboundPingTimer;
    }

    @Override
    public Histogram outboundUpdate() {
        return outboundUpdate;
    }

    @Override
    public Meter outboundUpdateRate() {
        return outboundUpdateRate;
    }

    @Override
    public Timer outboundUpdateTimer() {
        return outboundUpdateTimer;
    }

    @Override
    public Meter releaseRate() {
        return releaseRate;
    }

}
