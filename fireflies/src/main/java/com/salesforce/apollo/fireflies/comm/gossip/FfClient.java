/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.comm.gossip;

import com.codahale.metrics.Timer.Context;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.salesforce.apollo.archipelago.ManagedServerChannel;
import com.salesforce.apollo.archipelago.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.fireflies.FireflyMetrics;
import com.salesforce.apollo.fireflies.proto.*;
import com.salesforce.apollo.membership.Member;

import java.time.Duration;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class FfClient implements Fireflies {

    private final ManagedServerChannel                channel;
    private final FirefliesGrpc.FirefliesBlockingStub client;
    private final FireflyMetrics                      metrics;

    public FfClient(ManagedServerChannel channel, FireflyMetrics metrics) {
        this.channel = channel;
        this.client = channel.wrap(FirefliesGrpc.newBlockingStub(channel));
        this.metrics = metrics;
    }

    public static CreateClientCommunications<Fireflies> getCreate(FireflyMetrics metrics) {
        return (c) -> new FfClient(c, metrics);

    }

    @Override
    public void close() {
        channel.release();
    }

    @Override
    public Void enjoin(Join join) {
        channel.wrap(FirefliesGrpc.newFutureStub(channel)).enjoin(join);
        return null;
    }

    @Override
    public Member getMember() {
        return channel.getMember();
    }

    @Override
    public Gossip gossip(SayWhat sw) {
        if (metrics != null) {
            var serializedSize = sw.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundGossip().update(serializedSize);
        }
        var result = client.gossip(sw);
        if (metrics != null) {
            var serializedSize = result.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.gossipResponse().update(serializedSize);
        }
        return result;
    }

    @Override
    public ListenableFuture<Empty> ping(Ping ping, Duration timeout) {
        return channel.wrap(FirefliesGrpc.newFutureStub(channel)).ping(ping);
    }

    @Override
    public String toString() {
        return String.format("->[%s]", channel.getMember());
    }

    @Override
    public void update(State state) {
        Context timer = null;
        if (metrics != null) {
            timer = metrics.outboundUpdateTimer().time();
        }
        client.update(state);
        if (metrics != null) {
            var serializedSize = state.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundUpdate().update(serializedSize);
            timer.stop();
        }
    }
}
