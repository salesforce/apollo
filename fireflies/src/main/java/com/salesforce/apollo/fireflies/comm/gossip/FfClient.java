/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.comm.gossip;

import java.util.concurrent.ExecutionException;

import com.codahale.metrics.Timer.Context;
import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.fireflies.proto.FirefliesGrpc;
import com.salesfoce.apollo.fireflies.proto.FirefliesGrpc.FirefliesFutureStub;
import com.salesfoce.apollo.fireflies.proto.Gossip;
import com.salesfoce.apollo.fireflies.proto.SayWhat;
import com.salesfoce.apollo.fireflies.proto.State;
import com.salesforce.apollo.archipelago.ManagedServerChannel;
import com.salesforce.apollo.archipelago.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.fireflies.FireflyMetrics;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class FfClient implements Fireflies {

    public static CreateClientCommunications<Fireflies> getCreate(FireflyMetrics metrics) {
        return (c) -> new FfClient(c, metrics);

    }

    private final ManagedServerChannel channel;
    private final FirefliesFutureStub  client;
    private final FireflyMetrics       metrics;

    public FfClient(ManagedServerChannel channel, FireflyMetrics metrics) {
        this.channel = channel;
        this.client = FirefliesGrpc.newFutureStub(channel).withCompression("gzip");
        this.metrics = metrics;
    }

    @Override
    public void close() {
        channel.release();
    }

    @Override
    public Member getMember() {
        return channel.getMember();
    }

    @Override
    public ListenableFuture<Gossip> gossip(SayWhat sw) {
        ListenableFuture<Gossip> result = client.gossip(sw);
        if (metrics != null) {
            var serializedSize = sw.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundGossip().update(serializedSize);
        }
        result.addListener(() -> {
            if (metrics != null) {
                try {
                    var serializedSize = result.get().getSerializedSize();
                    metrics.inboundBandwidth().mark(serializedSize);
                    metrics.gossipResponse().update(serializedSize);
                } catch (InterruptedException | ExecutionException e) {
                    // nothing
                }
            }
        }, r -> r.run());
        return result;
    }

    public void release() {
        close();
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
