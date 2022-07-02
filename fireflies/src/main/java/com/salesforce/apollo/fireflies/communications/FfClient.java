/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.communications;

import java.util.concurrent.ExecutionException;

import com.codahale.metrics.Timer.Context;
import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.fireflies.proto.FirefliesGrpc;
import com.salesfoce.apollo.fireflies.proto.FirefliesGrpc.FirefliesFutureStub;
import com.salesfoce.apollo.fireflies.proto.Gossip;
import com.salesfoce.apollo.fireflies.proto.SayWhat;
import com.salesfoce.apollo.fireflies.proto.State;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.fireflies.FireflyMetrics;
import com.salesforce.apollo.fireflies.View.Node;
import com.salesforce.apollo.fireflies.View.Participant;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class FfClient implements Fireflies {

    public static CreateClientCommunications<Fireflies> getCreate(FireflyMetrics metrics) {
        return (t, f, c) -> new FfClient(c, (Participant) t, metrics);

    }

    private final ManagedServerConnection channel;
    private final FirefliesFutureStub     client;
    private final Participant             member;
    private final FireflyMetrics          metrics;

    public FfClient(ManagedServerConnection channel, Participant member, FireflyMetrics metrics) {
        this.member = member;
        assert !(member instanceof Node) : "whoops : " + member;
        this.channel = channel;
        this.client = FirefliesGrpc.newFutureStub(channel.channel).withCompression("gzip");
        this.metrics = metrics;
    }

    @Override
    public void close() {
        channel.release();
    }

    @Override
    public Participant getMember() {
        return member;
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
                Gossip gossip;
                try {
                    gossip = result.get();
                } catch (InterruptedException | ExecutionException e) {
                    // ignored
                    return;
                }
                var serializedSize = gossip.getSerializedSize();
                metrics.inboundBandwidth().mark(serializedSize);
                metrics.gossipResponse().update(serializedSize);
            }
        }, r -> r.run());
        return result;
    }

    public void release() {
        close();
    }

    @Override
    public String toString() {
        return String.format("->[%s]", member);
    }

    @Override
    public void update(State state) {
        Context timer = null;
        if (metrics != null) {
            timer = metrics.outboundUpdateTimer().time();
        }
        try {
            client.update(state);
            if (metrics != null) {
                var serializedSize = state.getSerializedSize();
                metrics.outboundBandwidth().mark(serializedSize);
                metrics.outboundUpdate().update(serializedSize);
            }
        } finally {
            if (timer != null) {
                timer.stop();
            }
        }
    }
}
