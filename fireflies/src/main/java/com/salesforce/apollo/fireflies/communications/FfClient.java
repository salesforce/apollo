/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.communications;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;

import com.codahale.metrics.Timer.Context;
import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.proto.Digests;
import com.salesfoce.apollo.proto.FirefliesGrpc;
import com.salesfoce.apollo.proto.FirefliesGrpc.FirefliesFutureStub;
import com.salesfoce.apollo.proto.Gossip;
import com.salesfoce.apollo.proto.Null;
import com.salesfoce.apollo.proto.SayWhat;
import com.salesfoce.apollo.proto.Signed;
import com.salesfoce.apollo.proto.State;
import com.salesfoce.apollo.proto.Update;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.fireflies.FireflyMetrics;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.fireflies.Participant;
import com.salesforce.apollo.protocols.Fireflies;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class FfClient implements Fireflies {

    public static CreateClientCommunications<FfClient> getCreate(FireflyMetrics metrics) {
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

    public Participant getMember() {
        return member;
    }

    @Override
    public ListenableFuture<Gossip> gossip(HashKey context, Signed note, int ring, Digests digests) {
        Context timer = null;
        if (metrics != null) {
            timer = metrics.outboundGossipTimer().time();
        }
        try {
            SayWhat sw = SayWhat.newBuilder()
                                .setContext(context.toID())
                                .setNote(note)
                                .setRing(ring)
                                .setGossip(digests)
                                .build();
            ListenableFuture<Gossip> result = client.gossip(sw);
            if (metrics != null) {
                metrics.outboundBandwidth().mark(sw.getSerializedSize());
                metrics.outboundGossip().update(sw.getSerializedSize());
                metrics.outboundGossipRate().mark();
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
                    metrics.inboundBandwidth().mark(gossip.getSerializedSize());
                    metrics.gossipResponse().update(gossip.getSerializedSize());
                }
            }, ForkJoinPool.commonPool());
            return result;
        } catch (Throwable e) {
            throw new IllegalStateException("Unexpected exception in communication", e);
        } finally {
            if (timer != null) {
                timer.stop();
            }
        }
    }

    @Override
    public int ping(HashKey context, int ping) {
        Context timer = null;
        if (metrics != null) {
            timer = metrics.outboundPingTimer().time();
        }
        try {
            client.ping(Null.newBuilder().setContext(context.toID()).build());
            if (metrics != null) {
                metrics.outboundPingRate().mark();
            }
        } catch (Throwable e) {
            throw new IllegalStateException("Unexpected exception in communication", e);
        } finally {
            if (timer != null) {
                timer.stop();
            }
        }
        return 0;
    }

    public void release() {
        channel.release();
    }

    public void start() {

    }

    @Override
    public String toString() {
        return String.format("->[%s]", member);
    }

    @Override
    public void update(HashKey context, int ring, Update update) {
        Context timer = null;
        if (metrics != null) {
            timer = metrics.outboundUpdateTimer().time();
        }
        try {
            State state = State.newBuilder().setContext(context.toID()).setRing(ring).setUpdate(update).build();
            client.update(state);
            if (metrics != null) {
                metrics.outboundBandwidth().mark(state.getSerializedSize());
                metrics.outboundUpdate().update(state.getSerializedSize());
                metrics.outboundUpdateRate().mark();
            }
        } catch (Throwable e) {
            throw new IllegalStateException("Unexpected exception in communication", e);
        } finally {
            if (timer != null) {
                timer.stop();
            }
        }
    }
}
