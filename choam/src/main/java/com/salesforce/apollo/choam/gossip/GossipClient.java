/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.gossip;

import java.util.concurrent.ExecutionException;

import com.codahale.metrics.Timer.Context;
import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.choam.proto.GossipGrpc;
import com.salesfoce.apollo.choam.proto.GossipGrpc.GossipFutureStub;
import com.salesfoce.apollo.choam.proto.Have;
import com.salesfoce.apollo.choam.proto.Update;
import com.salesforce.apollo.comm.RouterMetrics;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class GossipClient implements Gossip {

    public static CreateClientCommunications<Gossip> getCreate(RouterMetrics metrics) {
        return (t, f, c) -> {
            return new GossipClient(c, t, metrics);
        };

    }

    private final ManagedServerConnection channel;
    private final GossipFutureStub        client;
    private final Member                  member;
    private final RouterMetrics           metrics;

    public GossipClient(ManagedServerConnection channel, Member member, RouterMetrics metrics) {
        this.member = member;
        this.channel = channel;
        this.client = GossipGrpc.newFutureStub(channel.channel).withCompression("gzip");
        this.metrics = metrics;
    }

    @Override
    public void close() {
        channel.release();
    }

    @Override
    public Member getMember() {
        return member;
    }

    @Override
    public ListenableFuture<Update> gossip(Have request) {
        Context timer = null;
        if (metrics != null) {
            timer = metrics.outboundGossipTimer().time();
        }
        try {
            ListenableFuture<Update> result = client.gossip(request);
            result.addListener(() -> {
                if (metrics != null) {
                    Update messages;
                    try {
                        messages = result.get();
                        metrics.inboundBandwidth().mark(messages.getSerializedSize());
                        metrics.gossipResponse().update(messages.getSerializedSize());
                    } catch (InterruptedException | ExecutionException e) {
                        // purposefully ignored
                    }
                    metrics.outboundGossip().update(request.getSerializedSize());
                    metrics.outboundBandwidth().mark(request.getSerializedSize());
                    metrics.outboundGossipRate().mark();
                }
            }, r -> r.run());
            return result;
        } finally {
            if (timer != null) {
                timer.stop();
            }
        }
    }

    public void start() {

    }

    @Override
    public String toString() {
        return String.format("->[%s]", member);
    }
}
