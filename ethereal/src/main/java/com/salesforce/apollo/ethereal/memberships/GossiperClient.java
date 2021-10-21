/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.memberships;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import com.codahale.metrics.Timer.Context;
import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.ethereal.proto.Gossip;
import com.salesfoce.apollo.ethereal.proto.GossiperGrpc;
import com.salesfoce.apollo.ethereal.proto.GossiperGrpc.GossiperFutureStub;
import com.salesfoce.apollo.ethereal.proto.Update;
import com.salesforce.apollo.comm.RouterMetrics;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class GossiperClient implements Scuttlebutte {

    public static CreateClientCommunications<Scuttlebutte> getCreate(RouterMetrics metrics, Executor exeucutor) {
        return (t, f, c) -> {
            return new GossiperClient(c, t, metrics, exeucutor);
        };

    }

    private final ManagedServerConnection channel;
    private final GossiperFutureStub      client;
    private final Executor                executor;
    private final Member                  member;
    private final RouterMetrics           metrics;

    public GossiperClient(ManagedServerConnection channel, Member member, RouterMetrics metrics, Executor executor) {
        this.member = member;
        this.channel = channel;
        this.client = GossiperGrpc.newFutureStub(channel.channel).withCompression("gzip");
        this.metrics = metrics;
        this.executor = executor;
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
    public ListenableFuture<Update> gossip(Gossip request) {
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
            }, executor);
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
