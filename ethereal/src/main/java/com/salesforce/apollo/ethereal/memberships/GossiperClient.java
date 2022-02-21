/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.memberships;

import java.util.concurrent.ExecutionException;

import com.codahale.metrics.Timer.Context;
import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.ethereal.proto.ContextUpdate;
import com.salesfoce.apollo.ethereal.proto.Gossip;
import com.salesfoce.apollo.ethereal.proto.GossiperGrpc;
import com.salesfoce.apollo.ethereal.proto.GossiperGrpc.GossiperFutureStub;
import com.salesfoce.apollo.ethereal.proto.Update;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class GossiperClient implements Gossiper {

    public static CreateClientCommunications<Gossiper> getCreate(EtherealMetrics metrics) {
        return (t, f, c) -> {
            return new GossiperClient(c, t, metrics);
        };

    }

    private final ManagedServerConnection channel;
    private final GossiperFutureStub      client;
    private final Member                  member;
    private final EtherealMetrics         metrics;

    public GossiperClient(ManagedServerConnection channel, Member member, EtherealMetrics metrics) {
        this.member = member;
        this.channel = channel;
        this.client = GossiperGrpc.newFutureStub(channel.channel).withCompression("gzip");
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
    public ListenableFuture<Update> gossip(Gossip request) {
        Context timer = metrics == null ? null : metrics.outboundGossipTimer().time();
        if (metrics != null) {
            metrics.outboundGossip().mark(request.getSerializedSize());
            metrics.outboundBandwidth().mark(request.getSerializedSize());
        }
        ListenableFuture<Update> result = client.gossip(request);
        result.addListener(() -> {
            Update messages = null;
            try {
                messages = result.get();
            } catch (InterruptedException | ExecutionException e) {
                // purposefully ignored
            } finally {
                if (timer != null) {
                    if (messages != null) {
                        metrics.inboundBandwidth().mark(messages.getSerializedSize());
                        metrics.gossipResponse().mark(messages.getSerializedSize());
                    }
                    timer.stop();
                }
            }
        }, r -> r.run());
        return result;
    }

    public void start() {
    }

    @Override
    public String toString() {
        return String.format("->[%s]", member);
    }

    @Override
    public void update(ContextUpdate request) {
        Context timer = null;
        if (metrics != null) {
            timer = metrics.outboundUpdateTimer().time();
            metrics.outboundUpdate().mark(request.getSerializedSize());
            metrics.outboundBandwidth().mark(request.getSerializedSize());
        }
        try {
            client.update(request);
        } finally {
            if (timer != null) {
                timer.stop();
            }
        }
    }
}
