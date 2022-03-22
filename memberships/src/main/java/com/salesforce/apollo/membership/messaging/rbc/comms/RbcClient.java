/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging.rbc.comms;

import java.util.concurrent.ExecutionException;

import com.codahale.metrics.Timer.Context;
import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.messaging.proto.MessageBff;
import com.salesfoce.apollo.messaging.proto.RBCGrpc;
import com.salesfoce.apollo.messaging.proto.RBCGrpc.RBCFutureStub;
import com.salesfoce.apollo.messaging.proto.Reconcile;
import com.salesfoce.apollo.messaging.proto.ReconcileContext;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.rbc.RbcMetrics;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class RbcClient implements ReliableBroadcast {

    public static CreateClientCommunications<ReliableBroadcast> getCreate(RbcMetrics metrics) {
        return (t, f, c) -> {
            return new RbcClient(c, t, metrics);
        };

    }

    private final ManagedServerConnection channel;
    private final RBCFutureStub           client;
    private final Member                  member;
    private final RbcMetrics              metrics;

    public RbcClient(ManagedServerConnection channel, Member member, RbcMetrics metrics) {
        this.member = member;
        this.channel = channel;
        this.client = RBCGrpc.newFutureStub(channel.channel).withCompression("gzip");
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
    public ListenableFuture<Reconcile> gossip(MessageBff request) {
        Context timer = metrics == null ? null : metrics.outboundGossipTimer().time();
        if (metrics != null) {
            var serializedSize = request.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundGossip().mark(serializedSize);
        }
        var result = client.gossip(request);
        if (metrics != null) {
            result.addListener(() -> {
                Reconcile reconcile;
                try {
                    reconcile = result.get();
                    timer.stop();
                    var serializedSize = reconcile.getSerializedSize();
                    metrics.inboundBandwidth().mark(serializedSize);
                    metrics.gossipResponse().mark(serializedSize);
                } catch (InterruptedException | ExecutionException e) {
                    if (timer != null) {
                        timer.close();
                    }
                }
            }, r -> r.run());
        }
        return result;
    }

    @Override
    public void update(ReconcileContext request) {
        Context timer = metrics == null ? null : metrics.outboundUpdateTimer().time();
        if (metrics != null) {
            var serializedSize = request.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundUpdate().mark(serializedSize);
        }
        try {
            var result = client.update(request);
            if (metrics != null) {
                result.addListener(() -> {
                    if (timer != null) {
                        timer.stop();
                    }
                }, r -> r.run());
            }
        } catch (Throwable e) {
            if (timer != null) {
                timer.close();
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
