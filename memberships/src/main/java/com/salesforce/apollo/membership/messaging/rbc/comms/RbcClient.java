/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging.rbc.comms;

import com.codahale.metrics.Timer.Context;
import com.salesforce.apollo.archipelago.ManagedServerChannel;
import com.salesforce.apollo.archipelago.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.rbc.RbcMetrics;
import com.salesforce.apollo.messaging.proto.MessageBff;
import com.salesforce.apollo.messaging.proto.RBCGrpc;
import com.salesforce.apollo.messaging.proto.Reconcile;
import com.salesforce.apollo.messaging.proto.ReconcileContext;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class RbcClient implements ReliableBroadcast {

    private final ManagedServerChannel    channel;
    private final RBCGrpc.RBCBlockingStub client;
    private final RbcMetrics              metrics;

    public RbcClient(ManagedServerChannel c, RbcMetrics metrics) {
        this.channel = c;
        this.client = c.wrap(RBCGrpc.newBlockingStub(c));
        this.metrics = metrics;
    }

    public static CreateClientCommunications<ReliableBroadcast> getCreate(RbcMetrics metrics) {
        return (c) -> {
            return new RbcClient(c, metrics);
        };

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
    public Reconcile gossip(MessageBff request) {
        Context timer = metrics == null ? null : metrics.outboundGossipTimer().time();
        if (metrics != null) {
            var serializedSize = request.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundGossip().update(serializedSize);
        }
        var result = client.gossip(request);
        if (metrics != null) {
            timer.stop();
            var serializedSize = result.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.gossipResponse().update(serializedSize);
        }
        return result;
    }

    public void start() {

    }

    @Override
    public String toString() {
        return String.format("->[%s]", channel.getMember());
    }

    @Override
    public void update(ReconcileContext request) {
        Context timer = metrics == null ? null : metrics.outboundUpdateTimer().time();
        if (metrics != null) {
            var serializedSize = request.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundUpdate().update(serializedSize);
        }
        try {
            var result = client.update(request);
            if (metrics != null) {
                if (timer != null) {
                    timer.stop();
                }
            }
        } catch (Throwable e) {
            if (timer != null) {
                timer.close();
            }
        }
    }
}
