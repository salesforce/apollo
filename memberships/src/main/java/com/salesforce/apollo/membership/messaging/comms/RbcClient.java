/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging.comms;

import com.codahale.metrics.Timer.Context;
import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.messaging.proto.MessageBff;
import com.salesfoce.apollo.messaging.proto.RBCGrpc;
import com.salesfoce.apollo.messaging.proto.RBCGrpc.RBCFutureStub;
import com.salesfoce.apollo.messaging.proto.Reconcile;
import com.salesforce.apollo.comm.RouterMetrics;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcast;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class RbcClient implements ReliableBroadcast {

    public static CreateClientCommunications<ReliableBroadcast> getCreate(RouterMetrics metrics) {
        return (t, f, c) -> {
            return new RbcClient(c, t, metrics);
        };

    }

    private final ManagedServerConnection channel;
    private final RBCFutureStub           client;
    private final Member                  member;
    private final RouterMetrics           metrics;

    public RbcClient(ManagedServerConnection channel, Member member, RouterMetrics metrics) {
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
        Context timer = null;
        if (metrics != null) {
            timer = metrics.outboundGossipTimer().time();
        }
        try {
            return client.gossip(request);
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
