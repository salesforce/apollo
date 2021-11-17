/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging.comms;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.messaging.proto.CausalMessages;
import com.salesfoce.apollo.messaging.proto.CausalMessagingGrpc;
import com.salesfoce.apollo.messaging.proto.CausalMessagingGrpc.CausalMessagingFutureStub;
import com.salesfoce.apollo.messaging.proto.CausalPush;
import com.salesfoce.apollo.messaging.proto.MessageBff;
import com.salesforce.apollo.comm.Link;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.MessagingMetrics;
import com.salesforce.apollo.membership.messaging.causal.CausalMessaging;

/**
 * @author hal.hildebrand
 * @deprecated Will soon be eliminated
 * @since 220
 */
public class CausalMessagingClient implements CausalMessaging, Link {

    public static CreateClientCommunications<CausalMessaging> getCreate(MessagingMetrics metrics, Executor exeucutor) {
        return (t, f, c) -> {
            return new CausalMessagingClient(c, t, metrics, exeucutor);
        };

    }

    private final ManagedServerConnection   channel;
    private final CausalMessagingFutureStub client;
    private final Executor                  executor;
    private final Member                    member;
    private final MessagingMetrics          metrics;

    public CausalMessagingClient(ManagedServerConnection channel, Member member, MessagingMetrics metrics,
            Executor executor) {
        this.member = member;
        this.channel = channel;
        this.client = CausalMessagingGrpc.newFutureStub(channel.channel).withCompression("gzip");
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
    public ListenableFuture<CausalMessages> gossip(MessageBff request) {
        ListenableFuture<CausalMessages> result = client.gossip(request);
        result.addListener(() -> {
            if (metrics != null) {
                CausalMessages messages;
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
    }

    public void start() {

    }

    @Override
    public String toString() {
        return String.format("->[%s]", member);
    }

    @Override
    public void update(CausalPush push) {
        client.update(push);
        if (metrics != null) {
            metrics.outboundBandwidth().mark(push.getSerializedSize());
            metrics.outboundUpdate().update(push.getSerializedSize());
            metrics.outboundUpdateRate().mark();
        }
    }
}
