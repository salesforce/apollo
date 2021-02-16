/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging.comms;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.proto.MessageBff;
import com.salesfoce.apollo.proto.Messages;
import com.salesfoce.apollo.proto.MessagingGrpc;
import com.salesfoce.apollo.proto.MessagingGrpc.MessagingFutureStub;
import com.salesfoce.apollo.proto.Push;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.MessagingMetrics;
import com.salesforce.apollo.protocols.Messaging;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class MessagingClientCommunications implements Messaging {

    public static CreateClientCommunications<MessagingClientCommunications> getCreate(MessagingMetrics metrics) {
        return (t, f, c) -> new MessagingClientCommunications(c, t, metrics);

    }

    private final ManagedServerConnection channel;
    private final MessagingFutureStub     client;
    private final Member                  member;
    private final MessagingMetrics        metrics;

    public MessagingClientCommunications(ManagedServerConnection channel, Member member, MessagingMetrics metrics) {
        this.member = member;
        this.channel = channel;
        this.client = MessagingGrpc.newFutureStub(channel.channel).withCompression("gzip");
        this.metrics = metrics;
    }

    public Member getMember() {
        return member;
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
    public ListenableFuture<Messages> gossip(MessageBff request) {
        ListenableFuture<Messages> result = client.gossip(request);
        result.addListener(() -> {
            if (metrics != null) {
                Messages messages;
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
        }, ForkJoinPool.commonPool());
        return result;
    }

    @Override
    public void update(Push push) {
        client.update(push);
        if (metrics != null) {
            metrics.outboundBandwidth().mark(push.getSerializedSize());
            metrics.outboundUpdate().update(push.getSerializedSize());
            metrics.outboundUpdateRate().mark();
        }
    }
}
