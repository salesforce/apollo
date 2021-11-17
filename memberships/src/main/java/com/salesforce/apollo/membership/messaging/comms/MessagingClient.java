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
import com.salesfoce.apollo.messaging.proto.MessageBff;
import com.salesfoce.apollo.messaging.proto.Messages;
import com.salesfoce.apollo.messaging.proto.MessagingGrpc;
import com.salesfoce.apollo.messaging.proto.MessagingGrpc.MessagingFutureStub;
import com.salesfoce.apollo.messaging.proto.Push;
import com.salesforce.apollo.comm.Link;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.Messaging;
import com.salesforce.apollo.membership.messaging.MessagingMetrics;

/**
 * @author hal.hildebrand
 * @deprecated Will soon be eliminated
 * @since 220
 */
public class MessagingClient implements Messaging, Link {

    public static CreateClientCommunications<Messaging> getCreate(MessagingMetrics metrics, Executor exeucutor) {
        return (t, f, c) -> {
            return new MessagingClient(c, t, metrics, exeucutor);
        };

    }

    private final ManagedServerConnection channel;
    private final MessagingFutureStub     client;
    private final Executor                executor;
    private final Member                  member;
    private final MessagingMetrics        metrics;

    public MessagingClient(ManagedServerConnection channel, Member member, MessagingMetrics metrics,
            Executor executor) {
        this.member = member;
        this.channel = channel;
        this.client = MessagingGrpc.newFutureStub(channel.channel).withCompression("gzip");
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
    public void update(Push push) {
        client.update(push);
        if (metrics != null) {
            metrics.outboundBandwidth().mark(push.getSerializedSize());
            metrics.outboundUpdate().update(push.getSerializedSize());
            metrics.outboundUpdateRate().mark();
        }
    }
}
