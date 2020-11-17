/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging.comms;

import com.salesfoce.apollo.proto.MessageBff;
import com.salesfoce.apollo.proto.Messages;
import com.salesfoce.apollo.proto.MessagingGrpc;
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

    private final ManagedServerConnection             channel;
    private final MessagingGrpc.MessagingBlockingStub client;
    private final Member                              member;
    private final MessagingMetrics                    metrics;

    public MessagingClientCommunications(ManagedServerConnection channel, Member member, MessagingMetrics metrics) {
        this.member = member;
        this.channel = channel;
        this.client = MessagingGrpc.newBlockingStub(channel.channel).withCompression("gzip");
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
    public Messages gossip(MessageBff request) {
        Messages result = client.gossip(request);
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundBandwidth().mark(result.getSerializedSize());
            metrics.outboundGossip().update(request.getSerializedSize());
            metrics.gossipResponse().update(result.getSerializedSize());
            metrics.outboundGossipRate().mark();
        }
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
