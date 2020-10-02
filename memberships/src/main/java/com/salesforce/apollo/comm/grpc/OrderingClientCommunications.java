/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm.grpc;

import com.salesfoce.apollo.proto.OrdererGrpc;
import com.salesfoce.apollo.proto.OrdererGrpc.OrdererBlockingStub;
import com.salesfoce.apollo.proto.Submission;
import com.salesfoce.apollo.proto.SubmitResolution;
import com.salesforce.apollo.comm.CommonCommunications;
import com.salesforce.apollo.comm.Communications;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.Ordering;

/**
 * @author hal.hildebrand
 *
 */
public class OrderingClientCommunications implements Ordering {

    public static CommonCommunications<OrderingClientCommunications> commsFor(Member member, Ordering service,
                                                                              Communications communications) {
        return communications.create(member, getCreate(), new OrderingServerCommunications(service,
                communications.getClientIdentityProvider()));
    }

    public static CreateClientCommunications<OrderingClientCommunications> getCreate() {
        return (t, f, c) -> new OrderingClientCommunications(c, t);

    }

    private final ManagedServerConnection channel;
    private final OrdererBlockingStub     client;
    private final Member                  member;

    public OrderingClientCommunications(ManagedServerConnection channel, Member member) {
        this.member = member;
        this.channel = channel;
        this.client = OrdererGrpc.newBlockingStub(channel.channel);
    }

    public Member getMember() {
        return member;
    }

    public void release() {
        channel.release();
    }

    @Override
    public String toString() {
        return String.format("->[%s]", member);
    }

    @Override
    public SubmitResolution submit(Submission submission) {
        try {
            return client.submit(submission);
        } catch (Throwable e) {
            throw new IllegalStateException("Unexpected exception in communication", e);
        }
    }

}
