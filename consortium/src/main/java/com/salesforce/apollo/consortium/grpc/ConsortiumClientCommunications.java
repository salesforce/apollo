/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.grpc;

import com.salesfoce.apollo.consortium.proto.ConsortiumGrpc;
import com.salesfoce.apollo.consortium.proto.ConsortiumGrpc.ConsortiumBlockingStub;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.consortium.ConsortiumMetrics;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public class ConsortiumClientCommunications {

    public static CreateClientCommunications<ConsortiumClientCommunications> getCreate(ConsortiumMetrics metrics) {
        return (t, f, c) -> new ConsortiumClientCommunications(c, t, metrics);

    }

    private final ManagedServerConnection channel;
    @SuppressWarnings("unused")
    private final ConsortiumBlockingStub  client;
    private final Member                  member;
    @SuppressWarnings("unused")
    private final ConsortiumMetrics       metrics;

    public ConsortiumClientCommunications(ManagedServerConnection channel, Member member, ConsortiumMetrics metrics) {
        this.member = member;
        this.channel = channel;
        this.client = ConsortiumGrpc.newBlockingStub(channel.channel).withCompression("gzip");
        this.metrics = metrics;
    }

    public Member getMember() {
        return member;
    }

    public void release() {
        channel.release();
    }
}
