/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.comms;

import com.salesfoce.apollo.consortium.proto.Join;
import com.salesfoce.apollo.consortium.proto.JoinResult;
import com.salesfoce.apollo.consortium.proto.OrderingServiceGrpc;
import com.salesfoce.apollo.consortium.proto.OrderingServiceGrpc.OrderingServiceBlockingStub;
import com.salesfoce.apollo.consortium.proto.StopData;
import com.salesfoce.apollo.consortium.proto.SubmitTransaction;
import com.salesfoce.apollo.consortium.proto.TransactionResult;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.consortium.ConsortiumMetrics;
import com.salesforce.apollo.consortium.OrderingService;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public class ConsortiumClientCommunications implements OrderingService {

    public static CreateClientCommunications<ConsortiumClientCommunications> getCreate(ConsortiumMetrics metrics) {
        return (t, f, c) -> new ConsortiumClientCommunications(c, t, metrics);

    }

    private final ManagedServerConnection     channel;
    private final OrderingServiceBlockingStub client;
    private final Member                      member;
    @SuppressWarnings("unused")
    private final ConsortiumMetrics           metrics;

    public ConsortiumClientCommunications(ManagedServerConnection channel, Member member, ConsortiumMetrics metrics) {
        this.member = member;
        this.channel = channel;
        this.client = OrderingServiceGrpc.newBlockingStub(channel.channel).withCompression("gzip");
        this.metrics = metrics;
    }

    @Override
    public TransactionResult clientSubmit(SubmitTransaction request) {
        return client.submit(request);
    }

    public Member getMember() {
        return member;
    }

    @Override
    public JoinResult join(Join join) {
        return client.join(join);
    }

    public void release() {
        channel.release();
    }

    @Override
    public void stop(StopData stopData) {
        client.stop(stopData);
    }
}
