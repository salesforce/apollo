/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.comms;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.consortium.proto.CheckpointReplication;
import com.salesfoce.apollo.consortium.proto.CheckpointSegments;
import com.salesfoce.apollo.consortium.proto.CheckpointSync;
import com.salesfoce.apollo.consortium.proto.Join;
import com.salesfoce.apollo.consortium.proto.JoinResult;
import com.salesfoce.apollo.consortium.proto.OrderingServiceGrpc;
import com.salesfoce.apollo.consortium.proto.OrderingServiceGrpc.OrderingServiceFutureStub;
import com.salesfoce.apollo.consortium.proto.StopData;
import com.salesfoce.apollo.consortium.proto.SubmitTransaction;
import com.salesfoce.apollo.consortium.proto.TransactionResult;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public class ConsortiumClientCommunications implements ConsortiumService {

    public static CreateClientCommunications<ConsortiumClientCommunications> getCreate(ConsortiumMetrics metrics) {
        return (t, f, c) -> new ConsortiumClientCommunications(c, t, metrics);

    }

    private final ManagedServerConnection   channel;
    private final OrderingServiceFutureStub client;
    private final Member                    member;
    @SuppressWarnings("unused")
    private final ConsortiumMetrics         metrics;

    public ConsortiumClientCommunications(ManagedServerConnection channel, Member member, ConsortiumMetrics metrics) {
        this.member = member;
        this.channel = channel;
        this.client = OrderingServiceGrpc.newFutureStub(channel.channel).withCompression("gzip");
        this.metrics = metrics;
    }

    @Override
    public ListenableFuture<CertifiedBlock> checkpointSync(CheckpointSync sync) {
        return client.checkpointSync(sync);
    }

    @Override
    public ListenableFuture<TransactionResult> clientSubmit(SubmitTransaction txn) {
        return client.submit(txn);
    }

    @Override
    public ListenableFuture<CheckpointSegments> fetch(CheckpointReplication request) {
        return client.fetch(request);
    }

    public Member getMember() {
        return member;
    }

    @Override
    public ListenableFuture<JoinResult> join(Join join) {
        return client.join(join);
    }

    public void release() {
        channel.release();
    }

    @Override
    public void stopData(StopData stopData) {
        client.stopData(stopData);
    }
}
