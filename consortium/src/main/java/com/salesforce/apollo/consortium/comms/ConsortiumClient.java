/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.comms;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.consortium.proto.BlockReplication;
import com.salesfoce.apollo.consortium.proto.Blocks;
import com.salesfoce.apollo.consortium.proto.CheckpointReplication;
import com.salesfoce.apollo.consortium.proto.CheckpointSegments;
import com.salesfoce.apollo.consortium.proto.Initial;
import com.salesfoce.apollo.consortium.proto.Join;
import com.salesfoce.apollo.consortium.proto.JoinResult;
import com.salesfoce.apollo.consortium.proto.LinearServiceGrpc;
import com.salesfoce.apollo.consortium.proto.LinearServiceGrpc.LinearServiceFutureStub;
import com.salesfoce.apollo.consortium.proto.StopData;
import com.salesfoce.apollo.consortium.proto.SubmitTransaction;
import com.salesfoce.apollo.consortium.proto.Synchronize;
import com.salesfoce.apollo.consortium.proto.TransactionResult;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public class ConsortiumClient implements ConsortiumService {

    public static CreateClientCommunications<ConsortiumClient> getCreate(ConsortiumMetrics metrics) {
        return (t, f, c) -> new ConsortiumClient(c, t, metrics);

    }

    private final ManagedServerConnection channel;
    private final LinearServiceFutureStub client;
    private final Member                  member;
    @SuppressWarnings("unused")
    private final ConsortiumMetrics       metrics;

    public ConsortiumClient(ManagedServerConnection channel, Member member, ConsortiumMetrics metrics) {
        this.member = member;
        this.channel = channel;
        this.client = LinearServiceGrpc.newFutureStub(channel.channel).withCompression("gzip");
        this.metrics = metrics;
    }

    @Override
    public ListenableFuture<TransactionResult> clientSubmit(SubmitTransaction txn) {
        return client.submit(txn);
    }

    @Override
    public ListenableFuture<CheckpointSegments> fetch(CheckpointReplication request) {
        return client.fetch(request);
    }

    @Override
    public ListenableFuture<Blocks> fetchBlocks(BlockReplication replication) {
        return client.fetchBlocks(replication);
    }

    @Override
    public ListenableFuture<Blocks> fetchViewChain(BlockReplication replication) {
        return client.fetchViewChain(replication);
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

    @Override
    public ListenableFuture<Initial> sync(Synchronize sync) {
        return client.sync(sync);
    }
}
