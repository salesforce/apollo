/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.comms;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.consortium.proto.BlockReplication;
import com.salesfoce.apollo.consortium.proto.Blocks;
import com.salesfoce.apollo.consortium.proto.BoostrapGrpc;
import com.salesfoce.apollo.consortium.proto.BoostrapGrpc.BoostrapFutureStub;
import com.salesfoce.apollo.consortium.proto.CheckpointReplication;
import com.salesfoce.apollo.consortium.proto.CheckpointSegments;
import com.salesfoce.apollo.consortium.proto.Initial;
import com.salesfoce.apollo.consortium.proto.Synchronize;
import com.salesforce.apollo.comm.Link;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public class BootstrapClient implements BootstrapService, Link {

    public static CreateClientCommunications<BootstrapClient> getCreate(ConsortiumMetrics metrics) {
        return (t, f, c) -> new BootstrapClient(c, t, metrics);

    }

    private final ManagedServerConnection channel;
    private final BoostrapFutureStub      client;
    private final Member                  member;
    @SuppressWarnings("unused")
    private final ConsortiumMetrics       metrics;

    public BootstrapClient(ManagedServerConnection channel, Member member, ConsortiumMetrics metrics) {
        this.member = member;
        this.channel = channel;
        this.client = BoostrapGrpc.newFutureStub(channel.channel).withCompression("gzip");
        this.metrics = metrics;
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

    @Override
    public Member getMember() {
        return member;
    }

    public void release() {
        close();
    }

    @Override
    public void close() {
        channel.release();
    }

    @Override
    public ListenableFuture<Initial> sync(Synchronize sync) {
        return client.sync(sync);
    }
}
