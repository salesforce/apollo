/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.comm;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.choam.proto.BlockReplication;
import com.salesfoce.apollo.choam.proto.Blocks;
import com.salesfoce.apollo.choam.proto.CheckpointReplication;
import com.salesfoce.apollo.choam.proto.CheckpointSegments;
import com.salesfoce.apollo.choam.proto.Initial;
import com.salesfoce.apollo.choam.proto.JoinRequest;
import com.salesfoce.apollo.choam.proto.SubmitResult;
import com.salesfoce.apollo.choam.proto.SubmitTransaction;
import com.salesfoce.apollo.choam.proto.Synchronize;
import com.salesfoce.apollo.choam.proto.TerminalGrpc;
import com.salesfoce.apollo.choam.proto.TerminalGrpc.TerminalFutureStub;
import com.salesfoce.apollo.choam.proto.ViewMember;
import com.salesforce.apollo.choam.support.ChoamMetrics;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public class TerminalClient implements Terminal {

    public static CreateClientCommunications<Terminal> getCreate(ChoamMetrics metrics) {
        return (t, f, c) -> new TerminalClient(c, t, metrics);

    }

    private final ManagedServerConnection channel;

    private final TerminalFutureStub client;
    private final Member             member;
    @SuppressWarnings("unused")
    private final ChoamMetrics       metrics;

    public TerminalClient(ManagedServerConnection channel, Member member, ChoamMetrics metrics) {
        this.member = member;
        this.channel = channel;
        this.client = TerminalGrpc.newFutureStub(channel.channel).withCompression("gzip");
        this.metrics = metrics;
    }

    @Override
    public void close() {
        channel.release();
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

    @Override
    public ListenableFuture<ViewMember> join(JoinRequest join) {
        return client.join(join);
    }

    public void release() {
        close();
    }

    @Override
    public ListenableFuture<SubmitResult> submit(SubmitTransaction request) {
        return client.submit(request);
    }

    @Override
    public ListenableFuture<Initial> sync(Synchronize sync) {
        return client.sync(sync);
    }
}
