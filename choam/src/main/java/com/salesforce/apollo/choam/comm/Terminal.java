/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.comm;

import java.io.IOException;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.choam.proto.BlockReplication;
import com.salesfoce.apollo.choam.proto.Blocks;
import com.salesfoce.apollo.choam.proto.Certification;
import com.salesfoce.apollo.choam.proto.CheckpointReplication;
import com.salesfoce.apollo.choam.proto.CheckpointSegments;
import com.salesfoce.apollo.choam.proto.Initial;
import com.salesfoce.apollo.choam.proto.JoinRequest;
import com.salesfoce.apollo.choam.proto.SubmitResult;
import com.salesfoce.apollo.choam.proto.SubmitTransaction;
import com.salesfoce.apollo.choam.proto.Synchronize;
import com.salesfoce.apollo.choam.proto.ViewMember;
import com.salesforce.apollo.comm.Link;
import com.salesforce.apollo.membership.Member;

/**
 * Terminal RPC endpoint for CHOAM
 * 
 * @author hal.hildebrand
 *
 */
public interface Terminal extends Link {

    static Terminal getLocalLoopback(Member member) {
        return new Terminal() {

            @Override
            public void close() throws IOException {
            }

            @Override
            public ListenableFuture<CheckpointSegments> fetch(CheckpointReplication request) {
                return null;
            }

            @Override
            public ListenableFuture<Blocks> fetchBlocks(BlockReplication replication) {
                return null;
            }

            @Override
            public ListenableFuture<Blocks> fetchViewChain(BlockReplication replication) {
                return null;
            }

            @Override
            public Member getMember() {
                return member;
            }

            @Override
            public ListenableFuture<ViewMember> join(JoinRequest join) {
                return null;
            }

            @Override
            public ListenableFuture<SubmitResult> submit(SubmitTransaction request) {
                return null;
            }

            @Override
            public ListenableFuture<Initial> sync(Synchronize sync) {
                return null;
            }

            @Override
            public ListenableFuture<Certification> join2(JoinRequest join) {
                return null;
            }
        };
    }

    ListenableFuture<CheckpointSegments> fetch(CheckpointReplication request);

    ListenableFuture<Blocks> fetchBlocks(BlockReplication replication);

    ListenableFuture<Blocks> fetchViewChain(BlockReplication replication);

    ListenableFuture<ViewMember> join(JoinRequest join);

    ListenableFuture<Certification> join2(JoinRequest join);

    ListenableFuture<SubmitResult> submit(SubmitTransaction request);

    ListenableFuture<Initial> sync(Synchronize sync);
}
