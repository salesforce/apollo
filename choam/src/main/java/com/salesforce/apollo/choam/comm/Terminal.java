/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.comm;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.salesfoce.apollo.choam.proto.BlockReplication;
import com.salesfoce.apollo.choam.proto.Blocks;
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
import com.salesforce.apollo.membership.SigningMember;

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
            public Member getMember() {
                return member;
            }
        };
    }

    static Terminal getLocalLoopback(SigningMember member, Concierge service) {
        return new Terminal() {

            @Override
            public Member getMember() {
                return member;
            }

            @Override
            public ListenableFuture<ViewMember> join(JoinRequest join) {
                SettableFuture<ViewMember> f = SettableFuture.create();
                f.set(service.join(join, member.getId()));
                return f;
            }

            @Override
            public ListenableFuture<SubmitResult> submit(SubmitTransaction request) {
                SettableFuture<SubmitResult> f = SettableFuture.create();
                f.set(service.submit(request, member.getId()));
                return f;
            }
        };
    }

    default void close() {
    }

    default ListenableFuture<CheckpointSegments> fetch(CheckpointReplication request) {
        return null;
    }

    default ListenableFuture<Blocks> fetchBlocks(BlockReplication replication) {
        return null;
    }

    default ListenableFuture<Blocks> fetchViewChain(BlockReplication replication) {
        return null;
    }

    default ListenableFuture<ViewMember> join(JoinRequest join) {
        return null;
    }

    default ListenableFuture<SubmitResult> submit(SubmitTransaction request) {
        return null;
    }

    default ListenableFuture<Initial> sync(Synchronize sync) {
        return null;
    }
}
