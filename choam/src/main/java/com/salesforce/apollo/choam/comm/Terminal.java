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
import com.salesfoce.apollo.choam.proto.Synchronize;
import com.salesfoce.apollo.choam.proto.ViewMember;
import com.salesforce.apollo.archipelago.Link;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;

/**
 * Terminal RPC endpoint for CHOAM
 * 
 * @author hal.hildebrand
 *
 */
public interface Terminal extends Link {

    static Terminal getLocalLoopback(SigningMember member, Concierge service) {
        return new Terminal() {

            @Override
            public void close() {
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
            public ListenableFuture<ViewMember> join(Digest nextView) {
                SettableFuture<ViewMember> f = SettableFuture.create();
                f.set(service.join(nextView, member.getId()));
                return f;
            }

            @Override
            public ListenableFuture<Initial> sync(Synchronize sync) {
                return null;
            }
        };
    }

    ListenableFuture<CheckpointSegments> fetch(CheckpointReplication request);

    ListenableFuture<Blocks> fetchBlocks(BlockReplication replication);

    ListenableFuture<Blocks> fetchViewChain(BlockReplication replication);

    ListenableFuture<ViewMember> join(Digest nextView);

    ListenableFuture<Initial> sync(Synchronize sync);
}
