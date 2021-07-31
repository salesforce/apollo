/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import java.io.IOException;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.choam.proto.BlockReplication;
import com.salesfoce.apollo.choam.proto.Blocks;
import com.salesfoce.apollo.choam.proto.CheckpointReplication;
import com.salesfoce.apollo.choam.proto.CheckpointSegments;
import com.salesfoce.apollo.choam.proto.Initial;
import com.salesfoce.apollo.choam.proto.Synchronize;
import com.salesforce.apollo.comm.Link;
import com.salesforce.apollo.membership.Member;

/**
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
            public ListenableFuture<Initial> sync(Synchronize sync) {
                return null;
            }
        };
    }

    ListenableFuture<CheckpointSegments> fetch(CheckpointReplication request);

    ListenableFuture<Blocks> fetchBlocks(BlockReplication replication);

    ListenableFuture<Blocks> fetchViewChain(BlockReplication replication);

    ListenableFuture<Initial> sync(Synchronize sync);
}
