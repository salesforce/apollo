/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.comm;

import com.salesfoce.apollo.choam.proto.*;
import com.salesforce.apollo.archipelago.Link;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;

/**
 * Terminal RPC endpoint for CHOAM
 *
 * @author hal.hildebrand
 */
public interface Terminal extends Link {

    static Terminal getLocalLoopback(SigningMember member, Concierge service) {
        return new Terminal() {

            @Override
            public void close() {
            }

            @Override
            public CheckpointSegments fetch(CheckpointReplication request) {
                return null;
            }

            @Override
            public Blocks fetchBlocks(BlockReplication replication) {
                return null;
            }

            @Override
            public Blocks fetchViewChain(BlockReplication replication) {
                return null;
            }

            @Override
            public Member getMember() {
                return member;
            }

            @Override
            public ViewMember join(Digest nextView) {
                return service.join(nextView, member.getId());
            }

            @Override
            public Initial sync(Synchronize sync) {
                return null;
            }
        };
    }

    CheckpointSegments fetch(CheckpointReplication request);

    Blocks fetchBlocks(BlockReplication replication);

    Blocks fetchViewChain(BlockReplication replication);

    ViewMember join(Digest nextView);

    Initial sync(Synchronize sync);
}
