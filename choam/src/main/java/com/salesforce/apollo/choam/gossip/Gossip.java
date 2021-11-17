/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.gossip;

import java.io.IOException;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.choam.proto.Have;
import com.salesfoce.apollo.choam.proto.Update;
import com.salesforce.apollo.comm.Link;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public interface Gossip extends Link {

    static <S extends Member> Gossip getLocalLoopback(S member) {
        return new Gossip() {

            @Override
            public void close() throws IOException {
            }

            @Override
            public Member getMember() {
                return member;
            }

            @Override
            public ListenableFuture<Update> gossip(Have request) {
                return null;
            }
        };
    }

    ListenableFuture<Update> gossip(Have request);
}
