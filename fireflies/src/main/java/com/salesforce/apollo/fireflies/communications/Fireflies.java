/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.communications;

import java.io.IOException;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.fireflies.proto.Credentials;
import com.salesfoce.apollo.fireflies.proto.Gateway;
import com.salesfoce.apollo.fireflies.proto.Gossip;
import com.salesfoce.apollo.fireflies.proto.Redirect;
import com.salesfoce.apollo.fireflies.proto.SayWhat;
import com.salesfoce.apollo.fireflies.proto.State;
import com.salesforce.apollo.comm.Link;
import com.salesforce.apollo.fireflies.View.Node;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public interface Fireflies extends Link {

    static Fireflies getLocalLoopback(Node node) {
        return new Fireflies() {

            @Override
            public void close() throws IOException {
            }

            @Override
            public Member getMember() {
                return node;
            }

            @Override
            public ListenableFuture<Gossip> gossip(SayWhat sw) {
                return null;
            }

            @Override
            public ListenableFuture<Gateway> join(Credentials join) {
                return null;
            }

            @Override
            public ListenableFuture<Redirect> seed(Credentials join) {
                return null;
            }

            @Override
            public void update(State state) {
            }
        };
    }

    ListenableFuture<Gossip> gossip(SayWhat sw);

    ListenableFuture<Gateway> join(Credentials join);

    ListenableFuture<Redirect> seed(Credentials join);

    void update(State state);

}
