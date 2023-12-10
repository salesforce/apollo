/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.comm.gossip;

import com.salesforce.apollo.fireflies.proto.Gossip;
import com.salesforce.apollo.fireflies.proto.SayWhat;
import com.salesforce.apollo.fireflies.proto.State;
import com.salesforce.apollo.archipelago.Link;
import com.salesforce.apollo.fireflies.View.Node;
import com.salesforce.apollo.membership.Member;

import java.io.IOException;

/**
 * @author hal.hildebrand
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
            public Gossip gossip(SayWhat sw) {
                return null;
            }

            @Override
            public void update(State state) {
            }
        };
    }

    Gossip gossip(SayWhat sw);

    void update(State state);

}
