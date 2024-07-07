/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.comm.gossip;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Empty;
import com.salesforce.apollo.archipelago.Link;
import com.salesforce.apollo.fireflies.View.Node;
import com.salesforce.apollo.fireflies.proto.*;
import com.salesforce.apollo.membership.Member;

import java.io.IOException;
import java.time.Duration;

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
            public Void enjoin(Join join) {
                return null;
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
            public ListenableFuture<Empty> ping(Ping ping, Duration timeout) {
                var fs = SettableFuture.<Empty>create();
                fs.set(Empty.getDefaultInstance());
                return fs;
            }

            @Override
            public void update(State state) {
            }
        };
    }

    Void enjoin(Join join);

    Gossip gossip(SayWhat sw);

    ListenableFuture<Empty> ping(Ping ping, Duration timeout);

    void update(State state);
}
