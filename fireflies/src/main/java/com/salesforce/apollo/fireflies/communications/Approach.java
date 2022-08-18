/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.communications;

import java.io.IOException;
import java.time.Duration;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.fireflies.proto.Credentials;
import com.salesfoce.apollo.fireflies.proto.Gateway;
import com.salesfoce.apollo.fireflies.proto.Redirect;
import com.salesforce.apollo.comm.Link;
import com.salesforce.apollo.fireflies.View.Node;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public interface Approach extends Link {

    static Approach getLocalLoopback(Node node) {
        return new Approach() {

            @Override
            public void close() throws IOException {
            }

            @Override
            public Member getMember() {
                return node;
            }

            @Override
            public ListenableFuture<Gateway> join(Credentials join, Duration timeout) {
                return null;
            }

            @Override
            public ListenableFuture<Redirect> seed(Credentials join) {
                return null;
            }
        };
    }

    ListenableFuture<Gateway> join(Credentials join, Duration timeout);

    ListenableFuture<Redirect> seed(Credentials join);
}
