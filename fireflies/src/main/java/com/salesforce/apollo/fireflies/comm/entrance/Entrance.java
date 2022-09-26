/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.comm.entrance;

import java.io.IOException;
import java.time.Duration;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.fireflies.proto.Gateway;
import com.salesfoce.apollo.fireflies.proto.Join;
import com.salesfoce.apollo.fireflies.proto.Redirect;
import com.salesfoce.apollo.fireflies.proto.Registration;
import com.salesforce.apollo.comm.Link;
import com.salesforce.apollo.fireflies.View.Node;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public interface Entrance extends Link {

    static Entrance getLocalLoopback(Node node) {
        return new Entrance() {

            @Override
            public void close() throws IOException {
            }

            @Override
            public Member getMember() {
                return node;
            }

            @Override
            public ListenableFuture<Gateway> join(Join join, Duration timeout) {
                return null;
            }

            @Override
            public ListenableFuture<Redirect> seed(Registration registration) {
                return null;
            }
        };
    }

    ListenableFuture<Gateway> join(Join join, Duration timeout);

    ListenableFuture<Redirect> seed(Registration registration);
}
