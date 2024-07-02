/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.comm.entrance;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesforce.apollo.archipelago.Link;
import com.salesforce.apollo.fireflies.View.Node;
import com.salesforce.apollo.fireflies.proto.Gateway;
import com.salesforce.apollo.fireflies.proto.Join;
import com.salesforce.apollo.fireflies.proto.Redirect;
import com.salesforce.apollo.fireflies.proto.Registration;
import com.salesforce.apollo.membership.Member;

import java.io.IOException;
import java.time.Duration;

/**
 * @author hal.hildebrand
 */
public interface Entrance extends Link {

    static Entrance getLocalLoopback(Node node, EntranceService service) {
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
            public Redirect seed(Registration registration) {
                return null;
            }
        };
    }

    ListenableFuture<Gateway> join(Join join, Duration timeout);

    Redirect seed(Registration registration);
}
