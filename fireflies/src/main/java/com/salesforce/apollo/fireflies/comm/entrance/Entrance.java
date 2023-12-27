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
import com.salesforce.apollo.stereotomy.event.proto.EventCoords;
import com.salesforce.apollo.stereotomy.event.proto.IdentAndSeq;
import com.salesforce.apollo.stereotomy.event.proto.KeyState_;

import java.io.IOException;
import java.time.Duration;

/**
 * @author hal.hildebrand
 */
public interface Entrance extends Link {

    static Entrance getLocalLoopback(Node node) {
        return new Entrance() {

            @Override
            public void close() throws IOException {
            }

            @Override
            public ListenableFuture<KeyState_> getKeyState(IdentAndSeq idSeq) {
                return null;
            }

            @Override
            public ListenableFuture<KeyState_> getKeyState(EventCoords coords) {
                return null;
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

    ListenableFuture<KeyState_> getKeyState(IdentAndSeq idSeq);

    ListenableFuture<KeyState_> getKeyState(EventCoords coords);

    ListenableFuture<Gateway> join(Join join, Duration timeout);

    ListenableFuture<Redirect> seed(Registration registration);
}
