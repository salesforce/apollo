/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging;

import java.io.IOException;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.messaging.proto.MessageBff;
import com.salesfoce.apollo.messaging.proto.Messages;
import com.salesfoce.apollo.messaging.proto.Push;
import com.salesforce.apollo.comm.Link;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;

/**
 * @author hal.hildebrand
 * @deprecated Will soon be eliminated
 *
 */
public interface Messaging extends Link {

    static Messaging getLocalLoopback(SigningMember member) {
        return new Messaging() {

            @Override
            public void close() throws IOException {
            }

            @Override
            public Member getMember() {
                return member;
            }

            @Override
            public ListenableFuture<Messages> gossip(MessageBff bff) {
                return null;
            }

            @Override
            public void update(Push push) {
            }
        };
    }

    ListenableFuture<Messages> gossip(MessageBff bff);

    void update(Push push);

}
