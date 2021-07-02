/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging.causal;

import java.io.IOException;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.messaging.proto.CausalMessages;
import com.salesfoce.apollo.messaging.proto.CausalPush;
import com.salesfoce.apollo.messaging.proto.MessageBff;
import com.salesforce.apollo.comm.Link;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;

/**
 * @author hal.hildebrand
 *
 */
public interface CausalMessaging extends Link {

    static CausalMessaging getLocalLoopback(SigningMember member) {
        return new CausalMessaging() {

            @Override
            public void close() throws IOException {
            }

            @Override
            public Member getMember() {
                return member;
            }

            @Override
            public ListenableFuture<CausalMessages> gossip(MessageBff bff) {
                return null;
            }

            @Override
            public void update(CausalPush push) {
            }
        };
    }

    ListenableFuture<CausalMessages> gossip(MessageBff bff);

    void update(CausalPush push);

}
