/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion.comm.gather;

import java.io.IOException;
import java.time.Duration;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.gorgoneion.proto.EndorseNonce;
import com.salesfoce.apollo.stereotomy.event.proto.Validation_;
import com.salesforce.apollo.comm.Link;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public interface Endorsement extends Link {
    static Endorsement getLocalLoopback(Member member) {
        return new Endorsement() {

            @Override
            public void close() throws IOException {
            }

            @Override
            public ListenableFuture<Validation_> gather(EndorseNonce nonce, Duration timer) {
                return null;
            }

            @Override
            public Member getMember() {
                return member;
            }
        };
    }

    ListenableFuture<Validation_> gather(EndorseNonce nonce, Duration timer);
}
