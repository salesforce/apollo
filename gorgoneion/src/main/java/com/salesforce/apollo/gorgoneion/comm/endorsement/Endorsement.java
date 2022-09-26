/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion.comm.endorsement;

import java.io.IOException;
import java.time.Duration;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Empty;
import com.salesfoce.apollo.gorgoneion.proto.Credentials;
import com.salesfoce.apollo.gorgoneion.proto.EndorseNonce;
import com.salesfoce.apollo.gorgoneion.proto.Notarization;
import com.salesfoce.apollo.stereotomy.event.proto.Validation_;
import com.salesforce.apollo.comm.Link;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
public interface Endorsement extends Link {
    static Endorsement getLocalLoopback(Member member, EndorsementService service) {
        return new Endorsement() {

            @Override
            public void close() throws IOException {
            }

            @Override
            public ListenableFuture<Validation_> endorse(EndorseNonce nonce, Duration timer) {
                SettableFuture<Validation_> f = SettableFuture.create();
                service.endorse(nonce, member.getId()).whenComplete((e, t) -> {
                    if (t != null) {
                        f.setException(t);
                    } else {
                        f.set(e);
                    }
                });
                return f;
            }

            @Override
            public ListenableFuture<Empty> enroll(Notarization notarization, Duration timeout) {
                SettableFuture<Empty> f = SettableFuture.create();
                service.enroll(notarization, member.getId()).whenComplete((e, t) -> {
                    if (t != null) {
                        f.setException(t);
                    } else {
                        f.set(e);
                    }
                });
                return f;
            }

            @Override
            public Member getMember() {
                return member;
            }

            @Override
            public ListenableFuture<Validation_> validate(Credentials credentials, Duration timeout) {
                SettableFuture<Validation_> f = SettableFuture.create();
                service.validate(credentials, member.getId()).whenComplete((e, t) -> {
                    if (t != null) {
                        f.setException(t);
                    } else {
                        f.set(e);
                    }
                });
                return f;
            }
        };
    }

    ListenableFuture<Validation_> endorse(EndorseNonce nonce, Duration timer);

    ListenableFuture<Empty> enroll(Notarization notarization, Duration timeout);

    ListenableFuture<Validation_> validate(Credentials credentials, Duration timeout);
}
