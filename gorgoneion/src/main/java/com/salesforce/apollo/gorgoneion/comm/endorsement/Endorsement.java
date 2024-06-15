/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion.comm.endorsement;

import com.salesforce.apollo.archipelago.Link;
import com.salesforce.apollo.gorgoneion.proto.Credentials;
import com.salesforce.apollo.gorgoneion.proto.MemberSignature;
import com.salesforce.apollo.gorgoneion.proto.Nonce;
import com.salesforce.apollo.gorgoneion.proto.Notarization;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.stereotomy.event.proto.Validation_;

import java.io.IOException;
import java.time.Duration;

/**
 * @author hal.hildebrand
 */
public interface Endorsement extends Link {
    static Endorsement getLocalLoopback(Member member, EndorsementService service) {
        return new Endorsement() {

            @Override
            public void close() throws IOException {
            }

            @Override
            public MemberSignature endorse(Nonce nonce, Duration timer) {
                return service.endorse(nonce, member.getId());
            }

            @Override
            public void enroll(Notarization notarization, Duration timeout) {
                service.enroll(notarization, member.getId());
            }

            @Override
            public Member getMember() {
                return member;
            }

            @Override
            public Validation_ validate(Credentials credentials, Duration timeout) {
                return service.validate(credentials, member.getId());
            }
        };
    }

    MemberSignature endorse(Nonce nonce, Duration timer);

    void enroll(Notarization notarization, Duration timeout);

    Validation_ validate(Credentials credentials, Duration timeout);
}
