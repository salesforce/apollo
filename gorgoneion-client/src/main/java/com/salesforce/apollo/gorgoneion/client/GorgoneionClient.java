/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion.client;

import com.google.protobuf.Any;
import com.google.protobuf.Timestamp;
import com.salesforce.apollo.gorgoneion.client.client.comm.Admissions;
import com.salesforce.apollo.gorgoneion.proto.*;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.event.proto.KERL_;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.function.Function;

/**
 * @author hal.hildebrand
 */
public class GorgoneionClient {
    private static final Logger log = LoggerFactory.getLogger(GorgoneionClient.class);

    private final Function<SignedNonce, Any> attester;
    private final Admissions                 client;
    private final Clock                      clock;
    private final ControlledIdentifierMember member;

    public GorgoneionClient(ControlledIdentifierMember member, Function<SignedNonce, Any> attester, Clock clock,
                            Admissions client) {
        this.member = member;
        this.attester = attester;
        this.clock = clock;
        this.client = client;
    }

    public Establishment apply(Duration timeout) {
        KERL_ application = member.kerl();
        var fs = client.apply(application, timeout);
        Credentials credentials = credentials(fs);
        return client.register(credentials, timeout);
    }

    private SignedAttestation attestation(SignedNonce nonce, Any proof) {
        KERL_ kerl = member.kerl();
        var now = clock.instant();
        var attestation = Attestation.newBuilder()
                                     .setAttestation(proof)
                                     .setKerl(kerl)
                                     .setNonce(member.sign(nonce.toByteString()).toSig())
                                     .setTimestamp(
                                     Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()))
                                     .build();
        return SignedAttestation.newBuilder()
                                .setAttestation(attestation)
                                .setSignature(member.sign(attestation.toByteString()).toSig())
                                .build();

    }

    private Credentials credentials(SignedNonce nonce) {
        KERL_ kerl = member.kerl();
        var attestation = attester.apply(nonce);
        var sa = attestation(nonce, attestation);
        return Credentials.newBuilder().setNonce(nonce).setAttestation(sa).build();
    }
}
