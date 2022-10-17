/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Any;
import com.google.protobuf.Timestamp;
import com.salesfoce.apollo.gorgoneion.proto.Attestation;
import com.salesfoce.apollo.gorgoneion.proto.Credentials;
import com.salesfoce.apollo.gorgoneion.proto.Invitation;
import com.salesfoce.apollo.gorgoneion.proto.SignedAttestation;
import com.salesfoce.apollo.gorgoneion.proto.SignedNonce;
import com.salesforce.apollo.gorgoneion.comm.admissions.Admissions;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;

/**
 * @author hal.hildebrand
 *
 */
public class GorgoneionClient {
    private static final Logger log = LoggerFactory.getLogger(GorgoneionClient.class);

    private final Function<SignedNonce, CompletableFuture<Any>> attester;
    private final Admissions                                    client;
    private final Clock                                         clock;
    private final ControlledIdentifierMember                    member;

    public GorgoneionClient(ControlledIdentifierMember member, Function<SignedNonce, CompletableFuture<Any>> attester,
                            Clock clock, Admissions client) {
        this.member = member;
        this.attester = attester;
        this.clock = clock;
        this.client = client;
    }

    public CompletableFuture<Invitation> apply(Duration timeout) {
        var invitation = new CompletableFuture<Invitation>();
        member.kerl().whenComplete((application, t) -> {
            var fs = client.apply(application, timeout);
            fs.addListener(() -> complete(fs, invitation, timeout), r -> r.run());
        });
        return invitation;
    }

    private CompletableFuture<SignedAttestation> attestation(SignedNonce nonce, Any proof) {
        return member.kerl().thenApply(kerl -> {
            var now = clock.instant();
            var attestation = Attestation.newBuilder()
                                         .setAttestation(proof)
                                         .setKerl(kerl)
                                         .setNonce(member.sign(nonce.toByteString()).toSig())
                                         .setTimestamp(Timestamp.newBuilder()
                                                                .setSeconds(now.getEpochSecond())
                                                                .setNanos(now.getNano()))
                                         .build();
            return SignedAttestation.newBuilder()
                                    .setAttestation(attestation)
                                    .setSignature(member.sign(attestation.toByteString()).toSig())
                                    .build();
        });

    }

    private void complete(ListenableFuture<SignedNonce> fs, CompletableFuture<Invitation> invitation,
                          Duration timeout) {
        try {
            credentials(fs.get()).thenCompose(credentials -> {
                var invited = client.register(credentials, timeout);
                invited.addListener(() -> invite(invited, invitation), r -> r.run());
                return invitation;
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            log.error("Error applying on: {}", member.getId(), e.getCause());
            invitation.completeExceptionally(e.getCause());
        }
    }

    private CompletableFuture<Credentials> credentials(SignedNonce nonce) {
        return member.kerl()
                     .thenCompose(kerl -> attester.apply(nonce)
                                                  .thenCompose(attestation -> attestation(nonce, attestation))
                                                  .thenApply(sa -> Credentials.newBuilder()
                                                                              .setNonce(nonce)
                                                                              .setAttestation(sa)
                                                                              .build()));
    }

    private void invite(ListenableFuture<Invitation> invited, CompletableFuture<Invitation> invitation) {
        try {
            invitation.complete(invited.get());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            log.error("Error applying on: {}", member.getId(), e.getCause());
            invitation.completeExceptionally(e.getCause());
        }
    }
}
