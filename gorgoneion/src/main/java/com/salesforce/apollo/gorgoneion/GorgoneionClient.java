/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion;

import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.CancellationException;
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
import com.salesfoce.apollo.stereotomy.event.proto.Validations;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;

import io.grpc.StatusRuntimeException;

/**
 * @author hal.hildebrand
 *
 */
public class GorgoneionClient {
    private static final Logger log = LoggerFactory.getLogger(GorgoneionClient.class);

    private final Function<SignedNonce, CompletableFuture<Any>> attester;
    private final Clock                                         clock;
    private final Context<Member>                               context;
    private final ControlledIdentifierMember                    member;

    public GorgoneionClient(ControlledIdentifierMember member, Context<Member> context,
                            Function<SignedNonce, CompletableFuture<Any>> attester, Clock clock) {
        this.member = member;
        this.context = context;
        this.attester = attester;
        this.clock = clock;
    }

    public CompletableFuture<Credentials> credentials(SignedNonce nonce) {
        return member.kerl()
                     .thenCompose(kerl -> attester.apply(nonce)
                                                  .thenCompose(attestation -> attestation(nonce, attestation))
                                                  .thenApply(sa -> Credentials.newBuilder()
                                                                              .setContext(context.getId().toDigeste())
                                                                              .setNonce(nonce)
                                                                              .setAttestation(sa)
                                                                              .build()));
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

    @SuppressWarnings("unused")
    private boolean completeValidation(Member from, int majority, Digest v, CompletableFuture<Validations> validated,
                                       Optional<ListenableFuture<Invitation>> futureSailor) {
        if (futureSailor.isEmpty()) {
            return true;
        }
        Invitation invite;
        try {
            invite = futureSailor.get().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return true;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof StatusRuntimeException sre) {
                switch (sre.getStatus().getCode()) {
                case RESOURCE_EXHAUSTED:
                    log.trace("Resource exhausted in validation: {} with: {} : {} on: {}", v, from.getId(),
                              sre.getStatus(), member.getId());
                    break;
                case OUT_OF_RANGE:
                    log.debug("View change in validation: {} with: {} : {} on: {}", v, from.getId(), sre.getStatus(),
                              member.getId());
//                    view.resetBootstrapView();
//                    member.reset();
//                    exec.execute(() -> seeding());
                    return false;
                case DEADLINE_EXCEEDED:
                    log.trace("Validation timeout for view: {} with: {} : {} on: {}", v, from.getId(), sre.getStatus(),
                              member.getId());
                    break;
                case UNAUTHENTICATED:
                    log.trace("Validation unauthenticated for view: {} with: {} : {} on: {}", v, from.getId(),
                              sre.getStatus(), member.getId());
                    break;
                default:
                    log.warn("Failure in validation: {} with: {} : {} on: {}", v, from.getId(), sre.getStatus(),
                             member.getId());
                }
            } else {
                log.error("Failure in validation: {} with: {} on: {}", v, from.getId(), member.getId(), e.getCause());
            }
            return true;
        } catch (CancellationException e) {
            return true;
        }

        final var validations = invite.getValidations();
        if (!validate(validations, from)) {
            return true;
        }
        log.info("Validation: {} majority not achieved: {} required: {} context: {} on: {}", v,
                 validations.getValidationsCount(), majority, context.getId(), member.getId());
        return true;
    }

    private boolean validate(Validations validation, Member from) {
        return true;
    }
}
