/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion;

import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Any;
import com.google.protobuf.Timestamp;
import com.salesfoce.apollo.gorgoneion.proto.Application;
import com.salesfoce.apollo.gorgoneion.proto.Attestation;
import com.salesfoce.apollo.gorgoneion.proto.Credentials;
import com.salesfoce.apollo.gorgoneion.proto.Invitation;
import com.salesfoce.apollo.gorgoneion.proto.Nonce;
import com.salesfoce.apollo.gorgoneion.proto.SignedAttestation;
import com.salesfoce.apollo.gorgoneion.proto.SignedNonce;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.stereotomy.event.proto.Validation_;
import com.salesfoce.apollo.stereotomy.event.proto.Validations;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class Gorgoneion {
    private static final Logger log = LoggerFactory.getLogger(Gorgoneion.class);

    private final Context<Member>            context;
    private final DigestAlgorithm            digestAlgo;
    private final ControlledIdentifierMember member;
    private final Parameters                 params;

    public Gorgoneion(ControlledIdentifierMember member, Context<Member> context, Parameters params,
                      DigestAlgorithm digestAlgo) {
        super();
        this.member = member;
        this.context = context;
        this.params = params;
        this.digestAlgo = digestAlgo;
    }

    /**
     * @param request
     * @param from
     * @param responseObserver
     * @param timer
     */
    public void register(Credentials request, Digest from, StreamObserver<Invitation> responseObserver,
                         com.codahale.metrics.Timer.Context timer) {
        if (!validate(request, from)) {
            log.warn("Invalid credentials from: {} on: {}", from, member.getId());
            responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
            return;
        }
        verify(request).whenComplete((v, t) -> {
            if (t != null) {
                responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withCause(t)));
            } else if (v == null) {
                responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
            } else {
                responseObserver.onNext(Invitation.newBuilder().setValidation(v).build());
                responseObserver.onCompleted();
            }
        });
    }

    private SignedAttestation attestation(SignedNonce nonce, Any proof) {
        var now = params.clock().instant();
        var attestation = Attestation.newBuilder().setAttestation(proof)
//                                     .setKerl(member.kerl().get())
                                     .setNonce(member.sign(nonce.toByteString()).toSig())
                                     .setTimestamp(Timestamp.newBuilder()
                                                            .setSeconds(now.getEpochSecond())
                                                            .setNanos(now.getNano()))
                                     .build();
        return SignedAttestation.newBuilder()
                                .setAttestation(attestation)
                                .setSignature(member.sign(attestation.toByteString()).toSig())
                                .build();
    }

    @SuppressWarnings("unused")
    private boolean completeValidation(Member from, int majority, Digest v, CompletableFuture<Validations> validated,
                                       Optional<ListenableFuture<Invitation>> futureSailor,
                                       Map<Member, Validation_> validations) {
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

        final var validation = invite.getValidation();
        if (!validate(validation, from)) {
            return true;
        }
        if (validations.put(from, validation) == null) {
            if (validations.size() >= majority) {
                validated.complete(Validations.newBuilder().build());
                log.info("Validations acquired: {} majority: {} for view: {} context: {} on: {}", validations.size(),
                         majority, v, context.getId(), member.getId());
                return false;
            }
        }
        log.info("Validation: {} majority not achieved: {} required: {} context: {} on: {}", v, validations.size(),
                 majority, context.getId(), member.getId());
        return true;
    }

    @SuppressWarnings("unused")
    private CompletableFuture<Credentials> credentials(SignedNonce nonce) {
        return member.kerl()
                     .thenCompose(kerl -> params.attester()
                                                .apply(nonce)
                                                .thenApply(attestation -> Credentials.newBuilder()
                                                                                     .setContext(context.getId()
                                                                                                        .toDigeste())
                                                                                     .setKerl(kerl)
                                                                                     .setNonce(nonce)
                                                                                     .setAttestation(attestation(nonce,
                                                                                                                 attestation))
                                                                                     .build()));
    }

    @SuppressWarnings("unused")
    private CompletableFuture<SignedNonce> generateNonce(Application application) {
        var now = params.clock().instant();
        final var identifier = identifier(application);
        if (identifier == null) {
            var fs = new CompletableFuture<SignedNonce>();
            fs.completeExceptionally(new IllegalArgumentException("No identifier"));
            return fs;
        }
        var nonce = Nonce.newBuilder()
                         .setMember(identifier)
                         .setDuration(com.google.protobuf.Duration.newBuilder()
                                                                  .setSeconds(params.registrationTimeout()
                                                                                    .toSecondsPart())
                                                                  .setNanos(params.registrationTimeout().toNanosPart())
                                                                  .build())
                         .setIssuer(member.getIdentifier().getLastEstablishmentEvent().toEventCoords())
                         .setNoise(digestAlgo.random().toDigeste())
                         .setTimestamp(Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()))
                         .build();
        return member.getIdentifier().getSigner().thenApply(s -> {
            var validations = new ArrayList<Validation_>();
            validations.add(Validation_.newBuilder().setSignature(s.sign(nonce.toByteString()).toSig()).build());
            return validations;
        }).thenApply(validations -> SignedNonce.newBuilder().setNonce(nonce).addAllSignatures(validations).build());
    }

    private Ident identifier(Application application) {
        final var kerl = application.getKerl();
        if (ProtobufEventFactory.from(kerl.getEvents(kerl.getEventsCount() - 1))
                                .event() instanceof EstablishmentEvent establishment) {
            return establishment.getIdentifier().toIdent();
        }
        return null;
    }

    private CompletableFuture<Validation_> validate(Credentials credentials) {
        var event = credentials.getKerl().getEvents(0).getInception();
        return member.getIdentifier()
                     .getSigner()
                     .thenApply(signer -> Validation_.newBuilder()
                                                     .setValidator(member.getIdentifier()
                                                                         .getCoordinates()
                                                                         .toEventCoords())
                                                     .setSignature(signer.sign(event.toByteString()).toSig())
                                                     .build());
    }

    private boolean validate(Credentials credentials, Digest from) {
        var signedAtt = credentials.getAttestation();
        var kerl = credentials.getKerl();
        if (kerl.getEventsCount() == 0) {
            log.warn("Invalid credentials, no KERL from: {} on: {}", from, member.getId());
            return false;
        }
        if (ProtobufEventFactory.from(kerl.getEvents(kerl.getEventsCount() - 1))
                                .event() instanceof EstablishmentEvent establishment) {

            final var verifier = new Verifier.DefaultVerifier(establishment.getKeys());
            if (!verifier.verify(JohnHancock.from(signedAtt.getSignature()),
                                 signedAtt.getAttestation().toByteString())) {
                log.warn("Invalid attestation, invalid signature from: {} on: {}", establishment.getIdentifier(),
                         member.getId());
                return false;
            }
            if (!verifier.verify(JohnHancock.from(signedAtt.getAttestation().getNonce()),
                                 credentials.getNonce().toByteString())) {
                log.warn("Invalid attestation, invalid nonce signature from: {} on: {}", establishment.getIdentifier(),
                         member.getId());
                return false;
            }
            return true;
        } else {
            return false;
        }
    }

    private boolean validate(Validation_ validation, Member from) {
        return true;
    }

    private CompletableFuture<Validation_> verify(Credentials credentials) {
        return params.verifier().apply(credentials.getAttestation()).thenCompose(success -> {
            if (!success) {
                var fs = new CompletableFuture<Validation_>();
                fs.complete(null);
                return fs;
            } else {
                return validate(credentials);
            }
        });
    }
}
