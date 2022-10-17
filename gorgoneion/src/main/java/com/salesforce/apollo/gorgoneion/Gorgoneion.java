/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion;

import static com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory.digestOf;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import com.salesfoce.apollo.gorgoneion.proto.Credentials;
import com.salesfoce.apollo.gorgoneion.proto.Invitation;
import com.salesfoce.apollo.gorgoneion.proto.Nonce;
import com.salesfoce.apollo.gorgoneion.proto.Notarization;
import com.salesfoce.apollo.gorgoneion.proto.SignedNonce;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.Validation_;
import com.salesfoce.apollo.stereotomy.event.proto.Validations;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.gorgoneion.comm.GorgoneionMetrics;
import com.salesforce.apollo.gorgoneion.comm.admissions.Admissions;
import com.salesforce.apollo.gorgoneion.comm.admissions.AdmissionsClient;
import com.salesforce.apollo.gorgoneion.comm.admissions.AdmissionsServer;
import com.salesforce.apollo.gorgoneion.comm.admissions.AdmissionsService;
import com.salesforce.apollo.gorgoneion.comm.endorsement.Endorsement;
import com.salesforce.apollo.gorgoneion.comm.endorsement.EndorsementClient;
import com.salesforce.apollo.gorgoneion.comm.endorsement.EndorsementServer;
import com.salesforce.apollo.gorgoneion.comm.endorsement.EndorsementService;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.ring.SliceIterator;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.services.proto.ProtoEventObserver;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class Gorgoneion {
    private class Admit implements AdmissionsService {

        @Override
        public void apply(KERL_ request, Digest from, StreamObserver<SignedNonce> responseObserver,
                          Timer.Context time) {
            if (!validate(request, from)) {
                log.warn("Invalid application from: {} on: {}", from, member.getId());
                responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED.withDescription("Invalid application")));
                return;
            }
            generateNonce(request).whenComplete((sn, t) -> {
                if (t != null) {
                    if (t instanceof StatusRuntimeException sre) {
                        responseObserver.onError(t);
                    } else {
                        responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withCause(t)
                                                                                           .withDescription(t.toString())));
                    }
                } else if (sn == null) {
                    responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED.withDescription("Invalid application")));
                } else {
                    responseObserver.onNext(sn);
                    responseObserver.onCompleted();
                }
            });
        }

        @Override
        public void register(Credentials request, Digest from, StreamObserver<Invitation> responseObserver,
                             Timer.Context timer) {
            if (!validate(request, from)) {
                log.warn("Invalid credentials from: {} on: {}", from, member.getId());
                responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED.withDescription("Invalid credentials")));
                return;
            }
            Gorgoneion.this.register(request).whenComplete((invite, t) -> {
                if (t != null) {
                    responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withCause(t)));
                } else if (invite == null) {
                    responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED.withDescription("Invalid credentials")));
                } else {
                    responseObserver.onNext(invite);
                    responseObserver.onCompleted();
                }
            });
        }

        private boolean validate(Credentials credentials, Digest from) {
            var signedAtt = credentials.getAttestation();
            var kerl = signedAtt.getAttestation().getKerl();
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
                    log.warn("Invalid attestation, invalid nonce signature from: {} on: {}",
                             establishment.getIdentifier(), member.getId());
                    return false;
                }
                return true;
            } else {
                return false;
            }
        }

        private boolean validate(KERL_ request, Digest from) {
            // TODO Auto-generated method stub
            return true;
        }

    }

    private class Endorse implements EndorsementService {

        @Override
        public CompletableFuture<Validation_> endorse(Nonce request, Digest from) {
            if (!validate(request, from)) {
                log.warn("Invalid endorsement nonce from: {} on: {}", from, member.getId());
                var fs = new CompletableFuture<Validation_>();
                fs.completeExceptionally(new StatusRuntimeException(Status.UNAUTHENTICATED.withDescription("Invalid endorsement nonce")));
                return fs;
            }
            return Gorgoneion.this.endorse(request);
        }

        @Override
        public CompletableFuture<Empty> enroll(Notarization request, Digest from) {
            if (!validate(request, from)) {
                log.warn("Invalid notarization from: {} on: {}", from, member.getId());
                var fs = new CompletableFuture<Empty>();
                fs.completeExceptionally(new StatusRuntimeException(Status.UNAUTHENTICATED.withDescription("Invalid notarization")));
                return fs;
            }
            return Gorgoneion.this.enroll(request);
        }

        @Override
        public CompletableFuture<Validation_> validate(Credentials credentials, Digest from) {
            if (!validateCredentials(credentials, from)) {
                log.warn("Invalid credentials from: {} on: {}", from, member.getId());
                var fs = new CompletableFuture<Validation_>();
                fs.completeExceptionally(new StatusRuntimeException(Status.UNAUTHENTICATED.withDescription("Invalid credentials")));
                return fs;
            }
            return verificationOf(credentials);
        }

        private boolean validate(Nonce request, Digest from) {
            // TODO Auto-generated method stub
            return true;
        }

        private boolean validate(Notarization request, Digest from) {
            // TODO Auto-generated method stub
            return true;
        }

        private boolean validateCredentials(Credentials credentials, Digest from) {
            // TODO Auto-generated method stub
            return true;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(Gorgoneion.class);

    @SuppressWarnings("unused")
    private final CommonCommunications<Admissions, AdmissionsService>   admissionsComm;
    private final Context<Member>                                       context;
    private final CommonCommunications<Endorsement, EndorsementService> endorsementComm;
    private final Executor                                              exec;
    private final ControlledIdentifierMember                            member;
    private final ProtoEventObserver                                    observer;
    private final Parameters                                            parameters;
    private final ScheduledExecutorService                              scheduler;

    public Gorgoneion(Parameters parameters, ControlledIdentifierMember member, Context<Member> context,
                      ProtoEventObserver observer, Router router, ScheduledExecutorService scheduler,
                      GorgoneionMetrics metrics, Executor exec) {
        this(parameters, member, context, observer, router, scheduler, metrics, router, exec);
    }

    public Gorgoneion(Parameters parameters, ControlledIdentifierMember member, Context<Member> context,
                      ProtoEventObserver observer, Router admissionsRouter, ScheduledExecutorService scheduler,
                      GorgoneionMetrics metrics, Router endorsementRouter, Executor exec) {
        this.member = member;
        this.context = context;
        this.exec = exec;
        this.parameters = parameters;
        this.scheduler = scheduler;
        this.observer = observer;

        admissionsComm = admissionsRouter.create(member, context.getId(), new Admit(), ":admissions",
                                                 r -> new AdmissionsServer(admissionsRouter.getClientIdentityProvider(),
                                                                           r, metrics),
                                                 AdmissionsClient.getCreate(metrics),
                                                 Admissions.getLocalLoopback(member));

        final var service = new Endorse();
        endorsementComm = endorsementRouter.create(member, context.getId(), service, ":endorsement",
                                                   r -> new EndorsementServer(admissionsRouter.getClientIdentityProvider(),
                                                                              r, metrics),
                                                   EndorsementClient.getCreate(metrics),
                                                   Endorsement.getLocalLoopback(member, service));
    }

    private boolean completeEnrollment(Optional<ListenableFuture<Empty>> futureSailor, Member m,
                                       HashSet<Member> completed) {
        if (futureSailor.isEmpty()) {
            return true;
        }
        try {
            futureSailor.get().get();
            completed.add(m);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException e) {
            log.error("Error enrolling on: {}", member.getId(), e.getCause());
        }
        return true;
    }

    private boolean completeValidation(Optional<ListenableFuture<Validation_>> futureSailor, Member from,
                                       HashSet<Validation_> validations) {
        if (futureSailor.isEmpty()) {
            return true;
        }
        try {
            var v = futureSailor.get().get();
            validations.add(v);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException e) {
            log.error("Error validating nonce on: {}", member.getId(), e.getCause());
        }
        return true;
    }

    private boolean completeVerification(Optional<ListenableFuture<Validation_>> futureSailor, Member m,
                                         HashSet<Validation_> verifications) {
        if (futureSailor.isEmpty()) {
            return true;
        }
        try {
            var v = futureSailor.get().get();
            verifications.add(v);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException e) {
            log.error("Error verifying credentials on: {}", member.getId(), e.getCause());
        }
        return true;
    }

    private CompletableFuture<Validation_> endorse(Nonce request) {
        var fs = new CompletableFuture<Validation_>();
        fs.complete(Validation_.newBuilder()
                               .setValidator(member.getIdentifier().getCoordinates().toEventCoords())
                               .setSignature(member.sign(request.toByteString()).toSig())
                               .build());
        return fs;
    }

    private CompletableFuture<Empty> enroll(Notarization request) {
        var fs = new CompletableFuture<Empty>();
        observer.publish(request.getKerl(), Collections.singletonList(request.getValidations()));
        fs.complete(Empty.getDefaultInstance());
        return fs;
    }

    private CompletableFuture<SignedNonce> generateNonce(KERL_ application) {
        var generated = new CompletableFuture<SignedNonce>();
        final var identifier = identifier(application);
        if (identifier == null) {
            generated.completeExceptionally(new IllegalArgumentException("No identifier"));
            return generated;
        }
        var now = parameters.clock().instant();
        var nonce = Nonce.newBuilder()
                         .setMember(identifier)
                         .setIssuer(member.getIdentifier().getLastEstablishmentEvent().toEventCoords())
                         .setNoise(parameters.digestAlgorithm().random().toDigeste())
                         .setTimestamp(Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()))
                         .build();

        var successors = context.successors(digestOf(identifier, parameters.digestAlgorithm()));
        final var majority = context.activeCount() == 1 ? 0 : context.majority();
        final var redirecting = new SliceIterator<>("Nonce Endorsement", member, successors, endorsementComm, exec);
        var validations = new HashSet<Validation_>();
        redirecting.iterate((link, m) -> {
            log.debug("Validating nonce for: {} contacting: {} on: {}", Identifier.from(identifier),
                      link.getMember().getId(), member.getId());
            return link.endorse(nonce, parameters.registrationTimeout());
        }, (futureSailor, link, m) -> completeValidation(futureSailor, m, validations), () -> {
            if (validations.size() < majority) {
                generated.completeExceptionally(new StatusRuntimeException(Status.ABORTED.withDescription("Cannot gather required nonce endorsements")));
            } else {
                generated.complete(SignedNonce.newBuilder().setNonce(nonce).addAllSignatures(validations).build());
            }
        }, scheduler, parameters.frequency());
        return generated;
    }

    private Ident identifier(KERL_ kerl) {
        if (ProtobufEventFactory.from(kerl.getEvents(kerl.getEventsCount() - 1))
                                .event() instanceof EstablishmentEvent establishment) {
            return establishment.getIdentifier().toIdent();
        }
        return null;
    }

    private CompletableFuture<Invitation> notarize(Credentials credentials, Validations validations,
                                                   CompletableFuture<Invitation> invited) {
        final var kerl = credentials.getAttestation().getAttestation().getKerl();
        final var identifier = identifier(kerl);
        if (identifier == null) {
            invited.completeExceptionally(new IllegalArgumentException("No identifier"));
            return invited;
        }

        var notarization = Notarization.newBuilder()
                                       .setKerl(credentials.getAttestation().getAttestation().getKerl())
                                       .setValidations(validations)
                                       .build();

        var successors = context.successors(digestOf(identifier, parameters.digestAlgorithm()));
        final var majority = context.activeCount() == 1 ? 0 : context.majority();
        final var redirecting = new SliceIterator<>("Enrollment", member, successors, endorsementComm, exec);
        var completed = new HashSet<Member>();
        redirecting.iterate((link, m) -> {
            log.debug("Enrolling: {} contacting: {} on: {}", Identifier.from(identifier), link.getMember().getId(),
                      member.getId());
            return link.enroll(notarization, parameters.registrationTimeout());
        }, (futureSailor, link, m) -> completeEnrollment(futureSailor, m, completed), () -> {
            if (completed.size() < majority) {
                invited.completeExceptionally(new StatusRuntimeException(Status.ABORTED.withDescription("Cannot complete enrollment")));
            } else {
                invited.complete(Invitation.newBuilder().setValidations(validations).build());
            }
        }, scheduler, parameters.frequency());
        return invited;
    }

    private CompletableFuture<Invitation> register(Credentials request) {
        var invited = new CompletableFuture<Invitation>();
        final var kerl = request.getAttestation().getAttestation().getKerl();
        final var identifier = identifier(kerl);
        if (identifier == null) {
            invited.completeExceptionally(new IllegalArgumentException("No identifier"));
            return invited;
        }

        var validated = new CompletableFuture<Validations>();

        var successors = context.successors(digestOf(identifier, parameters.digestAlgorithm()));
        final var majority = context.activeCount() == 1 ? 0 : context.majority();
        final var redirecting = new SliceIterator<>("Credential verification", member, successors, endorsementComm,
                                                    exec);
        var verifications = new HashSet<Validation_>();
        redirecting.iterate((link, m) -> {
            log.debug("Validating  credentials for: {} contacting: {} on: {}", Identifier.from(identifier),
                      link.getMember().getId(), member.getId());
            return link.validate(request, parameters.registrationTimeout());
        }, (futureSailor, link, m) -> completeVerification(futureSailor, m, verifications), () -> {
            if (verifications.size() < majority) {
                invited.completeExceptionally(new StatusRuntimeException(Status.ABORTED.withDescription("Cannot gather required credential validations")));
            } else {
                validated.complete(Validations.newBuilder()
                                              .setCoordinates(ProtobufEventFactory.from(kerl.getEvents(kerl.getEventsCount()
                                              - 1)).event().getCoordinates().toEventCoords())
                                              .addAllValidations(verifications)
                                              .build());
            }
        }, scheduler, parameters.frequency());
        return validated.thenCompose(v -> notarize(request, v, invited));
    }

    private CompletableFuture<Validation_> validate(Credentials credentials) {
        var event = credentials.getAttestation().getAttestation().getKerl().getEvents(0).getInception();
        return member.getIdentifier()
                     .getSigner()
                     .thenApply(signer -> Validation_.newBuilder()
                                                     .setValidator(member.getIdentifier()
                                                                         .getCoordinates()
                                                                         .toEventCoords())
                                                     .setSignature(signer.sign(event.toByteString()).toSig())
                                                     .build());
    }

    private CompletableFuture<Validation_> verificationOf(Credentials credentials) {
        return parameters.verifier().apply(credentials.getAttestation()).thenCompose(success -> {
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
