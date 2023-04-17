/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion;

import static com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory.digestOf;

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
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
import com.salesfoce.apollo.gorgoneion.proto.MemberSignature;
import com.salesfoce.apollo.gorgoneion.proto.Nonce;
import com.salesfoce.apollo.gorgoneion.proto.Notarization;
import com.salesfoce.apollo.gorgoneion.proto.SignedNonce;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.Validation_;
import com.salesfoce.apollo.stereotomy.event.proto.Validations;
import com.salesfoce.apollo.utils.proto.Digeste;
import com.salesforce.apollo.archipelago.RouterImpl;
import com.salesforce.apollo.archipelago.RouterImpl.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.crypto.Verifier.DefaultVerifier;
import com.salesforce.apollo.gorgoneion.comm.GorgoneionMetrics;
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
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
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
        public void register(Credentials request, Digest from, StreamObserver<Validations> responseObserver,
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

        private boolean validate(KERL_ kerl, Digest from) {
            if (identifier(kerl) instanceof SelfAddressingIdentifier sai) {
                return sai.getDigest().equals(from);
            }
            return false;
        }

    }

    private class Endorse implements EndorsementService {

        @Override
        public CompletableFuture<MemberSignature> endorse(Nonce request, Digest from) {
            if (!validate(request, from)) {
                log.warn("Invalid endorsement nonce from: {} on: {}", from, member.getId());
                var fs = new CompletableFuture<MemberSignature>();
                fs.completeExceptionally(new StatusRuntimeException(Status.UNAUTHENTICATED.withDescription("Invalid endorsement nonce")));
                return fs;
            }
            log.info("Endorsing nonce for: {} from: {} on: {}", Identifier.from(request.getMember()), from,
                     member.getId());
            return Gorgoneion.this.endorse(request);
        }

        @Override
        public CompletableFuture<Empty> enroll(Notarization request, Digest from) {
            var kerl = request.getKerl();
            var identifier = identifier(kerl);
            if (!validate(request, identifier, kerl, from)) {
                log.warn("Invalid notarization for: {} from: {} on: {}", identifier, from, member.getId());
                var fs = new CompletableFuture<Empty>();
                fs.completeExceptionally(new StatusRuntimeException(Status.UNAUTHENTICATED.withDescription("Invalid notarization")));
                return fs;
            }
            log.info("Enrolling notorization for: {} from: {} on: {}", identifier, from, member.getId());
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
            final var issuer = Digest.from(request.getIssuer());
            if (!context.isMember(issuer)) {
                log.warn("Invalid nonce, non existent issuer: {} from: {} on: {}", issuer, from, member.getId());
                return false;
            }
            if (!from.equals(issuer)) {
                log.warn("Invalid nonce, issuer: {} not requester: {} on: {}", issuer, from, member.getId());
                return false;
            }
            if (request.getNoise().equals(Digeste.getDefaultInstance())) {
                log.warn("Invalid nonce, missing noise from: {} on: {}", from, member.getId());
                return false;
            }
            if (request.getMember().equals(Ident.getDefaultInstance())) {
                log.warn("Invalid nonce, missing member from: {} on: {}", from, member.getId());
                return false;
            }
            var nInstant = Instant.ofEpochSecond(request.getTimestamp().getSeconds(),
                                                 request.getTimestamp().getNanos());
            final var now = Instant.now();
            if (now.isBefore(nInstant) || nInstant.plus(parameters.maxDuration()).isBefore(now)) {
                log.warn("Invalid nonce, invalid timestamp: {} from: {} on: {}", nInstant, from, member.getId());
                return false;
            }
            log.info("Validated nonce from: {} on: {}", from, member.getId());
            return true;
        }

        private boolean validate(Notarization request, Identifier identifier, KERL_ kerl, Digest from) {
            if (ProtobufEventFactory.from(kerl.getEvents(kerl.getEventsCount() - 1))
                                    .event() instanceof EstablishmentEvent establishment) {
                var count = 0;
                for (var validation : request.getValidations().getValidationsList()) {
                    try {
                        if (new DefaultVerifier(parameters.kerl()
                                                          .getKeyState(EventCoordinates.from(validation.getValidator()))
                                                          .get()
                                                          .getKeys()).verify(JohnHancock.from(validation.getSignature()),
                                                                             establishment.toKeyEvent_()
                                                                                          .toByteString())) {
                            count++;
                        } else {
                            log.warn("Invalid notarization, invalid validation for: {} from: {} on: {}", identifier,
                                     from, member.getId());
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        log.warn("Invalid notarization, invalid validation for: {} from: {} on: {}", identifier, from,
                                 member.getId(), e);
                    }
                }
                final var majority = count >= context.majority();
                if (!majority) {
                    log.warn("Invalid notarization, no majority: {} required: {} for: {} from: {} on: {}", count,
                             context.majority(), identifier, from, member.getId());
                }
                return majority;
            } else {
                log.warn("Invalid notarization, invalid kerl for: {} from: {} on: {}", identifier, from,
                         member.getId());
                return false;
            }
        }

        private boolean validateCredentials(Credentials credentials, Digest from) {
            var sn = credentials.getNonce();
            final var issuer = Digest.from(sn.getNonce().getIssuer());
            if (!context.isMember(issuer)) {
                log.warn("Invalid credential nonce, non existent issuer: {} from: {} on: {}", issuer, from,
                         member.getId());
                return false;
            }
            if (!from.equals(issuer)) {
                log.warn("Invalid credential nonce, issuer: {} not requester: {} on: {}", issuer, from, member.getId());
                return false;
            }
            if (sn.getNonce().getNoise().equals(Digeste.getDefaultInstance())) {
                log.warn("Invalid credential nonce, missing noise from: {} on: {}", from, member.getId());
                return false;
            }
            var nInstant = Instant.ofEpochSecond(sn.getNonce().getTimestamp().getSeconds(),
                                                 sn.getNonce().getTimestamp().getNanos());
            final var now = Instant.now();
            if (now.isBefore(nInstant) || nInstant.plus(parameters.maxDuration()).isBefore(now)) {
                log.warn("Invalid credential nonce, invalid timestamp: {} from: {} on: {}", nInstant, from,
                         member.getId());
                return false;
            }

            final var serialized = sn.getNonce().toByteString();
            var count = 0;
            var issuerSigned = false;
            for (var signature : sn.getSignaturesList()) {
                final var id = Digest.from(signature.getId());
                var m = context.getMember(id);
                if (m == null) {
                    log.warn("Credential nonce, unknown signing member: {} from: {} on: {}", m, from, member.getId());
                    continue;
                }
                if (!m.verify(JohnHancock.from(signature.getSignature()), serialized)) {
                    log.warn("Credential nonce, invalid signature of: {} from: {} on: {}", m, from, member.getId());
                    continue;
                }
                if (!issuerSigned && issuer.equals(id)) {
                    issuerSigned = true;
                }
                count++;
            }

            if (count < context.majority()) {
                log.warn("Invalid credential nonce, no majority signature: {} required > {} from: {} on: {}", count,
                         context.majority(), from, member.getId());
                return false;
            }

            log.info("Valid credential nonce for: {} from: {} on: {}", Identifier.from(sn.getNonce().getMember()), from,
                     member.getId());

            var sa = credentials.getAttestation();
            final var kerl = sa.getAttestation().getKerl();
            var identifier = identifier(kerl);
            if (identifier == null) {
                log.warn("Invalid credential attestation, invalid identifier from: {} on: {}", from, member.getId());
                return false;
            }
            var m = Identifier.from(sn.getNonce().getMember());
            if (!m.equals(identifier)) {
                log.warn("Invalid credential attestation, identifier: {} not equal to nonce member: {} from: {} on: {}",
                         identifier, m, from, member.getId());
                return false;
            }
            if (identifier instanceof SelfAddressingIdentifier sai) {
            } else {
                log.warn("Invalid credential attestation, invalid identifier: {} from: {} on: {}", identifier, from,
                         member.getId());
                return false;
            }
            var aInstant = Instant.ofEpochSecond(sa.getAttestation().getTimestamp().getSeconds(),
                                                 sa.getAttestation().getTimestamp().getNanos());
            if (now.isBefore(aInstant) || aInstant.plus(parameters.maxDuration()).isBefore(now) ||
                aInstant.isBefore(nInstant)) {
                log.warn("Invalid credential attestation, invalid timestamp: {} for: {} from: {} on: {}", aInstant,
                         identifier, from, member.getId());
                return false;
            }
            if (ProtobufEventFactory.from(kerl.getEvents(kerl.getEventsCount() - 1))
                                    .event() instanceof EstablishmentEvent establishment) {
                final var verifier = new Verifier.DefaultVerifier(establishment.getKeys());
                if (!verifier.verify(JohnHancock.from(sa.getAttestation().getNonce()), sn.toByteString())) {
                    log.warn("Invalid credential attestation, invalid nonce signature for: {} from: {} on: {}",
                             identifier, from, member.getId());
                    return false;
                }
            } else {
                log.warn("Invalid credential attestation, invalid kerl for: {} from: {} on: {}", identifier, from,
                         member.getId());
                return false;
            }
            log.info("Valid credential attestation for: {} from: {} on: {}", identifier, from, member.getId());
            return true;
        }
    }

    public static final Logger log = LoggerFactory.getLogger(Gorgoneion.class);

    @SuppressWarnings("unused")
    private final CommonCommunications<?, AdmissionsService>            admissionsComm;
    private final Context<Member>                                       context;
    private final CommonCommunications<Endorsement, EndorsementService> endorsementComm;
    private final Executor                                              exec;
    private final ControlledIdentifierMember                            member;
    private final ProtoEventObserver                                    observer;
    private final Parameters                                            parameters;

    private final ScheduledExecutorService scheduler;

    public Gorgoneion(Parameters parameters, ControlledIdentifierMember member, Context<Member> context,
                      ProtoEventObserver observer, RouterImpl router, ScheduledExecutorService scheduler,
                      GorgoneionMetrics metrics, Executor exec) {
        this(parameters, member, context, observer, router, scheduler, metrics, router, exec);
    }

    public Gorgoneion(Parameters parameters, ControlledIdentifierMember member, Context<Member> context,
                      ProtoEventObserver observer, RouterImpl admissionsRouter, ScheduledExecutorService scheduler,
                      GorgoneionMetrics metrics, RouterImpl endorsementRouter, Executor exec) {
        this.member = member;
        this.context = context;
        this.exec = exec;
        this.parameters = parameters;
        this.scheduler = scheduler;
        this.observer = observer;

        admissionsComm = admissionsRouter.create(member, context.getId(), new Admit(), ":admissions",
                                                 r -> new AdmissionsServer(admissionsRouter.getClientIdentityProvider(),
                                                                           r, metrics));

        final var service = new Endorse();
        endorsementComm = endorsementRouter.create(member, context.getId(), service, ":endorsement",
                                                   r -> new EndorsementServer(admissionsRouter.getClientIdentityProvider(),
                                                                              r, metrics),
                                                   EndorsementClient.getCreate(metrics),
                                                   Endorsement.getLocalLoopback(member, service));
    }

    private boolean completeEndorsement(Optional<ListenableFuture<MemberSignature>> futureSailor, Member from,
                                        Set<MemberSignature> validations) {
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
            if (e.getCause() instanceof StatusRuntimeException sre) {
                log.error("Error validating nonce: {} on: {}", sre.getStatus(), member.getId());
            } else {
                log.error("Error validating nonce on: {}", member.getId(), e.getCause());
            }
        }
        return true;
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
            if (e.getCause() instanceof StatusRuntimeException sre) {
                log.error("Error enrolling: {} on: {}", sre.getStatus(), member.getId());
            } else {
                log.error("Error enrolling on: {}", member.getId(), e.getCause());
            }
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
            if (e.getCause() instanceof StatusRuntimeException sre) {
                log.error("Error verifying credentials: {} on: {}", sre.getStatus(), member.getId());
            } else {
                log.error("Error verifying credentials on: {}", member.getId(), e.getCause());
            }
        }
        return true;
    }

    private CompletableFuture<MemberSignature> endorse(Nonce request) {
        var fs = new CompletableFuture<MemberSignature>();
        fs.complete(MemberSignature.newBuilder()
                                   .setId(member.getId().toDigeste())
                                   .setSignature(member.sign(request.toByteString()).toSig())
                                   .build());
        return fs;
    }

    private CompletableFuture<Empty> enroll(Notarization request) {
        return observer.publish(request.getKerl(), Collections.singletonList(request.getValidations()))
                       .thenApply(v -> Empty.getDefaultInstance());
    }

    private CompletableFuture<SignedNonce> generateNonce(KERL_ application) {
        var generated = new CompletableFuture<SignedNonce>();
        final var identifier = identifier(application);
        if (identifier == null) {
            generated.completeExceptionally(new IllegalArgumentException("No identifier"));
            return generated;
        }
        log.debug("Generating nonce for: {} contacting: {} on: {}", identifier, identifier, member.getId());
        var now = parameters.clock().instant();
        final var ident = identifier.toIdent();
        var nonce = Nonce.newBuilder()
                         .setMember(ident)
                         .setIssuer(member.getId().toDigeste())
                         .setNoise(parameters.digestAlgorithm().random().toDigeste())
                         .setTimestamp(Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()))
                         .build();

        var successors = context.totalCount() == 1 ? Collections.singletonList(member)
                                                   : Context.uniqueSuccessors(context,
                                                                              digestOf(ident,
                                                                                       parameters.digestAlgorithm()));
        final var majority = context.totalCount() == 1 ? 1 : context.majority();
        final var redirecting = new SliceIterator<>("Nonce Endorsement", member, successors, endorsementComm, exec);
        Set<MemberSignature> endorsements = Collections.newSetFromMap(new ConcurrentHashMap<>());
        redirecting.iterate((link, m) -> {
            log.info("Request signing nonce for: {} contacting: {} on: {}", identifier, link.getMember().getId(),
                     member.getId());
            return link.endorse(nonce, parameters.registrationTimeout());
        }, (futureSailor, link, m) -> completeEndorsement(futureSailor, m, endorsements), () -> {
            if (endorsements.size() < majority) {
                generated.completeExceptionally(new StatusRuntimeException(Status.ABORTED.withDescription("Cannot gather required nonce endorsements")));
            } else {
                generated.complete(SignedNonce.newBuilder()
                                              .addSignatures(MemberSignature.newBuilder()
                                                                            .setId(member.getId().toDigeste())
                                                                            .setSignature(member.sign(nonce.toByteString())
                                                                                                .toSig())
                                                                            .build())
                                              .setNonce(nonce)
                                              .addAllSignatures(endorsements)
                                              .build());
                log.info("Generated nonce for: {} signatures: {} on: {}", identifier, endorsements.size(),
                         member.getId());
            }
        }, scheduler, parameters.frequency());
        return generated;
    }

    private Identifier identifier(KERL_ kerl) {
        if (ProtobufEventFactory.from(kerl.getEvents(kerl.getEventsCount() - 1))
                                .event() instanceof EstablishmentEvent establishment) {
            return establishment.getIdentifier();
        }
        return null;
    }

    private CompletableFuture<Validations> notarize(Credentials credentials, Validations validations,
                                                    CompletableFuture<Validations> invited) {
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

        var successors = Context.uniqueSuccessors(context,
                                                  digestOf(identifier.toIdent(), parameters.digestAlgorithm()));
        final var majority = context.activeCount() == 1 ? 0 : context.majority();
        final var redirecting = new SliceIterator<>("Enrollment", member, successors, endorsementComm, exec);
        var completed = new HashSet<Member>();
        redirecting.iterate((link, m) -> {
            log.debug("Enrolling: {} contacting: {} on: {}", identifier, link.getMember().getId(), member.getId());
            return link.enroll(notarization, parameters.registrationTimeout());
        }, (futureSailor, link, m) -> completeEnrollment(futureSailor, m, completed), () -> {
            if (completed.size() < majority) {
                invited.completeExceptionally(new StatusRuntimeException(Status.ABORTED.withDescription("Cannot complete enrollment")));
            } else {
                invited.complete(validations);
            }
        }, scheduler, parameters.frequency());
        return invited;
    }

    private CompletableFuture<Validations> register(Credentials request) {
        var invited = new CompletableFuture<Validations>();
        final var kerl = request.getAttestation().getAttestation().getKerl();
        final var identifier = identifier(kerl);
        if (identifier == null) {
            invited.completeExceptionally(new IllegalArgumentException("No identifier"));
            return invited;
        }
        log.debug("Validating credentials for: {} nonce signatures: {} on: {}", identifier,
                  request.getNonce().getSignaturesCount(), member.getId());

        var validated = new CompletableFuture<Validations>();

        var successors = Context.uniqueSuccessors(context,
                                                  digestOf(identifier.toIdent(), parameters.digestAlgorithm()));
        final var majority = context.activeCount() == 1 ? 0 : context.majority();
        final var redirecting = new SliceIterator<>("Credential verification", member, successors, endorsementComm,
                                                    exec);
        var verifications = new HashSet<Validation_>();
        redirecting.iterate((link, m) -> {
            log.debug("Validating  credentials for: {} contacting: {} on: {}", identifier, link.getMember().getId(),
                      member.getId());
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
                log.debug("Validated credentials for: {} verifications: {} on: {}", identifier, verifications.size(),
                          member.getId());
            }
        }, scheduler, parameters.frequency());
        return validated.thenCompose(v -> notarize(request, v, invited));
    }

    private CompletableFuture<Validation_> validate(Credentials credentials) {
        var event = (com.salesforce.apollo.stereotomy.event.InceptionEvent) ProtobufEventFactory.from(credentials.getAttestation()
                                                                                                                 .getAttestation()
                                                                                                                 .getKerl()
                                                                                                                 .getEvents(0))
                                                                                                .event();
        log.info("Validating credentials for: {} on: {}", event.getIdentifier(), member.getId());
        return member.getIdentifier().getSigner().thenApply(signer -> {
            return Validation_.newBuilder()
                              .setValidator(member.getIdentifier().getCoordinates().toEventCoords())
                              .setSignature(signer.sign(event.toKeyEvent_().toByteString()).toSig())
                              .build();
        });
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
