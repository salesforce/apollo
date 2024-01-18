/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion;

import com.codahale.metrics.Timer;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.RouterImpl.CommonCommunications;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.JohnHancock;
import com.salesforce.apollo.cryptography.Signer;
import com.salesforce.apollo.cryptography.Verifier;
import com.salesforce.apollo.cryptography.Verifier.DefaultVerifier;
import com.salesforce.apollo.cryptography.proto.Digeste;
import com.salesforce.apollo.gorgoneion.comm.GorgoneionMetrics;
import com.salesforce.apollo.gorgoneion.comm.admissions.AdmissionsServer;
import com.salesforce.apollo.gorgoneion.comm.admissions.AdmissionsService;
import com.salesforce.apollo.gorgoneion.comm.endorsement.Endorsement;
import com.salesforce.apollo.gorgoneion.comm.endorsement.EndorsementClient;
import com.salesforce.apollo.gorgoneion.comm.endorsement.EndorsementServer;
import com.salesforce.apollo.gorgoneion.comm.endorsement.EndorsementService;
import com.salesforce.apollo.gorgoneion.proto.*;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.ring.SliceIterator;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.InceptionEvent;
import com.salesforce.apollo.stereotomy.event.proto.Ident;
import com.salesforce.apollo.stereotomy.event.proto.KERL_;
import com.salesforce.apollo.stereotomy.event.proto.Validation_;
import com.salesforce.apollo.stereotomy.event.proto.Validations;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.services.proto.ProtoEventObserver;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Predicate;

import static com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory.digestOf;

/**
 * @author hal.hildebrand
 */
public class Gorgoneion {
    public static final Logger log = LoggerFactory.getLogger(Gorgoneion.class);

    @SuppressWarnings("unused")
    private final CommonCommunications<?, AdmissionsService>            admissionsComm;
    private final Context<Member>                                       context;
    private final CommonCommunications<Endorsement, EndorsementService> endorsementComm;
    private final ControlledIdentifierMember                            member;
    private final ProtoEventObserver                                    observer;
    private final Parameters                                            parameters;
    private final ScheduledExecutorService                              scheduler = Executors.newScheduledThreadPool(1,
                                                                                                                     Thread.ofVirtual()
                                                                                                                           .factory());
    private final Predicate<SignedAttestation>                          verifier;
    private final boolean                                               bootstrap;

    public Gorgoneion(boolean bootstrap, Predicate<SignedAttestation> verifier, Parameters parameters,
                      ControlledIdentifierMember member, Context<Member> context, ProtoEventObserver observer,
                      Router router, GorgoneionMetrics metrics) {
        this(bootstrap, verifier, parameters, member, context, observer, router, metrics, router);
    }

    public Gorgoneion(boolean bootstrap, Predicate<SignedAttestation> verifier, Parameters parameters,
                      ControlledIdentifierMember member, Context<Member> context, ProtoEventObserver observer,
                      Router admissionsRouter, GorgoneionMetrics metrics, Router endorsementRouter) {
        this.bootstrap = bootstrap;
        this.verifier = verifier;
        this.member = member;
        this.context = context;
        this.parameters = parameters;
        this.observer = observer;

        admissionsComm = admissionsRouter.create(member, context.getId(), new Admit(), ":admissions",
                                                 r -> new AdmissionsServer(admissionsRouter.getClientIdentityProvider(),
                                                                           r, metrics));

        final var service = new Endorse();
        endorsementComm = endorsementRouter.create(member, context.getId(), service, ":endorsement",
                                                   r -> new EndorsementServer(
                                                   admissionsRouter.getClientIdentityProvider(), r, metrics),
                                                   EndorsementClient.getCreate(metrics),
                                                   Endorsement.getLocalLoopback(member, service));
    }

    private boolean completeEndorsement(Optional<MemberSignature> futureSailor, Member from,
                                        Set<MemberSignature> validations) {
        if (futureSailor.isEmpty()) {
            return true;
        }
        var v = futureSailor;
        validations.add(v.get());
        return true;
    }

    private boolean completeEnrollment(Optional<Empty> futureSailor, Member m, HashSet<Member> completed) {
        if (futureSailor.isEmpty()) {
            return true;
        }
        futureSailor.get();
        completed.add(m);
        return true;
    }

    private boolean completeVerification(Optional<Validation_> futureSailor, Member m,
                                         HashSet<Validation_> verifications) {
        if (futureSailor.isEmpty()) {
            return true;
        }
        var v = futureSailor.get();
        verifications.add(v);
        return true;
    }

    private MemberSignature endorse(Nonce request) {
        return MemberSignature.newBuilder()
                              .setId(member.getId().toDigeste())
                              .setSignature(member.sign(request.toByteString()).toSig())
                              .build();
    }

    private void enroll(Notarization request) {
        observer.publish(request.getKerl(), Collections.singletonList(request.getValidations()));
    }

    private SignedNonce generateNonce(KERL_ application) {
        final var identifier = identifier(application);
        if (identifier == null) {
            throw new IllegalArgumentException("No identifier");
        }
        log.info("Generating nonce for: {} contacting: {} on: {}", identifier, identifier, member.getId());
        var now = parameters.clock().instant();
        final var ident = identifier.toIdent();
        var nonce = Nonce.newBuilder()
                         .setMember(ident)
                         .setIssuer(member.getId().toDigeste())
                         .setNoise(parameters.digestAlgorithm().random().toDigeste())
                         .setTimestamp(Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()))
                         .build();

        var successors = context.totalCount() == 1 ? Collections.singletonList(member)
                                                   : Context.uniqueSuccessors(context, digestOf(ident,
                                                                                                parameters.digestAlgorithm()));
        final var majority = context.majority(true);
        final var redirecting = new SliceIterator<>("Nonce Endorsement", member, successors, endorsementComm);
        Set<MemberSignature> endorsements = Collections.newSetFromMap(new ConcurrentHashMap<>());
        var generated = new CompletableFuture<SignedNonce>();
        redirecting.iterate((link, m) -> {
            log.info("Request signing nonce for: {} contacting: {} on: {}", identifier, link.getMember().getId(),
                     member.getId());
            return link.endorse(nonce, parameters.registrationTimeout());
        }, (futureSailor, link, m) -> completeEndorsement(futureSailor, m, endorsements), () -> {
            if (endorsements.size() < majority) {
                generated.completeExceptionally(new StatusRuntimeException(Status.ABORTED.withDescription(
                "Cannot gather required nonce endorsements: %s required: %s on: %s".formatted(endorsements.size(),
                                                                                              majority,
                                                                                              member.getId()))));
            } else {
                generated.complete(SignedNonce.newBuilder()
                                              .addSignatures(MemberSignature.newBuilder()
                                                                            .setId(member.getId().toDigeste())
                                                                            .setSignature(
                                                                            member.sign(nonce.toByteString()).toSig())
                                                                            .build())
                                              .setNonce(nonce)
                                              .addAllSignatures(endorsements)
                                              .build());
                log.info("Generated nonce for: {} signatures: {} on: {}", identifier, endorsements.size(),
                         member.getId());
            }
        }, scheduler, parameters.frequency());
        try {
            return generated.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    private Identifier identifier(KERL_ kerl) {
        if (ProtobufEventFactory.from(kerl.getEvents(kerl.getEventsCount() - 1))
                                .event() instanceof EstablishmentEvent establishment) {
            return establishment.getIdentifier();
        }
        return null;
    }

    private void notarize(Credentials credentials, Validations validations) {
        final var kerl = credentials.getAttestation().getAttestation().getKerl();
        final var identifier = identifier(kerl);
        if (identifier == null) {
            throw new IllegalArgumentException("No identifier");
        }

        var notarization = Notarization.newBuilder()
                                       .setKerl(credentials.getAttestation().getAttestation().getKerl())
                                       .setValidations(validations)
                                       .build();

        var successors = Context.uniqueSuccessors(context,
                                                  digestOf(identifier.toIdent(), parameters.digestAlgorithm()));
        final var majority = context.majority(true);
        SliceIterator<Endorsement> redirecting = new SliceIterator<>("Enrollment", member, successors, endorsementComm);
        var completed = new HashSet<Member>();
        redirecting.iterate((link, m) -> {
            log.info("Enrolling: {} contacting: {} on: {}", identifier, link.getMember().getId(), member.getId());
            link.enroll(notarization, parameters.registrationTimeout());
            return Empty.getDefaultInstance();
        }, (futureSailor, link, m) -> completeEnrollment(futureSailor, m, completed), () -> {
            if (completed.size() < majority) {
                throw new StatusRuntimeException(Status.ABORTED.withDescription("Cannot complete enrollment"));
            }
        }, scheduler, parameters.frequency());
    }

    private Validations register(Credentials request) {
        final var kerl = request.getAttestation().getAttestation().getKerl();
        final var identifier = identifier(kerl);
        if (identifier == null) {
            throw new IllegalArgumentException("No identifier");
        }
        log.debug("Validating credentials for: {} nonce signatures: {} on: {}", identifier,
                  request.getNonce().getSignaturesCount(), member.getId());

        var validated = new CompletableFuture<Validations>();

        var successors = Context.uniqueSuccessors(context,
                                                  digestOf(identifier.toIdent(), parameters.digestAlgorithm()));
        final var majority = context.majority(true);
        final var redirecting = new SliceIterator<>("Credential verification", member, successors, endorsementComm);
        var verifications = new HashSet<Validation_>();
        redirecting.iterate((link, m) -> {
            log.debug("Validating  credentials for: {} contacting: {} on: {}", identifier, link.getMember().getId(),
                      member.getId());
            return link.validate(request, parameters.registrationTimeout());
        }, (futureSailor, link, m) -> completeVerification(futureSailor, m, verifications), () -> {
            if (verifications.size() < majority) {
                throw new StatusRuntimeException(
                Status.ABORTED.withDescription("Cannot gather required credential validations"));
            } else {
                validated.complete(Validations.newBuilder()
                                              .setCoordinates(
                                              ProtobufEventFactory.from(kerl.getEvents(kerl.getEventsCount() - 1))
                                                                  .event()
                                                                  .getCoordinates()
                                                                  .toEventCoords())
                                              .addAllValidations(verifications)
                                              .build());
                log.debug("Validated credentials for: {} verifications: {} on: {}", identifier, verifications.size(),
                          member.getId());
            }
        }, scheduler, parameters.frequency());
        try {
            return validated.thenApply(v -> {
                notarize(request, v);
                return v;
            }).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private Validation_ validate(Credentials credentials) {
        var event = (InceptionEvent) ProtobufEventFactory.from(
        credentials.getAttestation().getAttestation().getKerl().getEvents(0)).event();
        log.info("Validating credentials for: {} on: {}", event.getIdentifier(), member.getId());
        Signer signer = member.getIdentifier().getSigner();
        return Validation_.newBuilder()
                          .setValidator(member.getIdentifier().getCoordinates().toEventCoords())
                          .setSignature(signer.sign(event.toKeyEvent_().toByteString()).toSig())
                          .build();
    }

    private Validation_ verificationOf(Credentials credentials) {
        if (verifier.test(credentials.getAttestation())) {
            return validate(credentials);
        }
        return null;
    }

    private class Admit implements AdmissionsService {

        @Override
        public void apply(KERL_ request, Digest from, StreamObserver<SignedNonce> responseObserver,
                          Timer.Context time) {
            if (!validate(request, from)) {
                log.warn("Invalid application from: {} on: {}", from, member.getId());
                responseObserver.onError(
                new StatusRuntimeException(Status.UNAUTHENTICATED.withDescription("Invalid application")));
                return;
            }
            SignedNonce sn = generateNonce(request);
            if (sn == null) {
                responseObserver.onError(
                new StatusRuntimeException(Status.UNAUTHENTICATED.withDescription("Invalid application")));
            } else {
                responseObserver.onNext(sn);
                responseObserver.onCompleted();
            }
        }

        @Override
        public void register(Credentials request, Digest from, StreamObserver<Validations> responseObserver,
                             Timer.Context timer) {
            if (!validate(request, from)) {
                log.warn("Invalid credentials from: {} on: {}", from, member.getId());
                responseObserver.onError(
                new StatusRuntimeException(Status.UNAUTHENTICATED.withDescription("Invalid credentials")));
                return;
            }
            try {
                Validations invite = Gorgoneion.this.register(request);
                if (invite == null) {
                    responseObserver.onError(
                    new StatusRuntimeException(Status.UNAUTHENTICATED.withDescription("Invalid credentials")));
                } else {
                    responseObserver.onNext(invite);
                    responseObserver.onCompleted();
                }
            } catch (StatusRuntimeException e) {
                responseObserver.onError(e);
            }
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
        public MemberSignature endorse(Nonce request, Digest from) {
            if (!validate(request, from)) {
                log.warn("Invalid endorsement nonce from: {} on: {}", from, member.getId());
                throw new StatusRuntimeException(Status.UNAUTHENTICATED.withDescription("Invalid endorsement nonce"));
            }
            log.info("Endorsing nonce for: {} from: {} on: {}", Identifier.from(request.getMember()), from,
                     member.getId());
            return Gorgoneion.this.endorse(request);
        }

        @Override
        public void enroll(Notarization request, Digest from) {
            var kerl = request.getKerl();
            var identifier = identifier(kerl);
            if (!validate(request, identifier, kerl, from)) {
                log.warn("Invalid notarization for: {} from: {} on: {}", identifier, from, member.getId());
                throw new StatusRuntimeException(Status.UNAUTHENTICATED.withDescription("Invalid notarization"));
            }
            log.info("Enrolling notorization for: {} from: {} on: {}", identifier, from, member.getId());
            Gorgoneion.this.enroll(request);
        }

        @Override
        public Validation_ validate(Credentials credentials, Digest from) {
            if (!validateCredentials(credentials, from)) {
                log.warn("Invalid credentials from: {} on: {}", from, member.getId());
                throw new StatusRuntimeException(Status.UNAUTHENTICATED.withDescription("Invalid credentials"));
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
                    if (new DefaultVerifier(
                    parameters.kerl().getKeyState(EventCoordinates.from(validation.getValidator())).getKeys()).verify(
                    JohnHancock.from(validation.getSignature()), establishment.toKeyEvent_().toByteString())) {
                        count++;
                    } else {
                        log.warn("Invalid notarization, invalid validation for: {} from: {} on: {}", identifier, from,
                                 member.getId());
                    }
                }
                // If there is only one active member in our context, it's us.
                var majority = context.majority(true);
                if (count < majority) {
                    log.warn("Invalid notarization, no majority: {} required: {} for: {} from: {} on: {}", count,
                             majority, identifier, from, member.getId());
                    return false;
                }
                return true;
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

            var majority = context.majority(true);
            if (count < majority) {
                log.warn("Invalid credential nonce, no majority signature: {} required >= {} from: {} on: {}", count,
                         majority, from, member.getId());
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
            if (now.isBefore(aInstant) || aInstant.plus(parameters.maxDuration()).isBefore(now) || aInstant.isBefore(
            nInstant)) {
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
}
