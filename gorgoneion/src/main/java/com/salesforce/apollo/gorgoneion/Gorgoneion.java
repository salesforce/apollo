/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion;

import java.time.Clock;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import com.salesfoce.apollo.gorgoneion.proto.Application;
import com.salesfoce.apollo.gorgoneion.proto.Credentials;
import com.salesfoce.apollo.gorgoneion.proto.EndorseNonce;
import com.salesfoce.apollo.gorgoneion.proto.Invitation;
import com.salesfoce.apollo.gorgoneion.proto.Nonce;
import com.salesfoce.apollo.gorgoneion.proto.Notarization;
import com.salesfoce.apollo.gorgoneion.proto.SignedAttestation;
import com.salesfoce.apollo.gorgoneion.proto.SignedNonce;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.stereotomy.event.proto.Validation_;
import com.salesfoce.apollo.stereotomy.event.proto.Validations;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.gorgoneion.comm.GorgoneionMetrics;
import com.salesforce.apollo.gorgoneion.comm.admissions.Admissions;
import com.salesforce.apollo.gorgoneion.comm.admissions.AdmissionsClient;
import com.salesforce.apollo.gorgoneion.comm.admissions.AdmissionsServer;
import com.salesforce.apollo.gorgoneion.comm.admissions.AdmissionsService;
import com.salesforce.apollo.gorgoneion.comm.gather.Endorsement;
import com.salesforce.apollo.gorgoneion.comm.gather.EndorsementClient;
import com.salesforce.apollo.gorgoneion.comm.gather.EndorsementServer;
import com.salesforce.apollo.gorgoneion.comm.gather.EndorsementService;
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
    private class Service implements AdmissionsService, EndorsementService {

        @Override
        public void apply(Application request, Digest from, StreamObserver<SignedNonce> responseObserver,
                          Timer.Context time) {
            if (!validate(request, from)) {
                log.warn("Invalid application from: {} on: {}", from, member.getId());
                responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED.withDescription("Invalid application")));
                return;
            }
            generateNonce(request).whenComplete((sn, t) -> {
                if (t != null) {
                    responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withCause(t)));
                } else if (sn == null) {
                    responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED.withDescription("Invalid application")));
                } else {
                    responseObserver.onNext(sn);
                    responseObserver.onCompleted();
                }
            });
        }

        @Override
        public void endorse(EndorseNonce request, Digest from, StreamObserver<Validation_> responseObserver,
                            Timer.Context timer) {
            if (!validate(request, from)) {
                log.warn("Invalid endorsement nonce from: {} on: {}", from, member.getId());
                responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED.withDescription("Invalid endorsement nonce")));
                return;
            }
            // TODO Auto-generated method stub

        }

        @Override
        public void enroll(Notarization request, Digest from, StreamObserver<Empty> responseObserver,
                           Timer.Context time) {
            if (!validate(request, from)) {
                log.warn("Invalid notarization from: {} on: {}", from, member.getId());
                responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED.withDescription("Invalid notarization")));
                return;
            }
        }

        @Override
        public void register(Credentials request, Digest from, StreamObserver<Invitation> responseObserver,
                             Timer.Context timer) {
            if (!validate(request, from)) {
                log.warn("Invalid credentials from: {} on: {}", from, member.getId());
                responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED.withDescription("Invalid credentials")));
                return;
            }
            verify(request).whenComplete((v, t) -> {
                if (t != null) {
                    responseObserver.onError(new StatusRuntimeException(Status.INTERNAL.withCause(t)));
                } else if (v == null) {
                    responseObserver.onError(new StatusRuntimeException(Status.UNAUTHENTICATED.withDescription("Invalid credentials")));
                } else {
                    responseObserver.onNext(Invitation.newBuilder()
                                                      .setValidations(Validations.newBuilder().addValidations(v))
                                                      .build());
                    responseObserver.onCompleted();
                }
            });
        }

    }

    private static final Logger log = LoggerFactory.getLogger(Gorgoneion.class);

    @SuppressWarnings("unused")
    private final CommonCommunications<Admissions, AdmissionsService>     admissionsComm;
    private final Clock                                                   clock;
    private final DigestAlgorithm                                         digestAlgo;
    @SuppressWarnings("unused")
    private final CommonCommunications<Endorsement, EndorsementService>   endorsementComm;
    private final ControlledIdentifierMember                              member;
    private final Function<SignedAttestation, CompletableFuture<Boolean>> verifier;

    public Gorgoneion(ControlledIdentifierMember member, Context<Member> context,
                      Function<SignedAttestation, CompletableFuture<Boolean>> verifier, Clock clock,
                      DigestAlgorithm digestAlgo, Router router, GorgoneionMetrics metrics, Executor exec) {
        this(member, context, verifier, clock, digestAlgo, router, metrics, router, exec);
    }

    public Gorgoneion(ControlledIdentifierMember member, Context<Member> context,
                      Function<SignedAttestation, CompletableFuture<Boolean>> verifier, Clock clock,
                      DigestAlgorithm digestAlgo, Router admissionsRouter, GorgoneionMetrics metrics,
                      Router endorsementRouter, Executor exec) {
        this.member = member;
        this.digestAlgo = digestAlgo;
        this.clock = clock;
        this.verifier = verifier;

        var service = new Service();

        admissionsComm = admissionsRouter.create(member, context.getId(), (AdmissionsService) service, ":admissions",
                                                 r -> new AdmissionsServer(admissionsRouter.getClientIdentityProvider(),
                                                                           r, exec, metrics),
                                                 AdmissionsClient.getCreate(metrics),
                                                 Admissions.getLocalLoopback(member));

        endorsementComm = endorsementRouter.create(member, context.getId(), (EndorsementService) service,
                                                   ":endorsement",
                                                   r -> new EndorsementServer(admissionsRouter.getClientIdentityProvider(),
                                                                              r, exec, metrics),
                                                   EndorsementClient.getCreate(metrics),
                                                   Endorsement.getLocalLoopback(member));
    }

    private CompletableFuture<SignedNonce> generateNonce(Application application) {
        var now = clock.instant();
        final var identifier = identifier(application);
        if (identifier == null) {
            var fs = new CompletableFuture<SignedNonce>();
            fs.completeExceptionally(new IllegalArgumentException("No identifier"));
            return fs;
        }
        var nonce = Nonce.newBuilder()
                         .setMember(identifier)
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

    private boolean validate(Application request, Digest from) {
        // TODO Auto-generated method stub
        return true;
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

    private boolean validate(EndorseNonce request, Digest from) {
        // TODO Auto-generated method stub
        return false;
    }

    private boolean validate(Notarization request, Digest from) {
        // TODO Auto-generated method stub
        return false;
    }

    private CompletableFuture<Validation_> verify(Credentials credentials) {
        return verifier.apply(credentials.getAttestation()).thenCompose(success -> {
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
