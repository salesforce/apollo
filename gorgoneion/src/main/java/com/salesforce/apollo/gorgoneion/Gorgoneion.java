/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion;

import static com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory.digestOf;

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
import com.salesfoce.apollo.gorgoneion.proto.Application;
import com.salesfoce.apollo.gorgoneion.proto.Credentials;
import com.salesfoce.apollo.gorgoneion.proto.EndorseNonce;
import com.salesfoce.apollo.gorgoneion.proto.Invitation;
import com.salesfoce.apollo.gorgoneion.proto.Nonce;
import com.salesfoce.apollo.gorgoneion.proto.Notarization;
import com.salesfoce.apollo.gorgoneion.proto.SignedNonce;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.stereotomy.event.proto.Validation_;
import com.salesfoce.apollo.stereotomy.event.proto.Validations;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.comm.SliceIterator;
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
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

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
            if (!Gorgoneion.this.validate(request, from)) {
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
        public CompletableFuture<Validation_> endorse(EndorseNonce request, Digest from) {
            if (!Gorgoneion.this.validate(request, from)) {
                log.warn("Invalid endorsement nonce from: {} on: {}", from, member.getId());
                var fs = new CompletableFuture<Validation_>();
                fs.completeExceptionally(new StatusRuntimeException(Status.UNAUTHENTICATED.withDescription("Invalid endorsement nonce")));
                return fs;
            }
            return Gorgoneion.this.endorse(request);
        }

        @Override
        public CompletableFuture<Empty> enroll(Notarization request, Digest from) {
            if (!Gorgoneion.this.validate(request, from)) {
                log.warn("Invalid notarization from: {} on: {}", from, member.getId());
                var fs = new CompletableFuture<Empty>();
                fs.completeExceptionally(new StatusRuntimeException(Status.UNAUTHENTICATED.withDescription("Invalid notarization")));
                return fs;
            }
            return Gorgoneion.this.enroll(request);
        }

        @Override
        public void register(Credentials request, Digest from, StreamObserver<Invitation> responseObserver,
                             Timer.Context timer) {
            if (!Gorgoneion.this.validate(request, from)) {
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

        @Override
        public CompletableFuture<Validation_> validate(Credentials credentials, Digest id) {
            // TODO Auto-generated method stub
            return null;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(Gorgoneion.class);

    @SuppressWarnings("unused")
    private final CommonCommunications<Admissions, AdmissionsService>   admissionsComm;
    private final Context<Member>                                       context;
    private final CommonCommunications<Endorsement, EndorsementService> endorsementComm;
    private final Executor                                              exec;
    private final ControlledIdentifierMember                            member;
    private final Parameters                                            parameters;
    private final ScheduledExecutorService                              scheduler;

    public Gorgoneion(Parameters parameters, ControlledIdentifierMember member, Context<Member> context, Router router,
                      ScheduledExecutorService scheduler, GorgoneionMetrics metrics, Executor exec) {
        this(parameters, member, context, router, scheduler, metrics, router, exec);
    }

    public Gorgoneion(Parameters parameters, ControlledIdentifierMember member, Context<Member> context,
                      Router admissionsRouter, ScheduledExecutorService scheduler, GorgoneionMetrics metrics,
                      Router endorsementRouter, Executor exec) {
        this.member = member;
        this.context = context;
        this.exec = exec;
        this.parameters = parameters;
        this.scheduler = scheduler;

        var service = new Service();

        admissionsComm = admissionsRouter.create(member, context.getId(), service, ":admissions",
                                                 r -> new AdmissionsServer(admissionsRouter.getClientIdentityProvider(),
                                                                           r, exec, metrics),
                                                 AdmissionsClient.getCreate(metrics),
                                                 Admissions.getLocalLoopback(member));

        endorsementComm = endorsementRouter.create(member, context.getId(), service, ":endorsement",
                                                   r -> new EndorsementServer(admissionsRouter.getClientIdentityProvider(),
                                                                              r, exec, metrics),
                                                   EndorsementClient.getCreate(metrics),
                                                   Endorsement.getLocalLoopback(member, service));
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

    private CompletableFuture<Validation_> endorse(EndorseNonce request) {
        var fs = new CompletableFuture<Validation_>();
        fs.complete(Validation_.newBuilder()
                               .setValidator(member.getIdentifier().getCoordinates().toEventCoords())
                               .setSignature(member.sign(request.toByteString()).toSig())
                               .build());
        return fs;
    }

    private CompletableFuture<Empty> enroll(Notarization request) {
        // TODO Auto-generated method stub
        return null;
    }

    private CompletableFuture<SignedNonce> generateNonce(Application application) {
        var generated = new CompletableFuture<SignedNonce>();
        final var identifier = identifier(application);
        if (identifier == null) {
            var fs = new CompletableFuture<SignedNonce>();
            fs.completeExceptionally(new IllegalArgumentException("No identifier"));
            return fs;
        }
        var now = parameters.clock().instant();
        var nonce = Nonce.newBuilder()
                         .setMember(identifier)
                         .setIssuer(member.getIdentifier().getLastEstablishmentEvent().toEventCoords())
                         .setNoise(parameters.digestAlgorithm().random().toDigeste())
                         .setTimestamp(Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()))
                         .build();
        var endorse = EndorseNonce.newBuilder().build();

        var successors = context.successors(digestOf(identifier, parameters.digestAlgorithm()));
        final var majority = context.activeCount() == 1 ? 0 : context.majority();
        final var redirecting = new SliceIterator<>("Nonce Endorsement", member, successors, endorsementComm, exec);
        var validations = new HashSet<Validation_>();
        redirecting.iterate((link, m) -> {
            log.debug("Joining: {} contacting: {} on: {}", Identifier.from(identifier), link.getMember().getId(),
                      member.getId());
            return link.endorse(endorse, null);
        }, (futureSailor, link, m) -> completeValidation(futureSailor, m, validations), () -> {
            if (validations.size() <= majority) {
                generated.completeExceptionally(new StatusRuntimeException(Status.ABORTED.withDescription("Cannot gather required nonce endorsements")));
            } else {
                generated.complete(SignedNonce.newBuilder().setNonce(nonce).addAllSignatures(validations).build());
            }
        }, scheduler, parameters.frequency());
        return generated;
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
        return true;
    }

    private boolean validate(Notarization request, Digest from) {
        // TODO Auto-generated method stub
        return true;
    }

    private CompletableFuture<Validation_> verify(Credentials credentials) {
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
