/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth;

import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Timestamp;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.thoth.proto.AdminGossip;
import com.salesfoce.apollo.thoth.proto.AdminUpdate;
import com.salesfoce.apollo.thoth.proto.Admittance;
import com.salesfoce.apollo.thoth.proto.Deny;
import com.salesfoce.apollo.thoth.proto.Endorsement;
import com.salesfoce.apollo.thoth.proto.Have;
import com.salesfoce.apollo.thoth.proto.Nonce;
import com.salesfoce.apollo.thoth.proto.Pending;
import com.salesfoce.apollo.thoth.proto.Proposal;
import com.salesfoce.apollo.thoth.proto.Registration;
import com.salesfoce.apollo.thoth.proto.SignedAttestation;
import com.salesfoce.apollo.thoth.proto.SignedDeny;
import com.salesfoce.apollo.thoth.proto.SignedEndorsement;
import com.salesfoce.apollo.thoth.proto.SignedNonce;
import com.salesfoce.apollo.thoth.proto.SignedProposal;
import com.salesfoce.apollo.thoth.proto.Validation;
import com.salesfoce.apollo.utils.proto.Biff;
import com.salesforce.apollo.comm.RingCommunications;
import com.salesforce.apollo.comm.RingCommunications.Destination;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;
import com.salesforce.apollo.thoth.grpc.admission.Admission;
import com.salesforce.apollo.thoth.grpc.admission.AdmissionClient;
import com.salesforce.apollo.thoth.grpc.admission.AdmissionServer;
import com.salesforce.apollo.thoth.grpc.admission.AdmissionService;
import com.salesforce.apollo.thoth.grpc.admission.gossip.AdmissionReplicationService;
import com.salesforce.apollo.thoth.grpc.admission.gossip.AdmissionsReplication;
import com.salesforce.apollo.thoth.grpc.admission.gossip.AdmissionsReplicationClient;
import com.salesforce.apollo.thoth.grpc.admission.gossip.AdmissionsReplicationServer;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.RoundScheduler;
import com.salesforce.apollo.utils.Utils;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomWindow;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * Apollo attested admission service
 *
 * @author hal.hildebrand
 *
 */
public class Gorgoneion {
    private class Admissions implements Admission {
        @Override
        public CompletableFuture<SignedNonce> apply(Registration request, Digest from) {
            if (!validate(request, from)) {
                var fs = new CompletableFuture<SignedNonce>();
                fs.complete(SignedNonce.getDefaultInstance());
                return fs;
            }
            return Gorgoneion.this.generateNonce(request);
        }

        @Override
        public void register(SignedAttestation request, Digest from, StreamObserver<Admittance> observer) {
            if (!validate(request, from)) {
                observer.onError(new StatusRuntimeException(io.grpc.Status.UNAUTHENTICATED));
                return;
            }
            Gorgoneion.this.register(request, from, observer);
        }
    }

    private class Replication implements AdmissionsReplication {

        @Override
        public AdminUpdate gossip(AdminGossip gossip, Digest from) {
            return updateFor(gossip.getHave()).setHave(Gorgoneion.this.getHave()).build();
        }

        @Override
        public void update(AdminUpdate update, Digest from) {
            update(update, from);
        }
    }

    private record Votes(Pending pending, Set<SignedEndorsement> endorsements, Set<SignedDeny> deny) {}

    private static final Logger log = LoggerFactory.getLogger(Gorgoneion.class);

    @SuppressWarnings("unused")
    private final CommonCommunications<AdmissionService, Admission> admissionComms;

    private final Admissions                                                               admissions     = new Admissions();
    private final Clock                                                                    clock;
    private final Context<Member>                                                          context;
    /** Denials key is signature hash of the denial */
    private final ConcurrentNavigableMap<Digest, SignedDeny>                               denies         = new ConcurrentSkipListMap<>();
    private final DigestAlgorithm                                                          digestAlgo;
    private final com.google.protobuf.Duration                                             duration;
    /** Endorsements key is signature hash of the endorsement */
    private final ConcurrentNavigableMap<Digest, SignedEndorsement>                        endorsements   = new ConcurrentSkipListMap<>();
    private final SecureRandom                                                             entropy;
    private final Executor                                                                 exec;
    private final double                                                                   fpr;
    private volatile ScheduledFuture<?>                                                    futureGossip;
    private final RingCommunications<Member, AdmissionReplicationService>                  gossiper;
    @SuppressWarnings("unused")
    private final KERL                                                                     kerl;
    private final ControlledIdentifierMember                                               member;
    /** Pending key is member id, so only one pending per joining member */
    private final ConcurrentNavigableMap<Digest, Pending>                                  pending        = new ConcurrentSkipListMap<>();
    private final ConcurrentNavigableMap<Digest, Consumer<List<Validation>>>               pendingClients = new ConcurrentSkipListMap<>();
    private final BloomWindow<Digest>                                                      processed;
    /** Proposal key is member id, so only one proposal per joining member */
    private final ConcurrentNavigableMap<Digest, SignedProposal>                           proposals      = new ConcurrentSkipListMap<>();
    private final AdmissionsReplication                                                    replication    = new Replication();
    private final CommonCommunications<AdmissionReplicationService, AdmissionsReplication> replicationComms;
    private final RoundScheduler                                                           roundTimers;
    private final AtomicBoolean                                                            started        = new AtomicBoolean();
    private final ControlledIdentifier<SelfAddressingIdentifier>                           validating;
    private final Function<SignedAttestation, CompletableFuture<Boolean>>                  verifier;
    @SuppressWarnings("unused")
    private final ConcurrentNavigableMap<Digest, Votes>                                    votes          = new ConcurrentSkipListMap<>();

    public Gorgoneion(ControlledIdentifierMember member, Context<Member> context, Router admissionsRouter, KERL kerl,
                      Router replicationRouter, Executor executor, Clock clock, SecureRandom entropy,
                      DigestAlgorithm digestAlgo, double fpr, Duration registrationTimeout,
                      Function<SignedAttestation, CompletableFuture<Boolean>> verifier) throws InterruptedException,
                                                                                        ExecutionException {
        this.clock = clock;
        this.digestAlgo = digestAlgo;
        this.entropy = entropy;
        this.fpr = fpr;
        this.member = member;
        this.context = context;
        this.kerl = kerl;
        this.exec = executor;
        duration = com.google.protobuf.Duration.newBuilder()
                                               .setSeconds(registrationTimeout.toSecondsPart())
                                               .setNanos(registrationTimeout.toNanosPart())
                                               .build();
        replicationComms = replicationRouter.create(member, context.getId(), replication, "replication",
                                                    r -> new AdmissionsReplicationServer(r,
                                                                                         replicationRouter.getClientIdentityProvider(),
                                                                                         executor, null),
                                                    AdmissionsReplicationClient.getCreate(context.getId(), null),
                                                    AdmissionsReplicationClient.getLocalLoopback(replication, member));
        admissionComms = replicationRouter.create(member, context.getId(), admissions, "admissions",
                                                  r -> new AdmissionServer(r,
                                                                           admissionsRouter.getClientIdentityProvider(),
                                                                           executor, null),
                                                  AdmissionClient.getCreate(context.getId(), null),
                                                  AdmissionClient.getLocalLoopback(admissions, member));
        gossiper = new RingCommunications<>(context, member, replicationComms, executor);
        roundTimers = new RoundScheduler("replications", context.timeToLive());
        this.verifier = verifier;
        validating = member.getIdentifier()
                           .newIdentifier(IdentifierSpecification.<SelfAddressingIdentifier>newBuilder())
                           .get();
        final var maxTracked = 1000;
        processed = new BloomWindow<>(maxTracked,
                                      () -> new DigestBloomFilter(Entropy.nextBitsStreamLong(), maxTracked, 0.000125),
                                      3);
    }

    public void start(Duration frequency, ScheduledExecutorService scheduler) {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        gossip(frequency, scheduler);
    }

    public void stop() {
        if (!started.compareAndExchange(true, false)) {
            return;
        }
        final var current = futureGossip;
        futureGossip = null;
        if (current != null) {
            futureGossip.cancel(true);
        }
    }

    private void add(Pending p) {
        final var digest = validate(p);
        if (digest == null) {
            return;
        }
        pending.putIfAbsent(digest, p);
    }

    private void add(SignedDeny c) {
        final var digest = validate(c);
        if (digest == null) {
            return;
        }
        denies.putIfAbsent(digest, c);
    }

    private void add(SignedEndorsement c) {
        final var digest = validate(c);
        if (digest == null) {
            return;
        }
        endorsements.putIfAbsent(digest, c);
    }

    private void add(SignedProposal p) {
        final var digest = validate(p);
        if (digest == null) {
            return;
        }
        proposals.putIfAbsent(digest, p);
    }

    private Validation endorse(Pending p) {
        var event = p.getKerl().getEvents(0).getInception();
        return Validation.newBuilder()
                         .setIdentifier(validating.getIdentifier().toIdent())
                         .setSignature(member.sign(event.toByteString()).toSig())
                         .build();
    }

    private Endorsement endorse(SignedProposal sp) {
        var forMember = memberId(sp.getProposal().getAttestation().getAttestation().getMember());
        var pend = pending.get(forMember);
        if (pend == null) {
            return Endorsement.getDefaultInstance();
        }
        return Endorsement.newBuilder()
                          .setEndorser(member.getId().toDigeste())
                          .setNonce(pend.getPending())
                          .setValidation(endorse(pend))
                          .build();
    }

    private CompletableFuture<SignedNonce> generateNonce(Registration registration) {
        var noise = new byte[digestAlgo.longLength()];
        entropy.nextBytes(noise);
        var now = clock.instant();
        var nonce = Nonce.newBuilder()
                         .setMember(registration.getIdentity())
                         .setDuration(duration)
                         .setIssuer(validating.getLastEstablishmentEvent().toEventCoords())
                         .setNoise(digestAlgo.random(entropy).toDigeste())
                         .setTimestamp(Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()))
                         .build();
        final var digest = new AtomicReference<Digest>();
        return validating.getSigner().thenApply(s -> {
            final var signature = s.sign(nonce.toByteString());
            digest.set(signature.toDigest(digestAlgo));
            return SignedNonce.newBuilder().setNonce(nonce).setSignature(signature.toSig()).build();
        }).thenApply(signed -> {
            pending.put(digest.get(), Pending.newBuilder().setPending(signed).setKerl(registration.getKerl()).build());
            return signed;
        });
    }

    private Have getHave() {
        return Have.newBuilder()
                   .setEndorsements(haveEndorsements())
                   .setDenies(haveDenies())
                   .setPending(havePending())
                   .setProposals(haveProposals())
                   .build();
    }

    private ListenableFuture<AdminUpdate> gossip(AdmissionReplicationService link, Integer ring) {
        if (!started.get()) {
            return null;
        }
        roundTimers.tick();
        return link.gossip(AdminGossip.newBuilder().setHave(getHave()).build());
    }

    private void gossip(Duration frequency, ScheduledExecutorService scheduler) {
        if (!started.get()) {
            return;
        }
        exec.execute(Utils.wrapped(() -> {
            if (context.activeCount() == 1) {
                roundTimers.tick();
            }
            gossiper.execute((link, ring) -> gossip(link, ring),
                             (futureSailor, destination) -> gossip(futureSailor, destination, frequency, scheduler));
        }, log));
    }

    private void gossip(Optional<ListenableFuture<AdminUpdate>> futureSailor,
                        Destination<Member, AdmissionReplicationService> destination, Duration frequency,
                        ScheduledExecutorService scheduler) {
        final var member = destination.member();
        try {
            if (futureSailor.isEmpty()) {
                return;
            }
            try {
                final var adminUpdate = futureSailor.get().get();
                process(adminUpdate);
                destination.link().update(updateFor(adminUpdate.getHave()).build());
            } catch (ExecutionException e) {
                log.trace("Error in gossip on: {}", member.getId(), e);
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        } catch (Throwable e) {
            log.warn("Error in gossip on: {}", member.getId(), e);
            return;
        } finally {
            if (!started.get()) {
                return;
            }
            futureGossip = scheduler.schedule(() -> gossip(frequency, scheduler), frequency.toNanos(),
                                              TimeUnit.NANOSECONDS);
        }
    }

    private Biff haveDenies() {
        var bif = new DigestBloomFilter(Entropy.nextBitsStreamLong(), Math.min(100, denies.size()), fpr);
        denies.keySet().forEach(d -> bif.add(d));
        return bif.toBff();
    }

    private Biff haveEndorsements() {
        var bif = new DigestBloomFilter(Entropy.nextBitsStreamLong(), Math.min(100, endorsements.size()), fpr);
        endorsements.keySet().forEach(d -> bif.add(d));
        return bif.toBff();
    }

    private Biff havePending() {
        var bif = new DigestBloomFilter(Entropy.nextBitsStreamLong(), Math.min(100, pending.size()), fpr);
        pending.keySet().forEach(d -> bif.add(d));
        return bif.toBff();
    }

    private Biff haveProposals() {
        var bif = new DigestBloomFilter(Entropy.nextBitsStreamLong(), Math.min(100, proposals.size()), fpr);
        proposals.keySet().forEach(d -> bif.add(d));
        return bif.toBff();
    }

    @SuppressWarnings("unused")
    private void maybeEndorse(SignedProposal p) {
        verifier.apply(p.getProposal().getAttestation()).whenComplete((success, t) -> {
            if (t != null || !success) {
                var deny = Deny.newBuilder().build();
                add(SignedDeny.newBuilder()
                              .setDenial(deny)
                              .setSignature(member.sign(deny.toByteString()).toSig())
                              .build());
            } else {
                Endorsement endorsement = endorse(p);
                add(SignedEndorsement.newBuilder()
                                     .setEndorsement(endorsement)
                                     .setSignature(member.sign(endorsement.toByteString()).toSig())
                                     .build());
            }
        });
    }

    private Digest memberId(Ident identifier) {
        return ((SelfAddressingIdentifier) Identifier.from(identifier)).getDigest();
    }

    private void process(AdminUpdate adminUpdate) {
        adminUpdate.getEndorsementsList().forEach(c -> add(c));
        adminUpdate.getDenyList().forEach(d -> add(d));
        adminUpdate.getProposalsList().forEach(p -> add(p));
        adminUpdate.getPendingList().forEach(p -> add(p));
    }

    private void register(SignedAttestation request, Digest from, StreamObserver<Admittance> observer) {
        var proposal = Proposal.newBuilder().setAttestation(request).setProposer(member.getId().toDigeste()).build();
        var signed = SignedProposal.newBuilder()
                                   .setProposal(proposal)
                                   .setSignature(member.sign(proposal.toByteString()).toSig())
                                   .build();
        pendingClients.putIfAbsent(from, validations -> {
            observer.onNext(Admittance.newBuilder().addAllValidations(validations).build());
            observer.onCompleted();
        });
        proposals.put(from, signed);
    }

    private List<SignedDeny> updateDenies(Biff have) {
        BloomFilter<Digest> d = BloomFilter.from(have);
        return denies.entrySet().stream().filter(e -> !d.contains(e.getKey())).map(e -> e.getValue()).toList();
    }

    private List<SignedEndorsement> updateEndorsements(Biff have) {
        BloomFilter<Digest> haves = BloomFilter.from(have);
        return endorsements.entrySet()
                           .stream()
                           .filter(e -> !haves.contains(e.getKey()))
                           .map(e -> e.getValue())
                           .toList();
    }

    private AdminUpdate.Builder updateFor(final Have have) {
        return AdminUpdate.newBuilder()
                          .addAllEndorsements(updateEndorsements(have.getEndorsements()))
                          .addAllDeny(updateDenies(have.getDenies()))
                          .addAllPending(updatePending(have.getPending()))
                          .addAllProposals(updateProposals(have.getProposals()));
    }

    private List<Pending> updatePending(Biff have) {
        BloomFilter<Digest> p = BloomFilter.from(have);
        return pending.entrySet().stream().filter(e -> !p.contains(e.getKey())).map(e -> e.getValue()).toList();
    }

    private List<SignedProposal> updateProposals(Biff have) {
        BloomFilter<Digest> p = BloomFilter.from(have);
        return proposals.entrySet().stream().filter(e -> !p.contains(e.getKey())).map(e -> e.getValue()).toList();
    }

    private Digest validate(Pending c) {
        final var memberId = memberId(c.getPending().getNonce().getMember());
        if (processed.contains(memberId)) {
            return null;
        }
        final var member = context.getActiveMember(memberId);
        if (member == null) {
            return null;
        }
        return memberId;
    }

    private boolean validate(Registration registration, Digest from) {
        return true;
    }

    private boolean validate(SignedAttestation request, Digest from) {
        return true;
    }

    private Digest validate(SignedDeny c) {
        final var memberId = memberId(c.getDenial().getNonce().getNonce().getMember());
        if (processed.contains(memberId)) {
            return null;
        }
        final var member = context.getActiveMember(memberId);
        if (member == null) {
            return null;
        }
        final var signature = JohnHancock.from(c.getSignature());
        if (!member.verify(signature, c.getDenial().toByteString())) {
            return null;
        }
        return signature.toDigest(digestAlgo);
    }

    private Digest validate(SignedEndorsement c) {
        final var memberId = memberId(c.getEndorsement().getNonce().getNonce().getMember());
        if (processed.contains(memberId)) {
            return null;
        }
        final var member = context.getActiveMember(memberId);
        if (member == null) {
            return null;
        }
        final var signature = JohnHancock.from(c.getSignature());
        if (!member.verify(signature, c.getEndorsement().toByteString())) {
            return null;
        }
        return signature.toDigest(digestAlgo);
    }

    private Digest validate(SignedProposal c) {
        final var memberId = memberId(c.getProposal().getNonce().getNonce().getMember());
        if (processed.contains(memberId)) {
            return null;
        }
        final var member = context.getActiveMember(memberId);
        if (member == null) {
            return null;
        }
        if (!member.verify(JohnHancock.from(c.getSignature()), c.getProposal().toByteString())) {
            return null;
        }
        return memberId;
    }
}
