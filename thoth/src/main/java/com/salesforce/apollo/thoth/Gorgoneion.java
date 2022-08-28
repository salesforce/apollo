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
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Timestamp;
import com.salesfoce.apollo.thoth.proto.AdminGossip;
import com.salesfoce.apollo.thoth.proto.AdminUpdate;
import com.salesfoce.apollo.thoth.proto.Admittance;
import com.salesfoce.apollo.thoth.proto.Commit;
import com.salesfoce.apollo.thoth.proto.Deny;
import com.salesfoce.apollo.thoth.proto.Have;
import com.salesfoce.apollo.thoth.proto.Nonce;
import com.salesfoce.apollo.thoth.proto.Pending;
import com.salesfoce.apollo.thoth.proto.Proposal;
import com.salesfoce.apollo.thoth.proto.Registration;
import com.salesfoce.apollo.thoth.proto.SignedAttestation;
import com.salesfoce.apollo.thoth.proto.SignedNonce;
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
            return Gorgoneion.this.generateNonce(request);
        }

        @Override
        public CompletableFuture<Admittance> register(SignedAttestation request, Digest from) {
            return Gorgoneion.this.register(request, from);
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

    private record PendingCommit(Commit commit, Set<Member> yes, Set<Member> no) {}

    private static final Logger log = LoggerFactory.getLogger(Gorgoneion.class);

    @SuppressWarnings("unused")
    private final CommonCommunications<AdmissionService, Admission>                        admissionComms;
    private final Admissions                                                               admissions     = new Admissions();
    private final Clock                                                                    clock;
    private final ConcurrentNavigableMap<Digest, Commit>                                   commits        = new ConcurrentSkipListMap<>();
    private final Context<Member>                                                          context;
    private final ConcurrentNavigableMap<Digest, Deny>                                     denies         = new ConcurrentSkipListMap<>();
    private final DigestAlgorithm                                                          digestAlgo;
    private final com.google.protobuf.Duration                                             duration;
    private final SecureRandom                                                             entropy;
    private final Executor                                                                 exec;
    private final double                                                                   fpr;
    private volatile ScheduledFuture<?>                                                    futureGossip;
    private final RingCommunications<Member, AdmissionReplicationService>                  gossiper;
    @SuppressWarnings("unused")
    private final KERL                                                                     kerl;
    @SuppressWarnings("unused")
    private final ControlledIdentifierMember                                               member;
    private final ConcurrentNavigableMap<Digest, Pending>                                  pending        = new ConcurrentSkipListMap<>();
    @SuppressWarnings("unused")
    private final ConcurrentNavigableMap<Digest, PendingCommit>                            pendingCommits = new ConcurrentSkipListMap<>();
    private final BloomWindow<Digest>                                                      processed;
    private final ConcurrentNavigableMap<Digest, Proposal>                                 proposals      = new ConcurrentSkipListMap<>();
    private final AdmissionsReplication                                                    replication    = new Replication();
    private final CommonCommunications<AdmissionReplicationService, AdmissionsReplication> replicationComms;
    private final RoundScheduler                                                           roundTimers;
    private final AtomicBoolean                                                            started        = new AtomicBoolean();
    private final ControlledIdentifier<SelfAddressingIdentifier>                           validating;
    @SuppressWarnings("unused")
    private final Predicate<SignedAttestation>                                             verifier;

    public Gorgoneion(ControlledIdentifierMember member, Context<Member> context, Router admissionsRouter, KERL kerl,
                      Router replicationRouter, Executor executor, Clock clock, SecureRandom entropy,
                      DigestAlgorithm digestAlgo, double fpr, Duration registrationTimeout,
                      Predicate<SignedAttestation> verifier) throws InterruptedException, ExecutionException {
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

    private void add(Commit c) {
        final var digest = validate(c);
        if (digest == null) {
            return;
        }
        if (processed.contains(digest)) {
            return;
        }
        commits.put(digest, c);
    }

    private void add(Deny c) {
        final var digest = validate(c);
        if (digest == null) {
            return;
        }
        if (processed.contains(digest)) {
            return;
        }
        denies.put(digest, c);
    }

    private void add(Pending p) {
        final var digest = validate(p);
        if (digest == null) {
            return;
        }
        if (processed.contains(digest)) {
            return;
        }
        pending.put(digest, p);
    }

    private void add(Proposal p) {
        final var digest = validate(p);
        if (digest == null) {
            return;
        }
        if (processed.contains(digest)) {
            return;
        }
        proposals.put(digest, p);
    }

    private CompletableFuture<SignedNonce> generateNonce(Registration registration) {
        if (!validate(registration)) {
            var fs = new CompletableFuture<SignedNonce>();
            fs.complete(SignedNonce.getDefaultInstance());
            return fs;
        }
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
            pending.computeIfAbsent(digest.get(),
                                    d -> Pending.newBuilder()
                                                .setPending(signed)
                                                .setKerl(registration.getKerl())
                                                .build());
            return signed;
        });
    }

    private Have getHave() {
        return Have.newBuilder()
                   .setCommits(haveCommits())
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

    private Biff haveCommits() {
        var bif = new DigestBloomFilter(Entropy.nextBitsStreamLong(), Math.min(100, commits.size()), fpr);
        commits.keySet().forEach(d -> bif.add(d));
        return bif.toBff();
    }

    private Biff haveDenies() {
        var bif = new DigestBloomFilter(Entropy.nextBitsStreamLong(), Math.min(100, commits.size()), fpr);
        denies.keySet().forEach(d -> bif.add(d));
        return bif.toBff();
    }

    private Biff havePending() {
        var bif = new DigestBloomFilter(Entropy.nextBitsStreamLong(), Math.min(100, commits.size()), fpr);
        pending.keySet().forEach(d -> bif.add(d));
        return bif.toBff();
    }

    private Biff haveProposals() {
        var bif = new DigestBloomFilter(Entropy.nextBitsStreamLong(), Math.min(100, commits.size()), fpr);
        proposals.keySet().forEach(d -> bif.add(d));
        return bif.toBff();
    }

    private void process(AdminUpdate adminUpdate) {
        adminUpdate.getCommitsList().forEach(c -> add(c));
        adminUpdate.getDenyList().forEach(d -> add(d));
        adminUpdate.getProposalsList().forEach(p -> add(p));
        adminUpdate.getPendingList().forEach(p -> add(p));
    }

    private CompletableFuture<Admittance> register(SignedAttestation request, Digest from) {
        // TODO Auto-generated method stub
        return null;
    }

    private List<Commit> updateCommits(Biff have) {
        BloomFilter<Digest> haves = BloomFilter.from(have);
        return commits.entrySet().stream().filter(e -> !haves.contains(e.getKey())).map(e -> e.getValue()).toList();
    }

    private List<Deny> updateDenies(Biff have) {
        BloomFilter<Digest> d = BloomFilter.from(have);
        return denies.entrySet().stream().filter(e -> !d.contains(e.getKey())).map(e -> e.getValue()).toList();
    }

    private AdminUpdate.Builder updateFor(final Have have) {
        return AdminUpdate.newBuilder()
                          .addAllCommits(updateCommits(have.getCommits()))
                          .addAllDeny(updateDenies(have.getDenies()))
                          .addAllPending(updatePending(have.getPending()))
                          .addAllProposals(updateProposals(have.getProposals()));
    }

    private List<Pending> updatePending(Biff have) {
        BloomFilter<Digest> p = BloomFilter.from(have);
        return pending.entrySet().stream().filter(e -> !p.contains(e.getKey())).map(e -> e.getValue()).toList();
    }

    private List<Proposal> updateProposals(Biff have) {
        BloomFilter<Digest> p = BloomFilter.from(have);
        return proposals.entrySet().stream().filter(e -> !p.contains(e.getKey())).map(e -> e.getValue()).toList();
    }

    private Digest validate(Commit c) {
        return JohnHancock.from(c.getSignature()).toDigest(digestAlgo);
    }

    private Digest validate(Deny c) {
        return JohnHancock.from(c.getSignature()).toDigest(digestAlgo);
    }

    private Digest validate(Pending c) {
        return JohnHancock.from(c.getPending().getSignature()).toDigest(digestAlgo);
    }

    private Digest validate(Proposal c) {
        return JohnHancock.from(c.getSignature()).toDigest(digestAlgo);
    }

    private boolean validate(Registration registration) {
        return true;
    }
}
