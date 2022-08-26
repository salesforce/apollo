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
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.thoth.proto.AdmissionsGossip;
import com.salesfoce.apollo.thoth.proto.AdmissionsUpdate;
import com.salesfoce.apollo.thoth.proto.Admittance;
import com.salesfoce.apollo.thoth.proto.Commit;
import com.salesfoce.apollo.thoth.proto.Nonce;
import com.salesfoce.apollo.thoth.proto.Pending;
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
import com.salesforce.apollo.stereotomy.KERL;
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

/**
 * Apollo attested admission service
 *
 * @author hal.hildebrand
 *
 */
public class Gorgoneion {

    private class Admissions implements Admission {
        @Override
        public SignedNonce apply(Registration request, Digest from) {
            return Gorgoneion.this.apply(request, from);
        }

        @Override
        public Admittance register(SignedAttestation request, Digest from) {
            return Gorgoneion.this.register(request, from);
        }
    }

    private class Replication implements AdmissionsReplication {
        @Override
        public void commit(Commit commit, Digest from) {
            Gorgoneion.this.commit(commit, from);
        }

        @Override
        public AdmissionsUpdate gossip(AdmissionsGossip gossip, Digest from) {
            return Gorgoneion.this.gossip(gossip, from);
        }

        @Override
        public void update(AdmissionsUpdate update, Digest from) {
            Gorgoneion.this.update(update, from);
        }
    }

    private record PendingAuth(Pending pending, Instant useBy) {}

    private static final Logger                                                            log         = LoggerFactory.getLogger(Gorgoneion.class);
    @SuppressWarnings("unused")
    private final CommonCommunications<AdmissionService, Admission>                        admissionComms;
    private final Admissions                                                               admissions  = new Admissions();
    private final Clock                                                                    clock;
    private final Context<Member>                                                          context;
    private final DigestAlgorithm                                                          digestAlgo;
    @SuppressWarnings("unused")
    private final SecureRandom                                                             entropy;
    private final Executor                                                                 exec;
    private final ConcurrentSkipListSet<Digest>                                            expunged    = new ConcurrentSkipListSet<>();
    private final double                                                                   fpr;
    private volatile ScheduledFuture<?>                                                    futureGossip;
    private final RingCommunications<Member, AdmissionReplicationService>                  gossiper;
    @SuppressWarnings("unused")
    private final KERL                                                                     kerl;
    private final ControlledIdentifierMember                                               member;
    private final ConcurrentNavigableMap<Digest, PendingAuth>                              pending     = new ConcurrentSkipListMap<>();
    private final AdmissionsReplication                                                    replication = new Replication();
    private final CommonCommunications<AdmissionReplicationService, AdmissionsReplication> replicationComms;
    private final RoundScheduler                                                           roundTimers;
    private final AtomicBoolean                                                            started     = new AtomicBoolean();

    @SuppressWarnings("unused")
    private final Predicate<SignedAttestation> verifier;

    public Gorgoneion(ControlledIdentifierMember member, Context<Member> context, Router admissionsRouter, KERL kerl,
                      Router replicationRouter, Executor executor, Clock clock, SecureRandom entropy,
                      DigestAlgorithm digestAlgo, double fpr, Predicate<SignedAttestation> verifier) {
        this.clock = clock;
        this.digestAlgo = digestAlgo;
        this.entropy = entropy;
        this.fpr = fpr;
        this.member = member;
        this.context = context;
        this.kerl = kerl;
        this.exec = executor;
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
        final var digest = JohnHancock.from(p.getPending().getSignature()).toDigest(digestAlgo);
        if (expunged.contains(digest)) {
            log.trace("Not adding expunged: {} on: {}", digest, member.getId());
            return;
        }
        final var duration = p.getPending().getNonce().getDuration();
        pending.putIfAbsent(digest,
                            new PendingAuth(p,
                                            clock.instant()
                                                 .plus(Duration.ofSeconds(duration.getSeconds(),
                                                                          duration.getNanos()))));
    }

    private SignedNonce apply(Registration request, Digest from) {
        // TODO Auto-generated method stub
        return null;
    }

    private void commit(Commit expunge, Digest from) {
        // TODO Auto-generated method stub
    }

    @SuppressWarnings("unused")
    private SignedNonce generateNonce() {
        var nonce = Nonce.newBuilder().build();
        return SignedNonce.newBuilder().setNonce(nonce).build();
    }

    private BloomFilter<Digest> getBff(long seed, double p) {
        var biff = new DigestBloomFilter(seed, Math.min(100, pending.size()), p);
        pending.keySet().forEach(e -> biff.add(e));
        return biff;
    }

    private ListenableFuture<AdmissionsUpdate> gossip(AdmissionReplicationService link, Integer ring) {
        if (!started.get()) {
            return null;
        }
        roundTimers.tick();
        return link.gossip(AdmissionsGossip.newBuilder()
                                           .setBff(getBff(Entropy.nextBitsStreamLong(), fpr).toBff())
                                           .build());
    }

    private AdmissionsUpdate gossip(AdmissionsGossip gossip, Digest from) {
        // TODO Auto-generated method stub
        return null;
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

    private void gossip(Optional<ListenableFuture<AdmissionsUpdate>> futureSailor,
                        Destination<Member, AdmissionReplicationService> destination, Duration frequency,
                        ScheduledExecutorService scheduler) {
        final var member = destination.member();
        try {
            if (futureSailor.isEmpty()) {
                return;
            }
            AdmissionsUpdate update;
            try {
                update = futureSailor.get().get();
                update.getUpdateList().forEach(sn -> add(sn));
            } catch (ExecutionException e) {
                log.trace("Error in gossip on: {}", member.getId());
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            final var bff = update.getBff();
            if (Biff.getDefaultInstance().equals(bff)) {
                return;
            }
            var response = updateFor(BloomFilter.from(bff));
            if (response == null) {
                return;
            }
        } finally {
            futureGossip = scheduler.schedule(() -> gossip(frequency, scheduler), frequency.toNanos(),
                                              TimeUnit.NANOSECONDS);
        }
    }

    private Admittance register(SignedAttestation request, Digest from) {
        // TODO Auto-generated method stub
        return null;
    }

    private void update(AdmissionsUpdate update, Digest from) {
        // TODO Auto-generated method stub

    }

    private AdmissionsUpdate updateFor(BloomFilter<Digest> have) {
        var update = AdmissionsUpdate.newBuilder();
        pending.entrySet()
               .stream()
               .filter(e -> !have.contains(e.getKey()))
               .forEach(e -> update.addUpdate(e.getValue().pending));
        return update.build();
    }
}
