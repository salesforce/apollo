/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.memberships;

import static com.salesforce.apollo.ethereal.memberships.comm.GossiperClient.getCreate;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.ethereal.proto.ContextUpdate;
import com.salesfoce.apollo.ethereal.proto.Gossip;
import com.salesfoce.apollo.ethereal.proto.Have;
import com.salesfoce.apollo.ethereal.proto.SignedCommit;
import com.salesfoce.apollo.ethereal.proto.SignedPreVote;
import com.salesfoce.apollo.ethereal.proto.Update;
import com.salesfoce.apollo.ethereal.proto.Update.Builder;
import com.salesfoce.apollo.utils.proto.Biff;
import com.salesforce.apollo.comm.RingCommunications;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.memberships.RbcAdder.ChRbc;
import com.salesforce.apollo.ethereal.memberships.comm.EtherealMetrics;
import com.salesforce.apollo.ethereal.memberships.comm.Gossiper;
import com.salesforce.apollo.ethereal.memberships.comm.GossiperServer;
import com.salesforce.apollo.ethereal.memberships.comm.GossiperService;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.utils.Utils;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Handles the gossip propigation of proposals, commits and preVotes from this
 * node, as well as the notification of the adder of such from other nodes.
 * 
 * @author hal.hildebrand
 *
 */
public class RbcGossiper implements ChRbc {

    /**
     * The Service implementing the 3 phase gossip
     *
     */
    private class Terminal implements GossiperService {
        @Override
        public Update gossip(Gossip request, Digest from) {
            Member predecessor = context.ring(request.getRing()).predecessor(member);
            if (predecessor == null || !from.equals(predecessor.getId())) {
                log.debug("Invalid inbound gossip on {}:{} from: {} on ring: {} - not predecessor: {}", context.getId(),
                          member, from, request.getRing(), predecessor);
                return Update.getDefaultInstance();
            }
            final var update = RbcGossiper.this.gossip(request.getHaves());
            log.trace("GossipService received from: {} missing: {} on: {}", from, update.getMissingCount(), member);
            return update;
        }

        @Override
        public void update(ContextUpdate request, Digest from) {
            Member predecessor = context.ring(request.getRing()).predecessor(member);
            if (predecessor == null || !from.equals(predecessor.getId())) {
                log.debug("Invalid inbound update on {}:{} from: {} on ring: {} - not predecessor: {}", context.getId(),
                          member, from, request.getRing(), predecessor);
                return;
            }
            log.debug("gossip update with {} on: {}", from, member);
            write(() -> RbcGossiper.this.updateFrom(request.getUpdate()));
        }
    }

    private static final Logger log = LoggerFactory.getLogger(RbcGossiper.class);

    private final RbcAdder                                        adder;
    private final CommonCommunications<Gossiper, GossiperService> comm;
    private final Map<Digest, SignedCommit>                       commits  = new HashMap<>();
    private final Context<Member>                                 context;
    private final SigningMember                                   member;
    private final EtherealMetrics                                 metrics;
    private final ReentrantReadWriteLock                          mx       = new ReentrantReadWriteLock();
    private final Map<Digest, SignedPreVote>                      preVotes = new HashMap<>();
    private final RingCommunications<Gossiper>                    ring;
    private volatile ScheduledFuture<?>                           scheduled;
    private final AtomicBoolean                                   started  = new AtomicBoolean();

    public RbcGossiper(Context<Member> context, SigningMember member, RbcAdder adder, Router communications,
                       Executor exec, EtherealMetrics m) {
        this.adder = adder;
        this.context = context;
        this.member = member;
        this.metrics = m;
        comm = communications.create((Member) member, context.getId(), new Terminal(),
                                     r -> new GossiperServer(communications.getClientIdentityProvider(), metrics, r),
                                     getCreate(metrics), Gossiper.getLocalLoopback(member));
        ring = new RingCommunications<Gossiper>(context, member, this.comm, exec);
    }

    @Override
    public void commit(SignedCommit sc) {
        Digest digest = null;
        commits.put(digest, sc);
    }

    @Override
    public void prevote(SignedPreVote spv) {
        Digest digest = null;
        preVotes.put(digest, spv);
    }

    /**
     * Start the receiver's gossip
     */
    public void start(Duration duration, ScheduledExecutorService scheduler) {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        Duration initialDelay = duration.plusMillis(Utils.bitStreamEntropy().nextLong(2 * duration.toMillis()));
        log.trace("Starting GossipService[{}] on: {}", context.getId(), member);
        comm.register(context.getId(), new Terminal());
        scheduler.schedule(() -> {
            try {
                oneRound(duration, scheduler);
            } catch (Throwable e) {
                log.error("Error in gossip on: {}", member, e);
            }
        }, initialDelay.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Stop the receiver's gossip
     */
    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        log.trace("Stopping GossipService [{}] for {}", context.getId(), member);
        comm.deregister(context.getId());
        final var current = scheduled;
        scheduled = null;
        if (current != null) {
            current.cancel(true);
        }
    }

    private SignedCommit committed(SignedCommit c) {
        if (!validate(c)) {
            return null;
        }
        final var commit = c.getCommit();
        adder.commit(Digest.from(commit.getHash()), (short) commit.getSource(), this);
        return c;
    }

    private Config config() {
        return adder.config();
    }

    private Gossip gossip() {
        return read(() -> Gossip.newBuilder().setContext(context.getId().toDigeste()).setHaves(have()).build());
    }

    /**
     * Answer the Update from the receiver's state, based on the suppled Have
     */
    private Update gossip(Have have) {
        final var builder = Update.newBuilder();
        read(() -> {
            builder.setHaves(have());
            update(have, builder);
        });
        return builder.build();
    }

    /**
     * Perform the first phase of the gossip. Send our partner the Have state of the
     * receiver
     */
    private ListenableFuture<Update> gossipRound(Gossiper link, int ring) {
        if (!started.get()) {
            return null;
        }
        log.debug("gossiping[{}] with {} ring: {} on {}", context.getId(), link.getMember(), ring, member);
        try {
            return link.gossip(gossip());
        } catch (StatusRuntimeException e) {
            log.debug("gossiping[{}] failed with: {} with {} ring: {} on {}", context.getId(), e.getMessage(), member,
                      ring, link.getMember(), e);
            return null;
        } catch (Throwable e) {
            log.warn("gossiping[{}] failed from {} with {} ring: {} on {}", context.getId(), member, ring,
                     link.getMember(), e);
            return null;
        }
    }

    /**
     * The second phase of the gossip. Handle the update from our gossip partner
     */
    private void handle(Optional<ListenableFuture<Update>> futureSailor, Gossiper link, int ring, Duration duration,
                        ScheduledExecutorService scheduler, Timer.Context timer) {
        if (!started.get() || link == null) {
            if (timer != null) {
                timer.stop();
            }
            return;
        }
        try {
            if (futureSailor.isEmpty()) {
                if (timer != null) {
                    timer.stop();
                }
                log.trace("no update from {} on: {}", link.getMember(), member);
                return;
            }
            Update update;
            try {
                update = futureSailor.get().get();
            } catch (InterruptedException e) {
                log.error("error gossiping with {} on: {}", link.getMember(), member, e);
                return;
            } catch (ExecutionException e) {
                var cause = e.getCause();
                if (cause instanceof StatusRuntimeException sre) {
                    final var code = sre.getStatus().getCode();
                    if (code.equals(Status.UNAVAILABLE.getCode()) || code.equals(Status.NOT_FOUND.getCode()) ||
                        code.equals(Status.UNIMPLEMENTED.getCode())) {
                        return;
                    }
                }
                log.warn("error gossiping with {} on: {}", link.getMember(), member, cause);
                return;
            }
            log.debug("gossip update with {} on: {}", link.getMember(), member);
            link.update(ContextUpdate.newBuilder()
                                     .setContext(context.getId().toDigeste())
                                     .setRing(ring)
                                     .setUpdate(update(update))
                                     .build());
        } finally {
            if (timer != null) {
                timer.stop();
            }
            if (started.get()) {
                scheduled = scheduler.schedule(() -> oneRound(duration, scheduler), duration.toMillis(),
                                               TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * Answer the Have state of the receiver
     */
    private Have have() {
        return Have.newBuilder()
                   .setHaveCommits(haveCommits())
                   .setHavePreVotes(havePreVotes())
                   .setHaveUnits(haveUnits())
                   .build();
    }

    /**
     * Answer the bloom filter with the commits the receiver has
     */
    private Biff haveCommits() {
        final var config = config();
        var biff = new DigestBloomFilter(Utils.bitStreamEntropy().nextLong(), config.epochLength()
        * config.numberOfEpochs() * config.nProc() * config.nProc() * 2, config.fpr());
        commits.keySet().forEach(h -> biff.add(h));
        return biff.toBff();
    }

    /**
     * Answer the bloom filter with the preVotes the receiver has
     */
    private Biff havePreVotes() {
        final var config = config();
        var biff = new DigestBloomFilter(Utils.bitStreamEntropy().nextLong(), config.epochLength()
        * config.numberOfEpochs() * config.nProc() * config.nProc() * 2, config.fpr());
        preVotes.keySet().forEach(h -> biff.add(h));
        return biff.toBff();
    }

    /**
     * Answer the bloom filter with the units the receiver has
     */
    private Biff haveUnits() {
        final var config = config();
        var biff = new DigestBloomFilter(Utils.bitStreamEntropy().nextLong(),
                                         config.epochLength() * config.numberOfEpochs() * config.nProc() * 2,
                                         config.fpr());
        adder.have(biff);
        return biff.toBff();
    }

    /**
     * Perform one round of gossip
     */
    private void oneRound(Duration duration, ScheduledExecutorService scheduler) {
        if (!started.get()) {
            return;
        }
        var timer = metrics == null ? null : metrics.gossipRoundDuration().time();
        ring.execute((link, ring) -> gossipRound(link, ring),
                     (futureSailor, link, ring) -> handle(futureSailor, link, ring, duration, scheduler, timer));
    }

    private SignedPreVote preVote(SignedPreVote spv) {
        if (!validate(spv)) {
            return null;
        }
        final var pv = spv.getVote();
        adder.preVote(Digest.from(pv.getHash()), (short) pv.getSource(), this);
        return spv;
    }

    private void read(Runnable r) {
        final var read = mx.readLock();
        read.lock();
        try {
            r.run();
        } finally {
            read.unlock();
        }
    }

    private <T> T read(Supplier<T> s) {
        final var read = mx.readLock();
        read.lock();
        try {
            return s.get();
        } finally {
            read.unlock();
        }
    }

    /**
     * Provide the missing state from the receiver based on the supplied haves
     */
    private void update(Have have, final Builder builder) {
        final var cbf = BloomFilter.from(have.getHaveCommits());
        commits.entrySet().forEach(e -> {
            if (!cbf.contains(e.getKey())) {
                builder.addMissingCommits(e.getValue());
            }
        });
        final var pbf = BloomFilter.from(have.getHavePreVotes());
        preVotes.entrySet().forEach(e1 -> {
            if (!pbf.contains(e1.getKey())) {
                builder.addMissingPreVotes(e1.getValue());
            }
        });
        final BloomFilter<Digest> pubf = BloomFilter.from(have.getHaveUnits());
        adder.missing(pubf, builder);
    }

    /**
     * Update the receiver state from the supplied update. Return an update based on
     * the current state and the haves of the supplied update
     */
    private Update update(Update update) {
        final var builder = Update.newBuilder();
        read(() -> update(update.getHaves(), builder));
        write(() -> updateFrom(update));
        return builder.setHaves(have()).build();
    }

    /**
     * Update the commit, prevote and unit state from the supplied update
     */
    private void updateFrom(Update update) {
        update.getMissingList().forEach(u -> {
            final var signature = JohnHancock.from(u.getSignature());
            final var digest = signature.toDigest(config().digestAlgorithm());
            adder.propose(digest, u, this);
        });
        update.getMissingCommitsList().forEach(c -> {
            final var signature = JohnHancock.from(c.getSignature());
            final var digest = signature.toDigest(config().digestAlgorithm());
            commits.computeIfAbsent(digest, h -> committed(c));
        });
        update.getMissingPreVotesList().forEach(pv -> {
            final var signature = JohnHancock.from(pv.getSignature());
            preVotes.computeIfAbsent(signature.toDigest(config().digestAlgorithm()), h -> preVote(pv));
        });
    }

    private boolean validate(SignedCommit c) {
        // TODO Auto-generated method stub
        return true;
    }

    private boolean validate(SignedPreVote pv) {
        // TODO Auto-generated method stub
        return true;
    }

    private void write(Runnable r) {
        final var write = mx.writeLock();
        write.lock();
        try {
            r.run();
        } finally {
            write.unlock();
        }
    }
}
