/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.memberships;

import static com.salesforce.apollo.ethereal.memberships.GossiperClient.getCreate;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.ethereal.proto.ContextUpdate;
import com.salesfoce.apollo.ethereal.proto.Gossip;
import com.salesfoce.apollo.ethereal.proto.Have;
import com.salesfoce.apollo.ethereal.proto.PreUnit_s;
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
import com.salesforce.apollo.ethereal.Unit;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.utils.Utils;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * @author hal.hildebrand
 *
 */
public class RbcGossiper {
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
            RbcGossiper.this.updateFrom(request.getUpdate());
        }
    }

    private static final Logger log = LoggerFactory.getLogger(RbcGossiper.class);

    private final CommonCommunications<Gossiper, GossiperService> comm;
    private final Map<Digest, SignedCommit>                       commits  = new ConcurrentHashMap<>();
    private final Config                                          config;
    private final Context<Member>                                 context;
    private final SigningMember                                   member;
    private final EtherealMetrics                                 metrics;
    private final Map<Digest, SignedPreVote>                      preVotes = new ConcurrentHashMap<>();
    private final RingCommunications<Gossiper>                    ring;
    private final AtomicInteger                                   round    = new AtomicInteger();
    private volatile ScheduledFuture<?>                           scheduled;
    private final AtomicBoolean                                   started  = new AtomicBoolean();
    private final Map<Digest, Node>                               units    = new ConcurrentHashMap<>();

    public RbcGossiper(Context<Member> context, SigningMember member, Config config, Router communications,
                       Executor exec, EtherealMetrics m) {
        this.context = context;
        this.member = member;
        this.metrics = m;
        this.config = config;
        comm = communications.create((Member) member, context.getId(), new Terminal(),
                                     r -> new GossiperServer(communications.getClientIdentityProvider(), metrics, r),
                                     getCreate(metrics), Gossiper.getLocalLoopback(member));
        final var cast = context;
        ring = new RingCommunications<Gossiper>(cast, member, this.comm, exec);
    }

    public Context<Member> getContext() {
        return context;
    }

    public void nextRound() {
        round.incrementAndGet();
    }

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

    public void submit(Unit unit) {

    }

    public void updateFrom(Update update) {
        update.getMissingList().forEach(u -> {
            final var signature = JohnHancock.from(u.getSignature());
            updateUnit(signature.toDigest(config.digestAlgorithm()), u);
        });
        update.getMissingCommitsList().forEach(c -> {
            final var signature = JohnHancock.from(c.getSignature());
            updateCommit(signature.toDigest(config.digestAlgorithm()), c);
        });
        update.getMissingPreVotesList().forEach(pv -> {
            final var signature = JohnHancock.from(pv.getSignature());
            updatePreVote(signature.toDigest(config.digestAlgorithm()), pv);
        });
    }

    private Update gossip(Have have) {
        final var builder = Update.newBuilder().setHaves(have());
        update(have, builder);
        return builder.build();
    }

    private ListenableFuture<Update> gossipRound(Gossiper link, int ring) {
        if (!started.get()) {
            return null;
        }
        log.debug("gossiping[{}] with {} ring: {} on {}", context.getId(), link.getMember(), ring, member);
        try {
            return link.gossip(Gossip.newBuilder().setContext(context.getId().toDigeste()).setHaves(have()).build());
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

    private Have have() {
        return Have.newBuilder()
                   .setHaveCommits(haveCommits())
                   .setHavePreVotes(havePreVotes())
                   .setHaveUnits(haveUnits())
                   .build();
    }

    private Biff haveCommits() {
        var biff = new DigestBloomFilter(Utils.bitStreamEntropy().nextLong(), config.epochLength()
        * config.numberOfEpochs() * config.nProc() * config.nProc() * 2, config.fpr());
        commits.keySet().forEach(h -> biff.add(h));
        return biff.toBff();
    }

    private void missingCommits(BloomFilter<Digest> have, Update.Builder builder) {
        commits.entrySet().forEach(e -> {
            if (!have.contains(e.getKey())) {
                builder.addMissingCommits(e.getValue());
            }
        });
    }

    private Biff havePreVotes() {
        var biff = new DigestBloomFilter(Utils.bitStreamEntropy().nextLong(), config.epochLength()
        * config.numberOfEpochs() * config.nProc() * config.nProc() * 2, config.fpr());
        preVotes.keySet().forEach(h -> biff.add(h));
        return biff.toBff();
    }

    private void missingPreVotes(BloomFilter<Object> have, Builder builder) {
        preVotes.entrySet().forEach(e -> {
            if (!have.contains(e.getKey())) {
                builder.addMissingPreVotes(e.getValue());
            }
        });
    }

    private Biff haveUnits() {
        var biff = new DigestBloomFilter(Utils.bitStreamEntropy().nextLong(),
                                         config.epochLength() * config.numberOfEpochs() * config.nProc() * 2,
                                         config.fpr());
        units.keySet().forEach(h -> biff.add(h));
        return biff.toBff();
    }

    private void missingUnits(BloomFilter<Object> have, Builder builder) {
        units.entrySet().forEach(e -> {
            e.getValue().missing(pu -> builder.addMissing(pu));
        });
    }

    private void oneRound(Duration duration, ScheduledExecutorService scheduler) {
        if (!started.get()) {
            return;
        }
        var timer = metrics == null ? null : metrics.gossipRoundDuration().time();
        ring.execute((link, ring) -> gossipRound(link, ring),
                     (futureSailor, link, ring) -> handle(futureSailor, link, ring, duration, scheduler, timer));
    }

    private void update(Have have, final Builder builder) {
        missingCommits(BloomFilter.from(have.getHaveCommits()), builder);
        missingPreVotes(BloomFilter.from(have.getHavePreVotes()), builder);
        missingUnits(BloomFilter.from(have.getHaveUnits()), builder);
    }

    private Update update(Update update) {
        final var builder = Update.newBuilder().setHaves(have());
        update(update.getHaves(), builder);
        return builder.build();
    }

    private void updateCommit(Digest digest, SignedCommit c) {
        commits.computeIfAbsent(digest, h -> c);
    }

    private void updatePreVote(Digest digest, SignedPreVote pv) {
        preVotes.computeIfAbsent(digest, h -> pv);
    }

    private void updateUnit(Digest digest, PreUnit_s u) {
        units.computeIfAbsent(digest, h -> new MaterializedNode(digest, u));
    }
}
