/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.gossip;

import static com.salesforce.apollo.choam.gossip.GossipClient.getCreate;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.choam.proto.Have;
import com.salesfoce.apollo.choam.proto.Update;
import com.salesfoce.apollo.choam.proto.Validate;
import com.salesfoce.apollo.choam.proto.ViewMember;
import com.salesforce.apollo.comm.RingCommunications;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.comm.RouterMetrics;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.ethereal.memberships.ContextGossiper;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.utils.Utils;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;

import io.grpc.StatusRuntimeException;

/**
 * @author hal.hildebrand
 *
 */
public class Gossiper {
    private class Terminal implements GossipService {
        @Override
        public Update gossip(Have request, Digest from) {
            Member predecessor = context.ring(request.getRing()).predecessor(member);
            if (predecessor == null || !from.equals(predecessor.getId())) {
                log.trace("Invalid inbound gossip on {}:{} from: {} on ring: {} - not predecessor: {}", context.getId(),
                          member, from, request.getRing(), predecessor);
                return Update.getDefaultInstance();
            }
            final var builder = Update.newBuilder();
            BloomFilter<Digest> biff = BloomFilter.from(request.getValidations());
            validations.entrySet().forEach(e -> {
                if (!biff.contains(e.getKey())) {
                    builder.addValidations(e.getValue());
                }
            });
            return builder.build();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ContextGossiper.class);

    private final Predicate<ViewMember>                       acceptJoin;
    private final Predicate<Validate>                         acceptValidate;
    private final DigestAlgorithm                             algo;
    private final CommonCommunications<Gossip, GossipService> comm;
    private final Context<Member>                             context;
    private final List<BloomFilter<Digest>>                   joinBiffs       = new ArrayList<>();
    private final ConcurrentMap<Digest, ViewMember>           joins           = new ConcurrentHashMap<>();
    private final SigningMember                               member;
    private final Lock                                        mtx             = new ReentrantLock();
    private final RingCommunications<Gossip>                  ring;
    private final AtomicInteger                               round           = new AtomicInteger();
    private final Consumer<Integer>                           roundListener;
    private final AtomicBoolean                               started         = new AtomicBoolean();
    private final List<BloomFilter<Digest>>                   validationBiffs = new ArrayList<>();
    private final ConcurrentMap<Digest, Validate>             validations     = new ConcurrentHashMap<>();

    public Gossiper(Context<Member> context, SigningMember member, Predicate<Validate> acceptValidate,
                    Predicate<ViewMember> acceptJoin, Router communications, RouterMetrics metrics,
                    DigestAlgorithm algo, Consumer<Integer> roundListener) {
        this(context, member, acceptValidate, acceptJoin, communications, metrics, algo, roundListener, r -> r.run());
    }

    public Gossiper(Context<Member> context, SigningMember member, Predicate<Validate> acceptValidate,
                    Predicate<ViewMember> acceptJoin, Router communications, RouterMetrics metrics,
                    DigestAlgorithm algo, Consumer<Integer> roundListener, Executor exec) {
        this.context = context;
        this.member = member;
        this.algo = algo;
        this.roundListener = roundListener;
        comm = communications.create((Member) member, context.getId(), new Terminal(),
                                     r -> new GossipServer(communications.getClientIdentityProvider(), metrics, r),
                                     getCreate(metrics), Gossip.getLocalLoopback(member));
        final var cast = (Context<Member>) context;
        ring = new RingCommunications<Gossip>(cast, member, this.comm, null);
        this.acceptValidate = acceptValidate;
        this.acceptJoin = acceptJoin;
    }

    public void add(Validate validate) {
        final var key = JohnHancock.from(validate.getWitness().getSignature()).toDigest(algo);
        validations.put(key, validate);
        mtx.lock();
        try {
            validationBiffs.forEach(biff -> biff.add(key));
        } finally {
            mtx.unlock();
        }
    }

    public void add(ViewMember join) {
        final var key = JohnHancock.from(join.getSignature()).toDigest(algo);
        joins.put(key, join);
        mtx.lock();
        try {
            joinBiffs.forEach(biff -> biff.add(key));
        } finally {
            mtx.unlock();
        }
    }

    public Context<Member> getContext() {
        return context;
    }

    public ViewMember getJoin(Digest id) {
        return joins.get(id);
    }

    public int getJoinCount() {
        return joins.size();
    }

    public void start(Duration duration, ScheduledExecutorService scheduler) {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        Duration initialDelay = duration.plusMillis(Utils.bitStreamEntropy().nextLong(duration.toMillis()));
        log.info("Starting Gossiper[{}] on: {}", context.getId(), member);
        comm.register(context.getId(), new Terminal());
        scheduler.schedule(() -> oneRound(duration, scheduler), initialDelay.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        log.info("Stopping Gossiper [{}] for {}", context.getId(), member);
        comm.deregister(context.getId());
    }

    private ListenableFuture<Update> gossipRound(Gossip link, int ring) {
        if (!started.get()) {
            return null;
        }
        log.trace("gossiping[{}] from {} with {} ring: {} on {}", context.getId(), link.getMember(), ring, member);
        mtx.lock();
        try {
            return link.gossip(Have.newBuilder().setContext(context.getId().toDigeste())
                                   .setValidations(validationBiffs.get(Utils.bitStreamEntropy()
                                                                            .nextInt(validationBiffs.size()))
                                                                  .toBff())
                                   .setJoins(joinBiffs.get(Utils.bitStreamEntropy().nextInt(joinBiffs.size())).toBff())
                                   .build());
        } catch (StatusRuntimeException e) {
            log.debug("gossiping[{}] failed with: {} from {} with {} ring: {} on {}", context.getId(), e.getMessage(),
                      member, ring, link.getMember(), e);
            return null;
        } catch (Throwable e) {
            log.warn("gossiping[{}] failed from {} with {} ring: {} on {}", context.getId(), member, ring,
                     link.getMember(), e);
            return null;
        } finally {
            mtx.unlock();
        }
    }

    private void handle(Optional<ListenableFuture<Update>> futureSailor, Gossip link, int ring, Duration duration,
                        ScheduledExecutorService scheduler) {
        try {
            if (futureSailor.isEmpty()) {
                return;
            }
            Update update;
            try {
                update = futureSailor.get().get();
            } catch (InterruptedException e) {
                log.debug("error gossiping with {} on: {}", link.getMember(), member, e);
                return;
            } catch (ExecutionException e) {
                if (e.getCause()instanceof StatusRuntimeException sre) {
                    log.debug("error gossiping with {} : {} on: {}", link.getMember(), sre.getMessage(), member);
                    return;
                }
                log.warn("error gossiping with {} on: {}", link.getMember(), member, e.getCause());
                return;
            }
            log.debug("gossip update with {} on: {}", link.getMember(), member);
            mtx.lock();
            try {
                updateJoins(update);
                updateValidations(update);
            } finally {
                mtx.unlock();
            }
        } finally {
            if (roundListener != null) {
                try {
                    roundListener.accept(round.incrementAndGet());
                } catch (Throwable t) {
                    log.warn("Error during round listerner on: {}", member, t);
                }
                if (started.get()) {
                    scheduler.schedule(() -> oneRound(duration, scheduler), duration.toMillis(), TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    private void oneRound(Duration duration, ScheduledExecutorService scheduler) {
        if (!started.get()) {
            return;
        }

        ring.execute((link, ring) -> gossipRound(link, ring),
                     (futureSailor, link, ring) -> handle(futureSailor, link, ring, duration, scheduler));
    }

    private void updateJoins(Update update) {
        update.getJoinsList().forEach(j -> {
            Digest key = JohnHancock.from(j.getSignature()).toDigest(algo);
            joins.computeIfAbsent(key, k -> {
                joinBiffs.forEach(biff -> biff.add(k)); // note we've already seen it, even if not
                                                        // accepted
                if (acceptJoin.test(j)) {
                    return j;
                } else {
                    return null;
                }
            });
        });
    }

    private void updateValidations(Update update) {
        update.getValidationsList().forEach(v -> {
            Digest key = JohnHancock.from(v.getWitness().getSignature()).toDigest(algo);
            validations.computeIfAbsent(key, k -> {
                validationBiffs.forEach(biff -> biff.add(k)); // note we've already seen it, even if not
                                                              // accepted
                if (acceptValidate.test(v)) {
                    return v;
                } else {
                    return null;
                }
            });
        });
    }
}
