/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.memberships;

import static com.salesforce.apollo.ethereal.memberships.GossiperClient.getCreate;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.ethereal.proto.ContextUpdate;
import com.salesfoce.apollo.ethereal.proto.Gossip;
import com.salesfoce.apollo.ethereal.proto.Update;
import com.salesforce.apollo.comm.RingCommunications;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.ethereal.Ethereal.Controller;
import com.salesforce.apollo.ethereal.GossipService;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.utils.Utils;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * @author hal.hildebrand
 *
 */
public class ContextGossiper {
    private class Terminal implements GossiperService {
        @Override
        public Update gossip(Gossip request, Digest from) {
            Member predecessor = context.ring(request.getRing()).predecessor(member);
            if (predecessor == null || !from.equals(predecessor.getId())) {
                log.debug("Invalid inbound gossip on {}:{} from: {} on ring: {} - not predecessor: {}", context.getId(),
                          member, from, request.getRing(), predecessor);
                return Update.getDefaultInstance();
            }
            final var update = gossiper.gossip(request);
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
            gossiper.update(request.getUpdate());
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ContextGossiper.class);

    private final CommonCommunications<Gossiper, GossiperService> comm;
    private final Context<Member>                                 context;
    private final GossipService                                   gossiper;
    private final SigningMember                                   member;
    private final EtherealMetrics                                 metrics;
    private final RingCommunications<Gossiper>                    ring;
    private volatile ScheduledFuture<?>                           scheduled;
    private final AtomicBoolean                                   started = new AtomicBoolean();

    public ContextGossiper(Controller controller, Context<Member> context, SigningMember member, Router communications,
                           Executor exec, EtherealMetrics metrics) {
        this(new GossipService(controller), context, member, communications, exec, metrics);
    }

    public ContextGossiper(GossipService gossiper, Context<Member> context, SigningMember member, Router communications,
                           Executor exec, EtherealMetrics m) {
        this.context = context;
        this.gossiper = gossiper;
        this.member = member;
        this.metrics = m;
        comm = communications.create((Member) member, context.getId(), new Terminal(),
                                     r -> new GossiperServer(communications.getClientIdentityProvider(), metrics, r),
                                     getCreate(metrics), Gossiper.getLocalLoopback(member));
        final var cast = (Context<Member>) context;
        ring = new RingCommunications<Gossiper>(cast, member, this.comm, exec);
    }

    public Context<Member> getContext() {
        return context;
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

    private ListenableFuture<Update> gossipRound(Gossiper link, int ring) {
        if (!started.get()) {
            return null;
        }
        log.debug("gossiping[{}] with {} ring: {} on {}", context.getId(), link.getMember(), ring, member);
        try {
            return link.gossip(gossiper.gossip(context.getId(), ring));
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
            gossiper.update(update);
            link.update(ContextUpdate.newBuilder()
                                     .setContext(context.getId().toDigeste())
                                     .setRing(ring)
                                     .setUpdate(gossiper.updateFor(BloomFilter.from(update.getHave())))
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

    private void oneRound(Duration duration, ScheduledExecutorService scheduler) {
        if (!started.get()) {
            return;
        }
        var timer = metrics == null ? null : metrics.gossipRoundDuration().time();
        ring.execute((link, ring) -> gossipRound(link, ring),
                     (futureSailor, link, ring) -> handle(futureSailor, link, ring, duration, scheduler, timer));
    }
}
