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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.ethereal.proto.Gossip;
import com.salesfoce.apollo.ethereal.proto.Update;
import com.salesforce.apollo.comm.RingCommunications;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.comm.RouterMetrics;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.ethereal.Gossiper;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class ContextGossiper<T extends Member, S extends SigningMember> {
    private class Terminal implements GossiperService {
        @Override
        public Update gossip(Gossip request, Digest from) {
            @SuppressWarnings("unchecked")
            T predecessor = context.ring(request.getRing()).predecessor((T) member);
            if (predecessor == null || !from.equals(predecessor.getId())) {
                log.trace("Invalid inbound gossip on {}:{} from: {} on ring: {} - not predecessor: {}", context.getId(),
                          member, from, request.getRing(), predecessor);
                return Update.getDefaultInstance();
            }
            return gossiper.gossip(request);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ContextGossiper.class);

    private final CommonCommunications<Scuttlebutte, GossiperService> comm;
    private final Context<T>                                          context;
    private final Gossiper                                            gossiper;
    private final S                                                   member;
    private final RingCommunications<Scuttlebutte>                    ring;
    private final AtomicBoolean                                       started = new AtomicBoolean();

    public ContextGossiper(Gossiper gossiper, Context<T> context, S member, Router communications, Executor executor,
                           RouterMetrics metrics) {
        this.context = context;
        this.gossiper = gossiper;
        comm = communications.create((Member) member, context.getId(), new Terminal(),
                                     r -> new GossiperServer(communications.getClientIdentityProvider(), metrics,
                                                             (RoutableService<GossiperService>) r),
                                     getCreate(metrics, executor), Scuttlebutte.getLocalLoopback(member));
        @SuppressWarnings("unchecked")
        final var cast = (Context<Member>) context;
        ring = new RingCommunications<Scuttlebutte>(cast, member, this.comm, executor);
        this.member = member;
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

    private ListenableFuture<Update> gossipRound(Scuttlebutte link, int ring) {
        if (!started.get()) {
            return null;
        }
        log.trace("gossiping[{}] from {} with {} ring: {} on {}", context.getId(), link.getMember(), ring, member);
        try {
            return link.gossip(gossiper.gossip(context.getId()));
        } catch (Throwable e) {
            log.trace("gossiping[{}] failed from {} with {} ring: {} on {}", context.getId(), member, ring,
                      link.getMember(), e);
            return null;
        }
    }

    private void handle(Optional<ListenableFuture<Update>> futureSailor, Scuttlebutte link, int ring, Duration duration,
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
                log.debug("error gossiping with {} on: {}", link.getMember(), member, e.getCause());
                return;
            }
            log.debug("gossip update with {} on: {}", link.getMember(), member);
            gossiper.update(update);
        } finally {
            if (started.get()) {
                scheduler.schedule(() -> oneRound(duration, scheduler), duration.toMillis(), TimeUnit.MILLISECONDS);
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
}
