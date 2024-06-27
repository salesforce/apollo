/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.memberships;

import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.RouterImpl.CommonCommunications;
import com.salesforce.apollo.archipelago.server.FernetServerInterceptor;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.ethereal.Processor;
import com.salesforce.apollo.ethereal.memberships.comm.EtherealMetrics;
import com.salesforce.apollo.ethereal.memberships.comm.Gossiper;
import com.salesforce.apollo.ethereal.memberships.comm.GossiperServer;
import com.salesforce.apollo.ethereal.memberships.comm.GossiperService;
import com.salesforce.apollo.ethereal.proto.ContextUpdate;
import com.salesforce.apollo.ethereal.proto.Gossip;
import com.salesforce.apollo.ethereal.proto.Update;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.ring.SliceIterator;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.Utils;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static com.salesforce.apollo.ethereal.memberships.comm.GossiperClient.getCreate;

/**
 * Handles the gossip propagation of proposals, the commits and preVotes from this node, and the notification of the
 * Adder of such events from other nodes.
 *
 * @author hal.hildebrand
 */
public class ChRbcGossip {

    private static final Logger                                          log      = LoggerFactory.getLogger(
    ChRbcGossip.class);
    private final        CommonCommunications<Gossiper, GossiperService> comm;
    private final        Digest                                          id;
    private final        SigningMember                                   member;
    private final        EtherealMetrics                                 metrics;
    private final        Processor                                       processor;
    private final        SliceIterator<Gossiper>                         ring;
    private final        AtomicBoolean                                   started  = new AtomicBoolean();
    private final        Terminal                                        terminal = new Terminal();
    private volatile     ScheduledFuture<?>                              scheduled;

    public ChRbcGossip(Digest id, SigningMember member, Collection<Member> membership, Processor processor,
                       Router communications, EtherealMetrics m, ScheduledExecutorService scheduler) {
        this.processor = processor;
        this.member = member;
        this.metrics = m;
        this.id = id;
        comm = communications.create(member, id, terminal, getClass().getCanonicalName(),
                                     r -> new GossiperServer(communications.getClientIdentityProvider(), metrics, r),
                                     getCreate(metrics), Gossiper.getLocalLoopback(member));
        ring = new SliceIterator<>("ChRbcGossip[%s on: %s]".formatted(id, member.getId()), member, membership, comm,
                                   scheduler);
    }

    /**
     * Start the receiver's gossip
     */
    public void start(Duration duration) {
        start(duration, null);
    }

    /**
     * Start the receiver's gossip
     */
    public void start(Duration duration, Predicate<FernetServerInterceptor.HashedToken> validator) {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        Duration initialDelay = duration.plusMillis(Entropy.nextBitsStreamLong(duration.toMillis()));
        log.trace("Starting GossipService[{}] on: {}", id, member.getId());
        comm.register(id, terminal, validator);
        try {
            var scheduler = Executors.newScheduledThreadPool(1, Thread.ofVirtual().factory());
            scheduler.schedule(() -> Thread.ofVirtual().start(Utils.wrapped(() -> {
                try {
                    gossip(duration, scheduler);
                } catch (Throwable e) {
                    log.error("Error in gossip on: {}", member.getId(), e);
                }
            }, log)), initialDelay.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Throwable e) {
            log.error("Error in gossip on: {}", member.getId(), e);
        }
    }

    /**
     * Stop the receiver's gossip
     */
    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        log.trace("Stopping GossipService [{}] for {}", id, member.getId());
        comm.deregister(id);
        final var current = scheduled;
        scheduled = null;
        if (current != null) {
            current.cancel(true);
        }
    }

    private void gossip(Duration frequency, ScheduledExecutorService scheduler) {
        if (!started.get()) {
            return;
        }
        var timer = metrics == null ? null : metrics.gossipRoundDuration().time();
        ring.iterate((link) -> gossipRound(link), (result, _, link, _) -> {
            handle(result, link);
            return true;
        }, () -> {
            if (timer != null) {
                timer.stop();
            }
            if (started.get()) {
                scheduled = scheduler.schedule(
                () -> Thread.ofVirtual().start(Utils.wrapped(() -> gossip(frequency, scheduler), log)),
                frequency.toNanos(), TimeUnit.NANOSECONDS);
            }
        }, frequency);
    }

    /**
     * Perform the first phase of the gossip. Send our partner the Have state of the receiver
     */
    private Update gossipRound(Gossiper link) {
        if (!started.get()) {
            return null;
        }
        log.trace("gossiping[{}] with {} on {}", id, link.getMember(), member.getId());
        try {
            return link.gossip(processor.gossip(id));
        } catch (StatusRuntimeException e) {
            log.debug("gossiping[{}] failed: {} with: {} with {} on: {}", id, e.getMessage(), member.getId(),
                      link.getMember().getId(), member.getId());
            return null;
        } catch (Throwable e) {
            log.warn("gossiping:{} with: {} failed on: {}", id, link.getMember().getId(), member.getId(), e);
            return null;
        }
    }

    /**
     * The second phase of the gossip. Handle the update from our gossip partner
     */
    private void handle(Optional<Update> result, Gossiper link) {
        if (!started.get()) {
            return;
        }
        if (link == null) {
            return;
        }
        if (result.isEmpty()) {
            return;
        }
        Update update = result.get();
        if (update.equals(Update.getDefaultInstance())) {
            return;
        }
        try {
            var u = processor.update(update);
            if (!Update.getDefaultInstance().equals(u)) {
                log.trace("Gossip update with: {} on: {}", link.getMember().getId(), member.getId());
                link.update(ContextUpdate.newBuilder().setUpdate(u).build());
            }
        } catch (StatusRuntimeException e) {
            log.debug("gossiping[{}] failed: {} with: {} with {} on: {}", id, e.getMessage(), member.getId(),
                      link.getMember().getId(), member.getId());
        } catch (Throwable e) {
            log.warn("gossiping[{}] failed: {} with: {} with {} on: {}", id, e.getMessage(), member.getId(),
                     link.getMember().getId(), member.getId(), e);
        }
    }

    /**
     * The Service implementing the 3-phase gossip
     */
    private class Terminal implements GossiperService, Router.ServiceRouting {
        @Override
        public Update gossip(Gossip request, Digest from) {
            final var update = processor.gossip(request);
            log.trace("GossipService received from: {} missing: {} on: {}", from, update.getMissingCount(),
                      member.getId());
            return update;
        }

        @Override
        public void update(ContextUpdate request, Digest from) {
            log.trace("gossip update with {} on: {}", from, member.getId());
            processor.updateFrom(request.getUpdate());
        }
    }
}
