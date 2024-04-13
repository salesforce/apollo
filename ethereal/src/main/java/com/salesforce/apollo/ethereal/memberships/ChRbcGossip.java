/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.memberships;

import com.codahale.metrics.Timer;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.RouterImpl.CommonCommunications;
import com.salesforce.apollo.archipelago.server.FernetServerInterceptor;
import com.salesforce.apollo.context.Context;
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
import com.salesforce.apollo.ring.RingCommunications;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.Utils;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static com.salesforce.apollo.ethereal.memberships.comm.GossiperClient.getCreate;

/**
 * Handles the gossip propigation of proposals, commits and preVotes from this node, as well as the notification of the
 * adder of such from other nodes.
 *
 * @author hal.hildebrand
 */
public class ChRbcGossip {

    private static final Logger                                          log      = LoggerFactory.getLogger(
    ChRbcGossip.class);
    private final        CommonCommunications<Gossiper, GossiperService> comm;
    private final        Context<Member>                                 context;
    private final        SigningMember                                   member;
    private final        EtherealMetrics                                 metrics;
    private final        Processor                                       processor;
    private final        RingCommunications<Member, Gossiper>            ring;
    private final        AtomicBoolean                                   started  = new AtomicBoolean();
    private final        Terminal                                        terminal = new Terminal();
    private volatile     ScheduledFuture<?>                              scheduled;

    public ChRbcGossip(Context<Member> context, SigningMember member, Processor processor, Router communications,
                       EtherealMetrics m) {
        this.processor = processor;
        this.context = context;
        this.member = member;
        this.metrics = m;
        comm = communications.create(member, context.getId(), terminal, getClass().getCanonicalName(),
                                     r -> new GossiperServer(communications.getClientIdentityProvider(), metrics, r),
                                     getCreate(metrics), Gossiper.getLocalLoopback(member));
        ring = new RingCommunications<>(context, member, this.comm);
    }

    public Context<Member> getContext() {
        return context;
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
        log.trace("Starting GossipService[{}] on: {}", context.getId(), member.getId());
        comm.register(context.getId(), terminal, validator);
        var scheduler = Executors.newScheduledThreadPool(1, Thread.ofVirtual().factory());
        scheduler.schedule(() -> Thread.ofVirtual().start(Utils.wrapped(() -> {
            try {
                oneRound(duration, scheduler);
            } catch (Throwable e) {
                log.error("Error in gossip on: {}", member.getId(), e);
            }
        }, log)), initialDelay.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Stop the receiver's gossip
     */
    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        log.trace("Stopping GossipService [{}] for {}", context.getId(), member.getId());
        comm.deregister(context.getId());
        final var current = scheduled;
        scheduled = null;
        if (current != null) {
            current.cancel(true);
        }
    }

    /**
     * Perform the first phase of the gossip. Send our partner the Have state of the receiver
     */
    private Update gossipRound(Gossiper link, int ring) {
        if (!started.get()) {
            return null;
        }
        log.trace("gossiping[{}] with {} ring: {} on {}", context.getId(), link.getMember(), ring, member);
        try {
            return link.gossip(processor.gossip(context.getId(), ring));
        } catch (StatusRuntimeException e) {
            log.debug("gossiping[{}] failed: {} with: {} with {} ring: {} on: {}", context.getId(), e.getMessage(),
                      member.getId(), ring, link.getMember().getId(), member.getId());
            return null;
        } catch (Throwable e) {
            log.warn("gossiping[{}] failed: {} from {} with {} ring: {} on: {}", context.getId(), member.getId(), ring,
                     link.getMember().getId(), ring, member.getId(), e);
            return null;
        }
    }

    /**
     * The second phase of the gossip. Handle the update from our gossip partner
     */
    private void handle(Optional<Update> result, RingCommunications.Destination<Member, Gossiper> destination,
                        Duration duration, ScheduledExecutorService scheduler, Timer.Context timer) {
        if (!started.get() || destination == null || destination.link() == null) {
            if (timer != null) {
                timer.stop();
            }
            return;
        }
        try {
            if (result.isEmpty()) {
                if (timer != null) {
                    timer.stop();
                }
                return;
            }
            Update update = result.get();
            if (update.equals(Update.getDefaultInstance())) {
                return;
            }
            try {
                var u = processor.update(update);
                if (!Update.getDefaultInstance().equals(u)) {
                    log.trace("Gossip update with: {} on: {}", destination.member().getId(), member.getId());
                    destination.link()
                               .update(ContextUpdate.newBuilder().setRing(destination.ring()).setUpdate(u).build());
                }
            } catch (StatusRuntimeException e) {
                log.debug("gossiping[{}] failed: {} with: {} with {} ring: {} on: {}", context.getId(), e.getMessage(),
                          member.getId(), ring, destination.member().getId(), member.getId());
            } catch (Throwable e) {
                log.warn("gossiping[{}] failed: {} with: {} with {} ring: {} on: {}", context.getId(), e.getMessage(),
                         member.getId(), ring, destination.member().getId(), member.getId(), e);
            }
        } finally {
            if (timer != null) {
                timer.stop();
            }
            if (started.get()) {
                scheduled = scheduler.schedule(
                () -> Thread.ofVirtual().start(Utils.wrapped(() -> oneRound(duration, scheduler), log)),
                duration.toMillis(), TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * Perform one round of gossip
     */
    private void oneRound(Duration duration, ScheduledExecutorService scheduler) {
        if (!started.get()) {
            return;
        }
        var timer = metrics == null ? null : metrics.gossipRoundDuration().time();
        ring.execute(this::gossipRound,
                     (result, destination) -> handle(result, destination, duration, scheduler, timer));
    }

    /**
     * The Service implementing the 3-phase gossip
     */
    private class Terminal implements GossiperService, Router.ServiceRouting {
        @Override
        public Update gossip(Gossip request, Digest from) {
            Member predecessor = context.predecessor(request.getRing(), member);
            if (predecessor == null || !from.equals(predecessor.getId())) {
                log.debug("Invalid inbound gossip context: {} from: {} on ring: {} - not predecessor: {} on: {}",
                          context.getId(), from, request.getRing(),
                          predecessor == null ? "<null>" : predecessor.getId(), member.getId());
                return Update.getDefaultInstance();
            }
            final var update = processor.gossip(request);
            log.trace("GossipService received from: {} missing: {} on: {}", from, update.getMissingCount(),
                      member.getId());
            return update;
        }

        @Override
        public void update(ContextUpdate request, Digest from) {
            Member predecessor = context.predecessor(request.getRing(), member);
            if (predecessor == null || !from.equals(predecessor.getId())) {
                log.debug("Invalid inbound update context:{} from: {} on ring: {} - not predecessor: {} on: {}",
                          context.getId(), from, request.getRing(),
                          predecessor == null ? "<null>" : predecessor.getId(), member.getId());
                return;
            }
            log.trace("gossip update with {} on: {}", from, member.getId());
            processor.updateFrom(request.getUpdate());
        }
    }
}
