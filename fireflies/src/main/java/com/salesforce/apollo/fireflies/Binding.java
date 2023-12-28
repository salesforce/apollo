/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import com.codahale.metrics.Timer;
import com.google.common.collect.HashMultiset;
import com.google.protobuf.ByteString;
import com.salesforce.apollo.archipelago.RouterImpl.CommonCommunications;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.cryptography.HexBloom;
import com.salesforce.apollo.cryptography.SignatureAlgorithm;
import com.salesforce.apollo.cryptography.proto.HexBloome;
import com.salesforce.apollo.fireflies.View.Node;
import com.salesforce.apollo.fireflies.View.Participant;
import com.salesforce.apollo.fireflies.View.Seed;
import com.salesforce.apollo.fireflies.View.Service;
import com.salesforce.apollo.fireflies.comm.entrance.Entrance;
import com.salesforce.apollo.fireflies.proto.*;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.ring.SliceIterator;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.Utils;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.joou.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Embodiment of the client side join protocol
 *
 * @author hal.hildebrand
 */
class Binding {
    private final static Logger                                  log = LoggerFactory.getLogger(Binding.class);
    private final        CommonCommunications<Entrance, Service> approaches;
    private final        Context<Participant>                    context;
    private final        DigestAlgorithm                         digestAlgo;
    private final        Duration                                duration;
    private final        FireflyMetrics                          metrics;
    private final        Node                                    node;
    private final        Parameters                              params;
    private final        ScheduledExecutorService                scheduler;
    private final        List<Seed>                              seeds;
    private final        View                                    view;

    public Binding(View view, List<Seed> seeds, Duration duration, ScheduledExecutorService scheduler,
                   Context<Participant> context, CommonCommunications<Entrance, Service> approaches, Node node,
                   Parameters params, FireflyMetrics metrics, DigestAlgorithm digestAlgo) {
        this.view = view;
        this.duration = duration;
        this.seeds = new ArrayList<>(seeds);
        this.scheduler = scheduler;
        this.context = context;
        this.node = node;
        this.params = params;
        this.metrics = metrics;
        this.approaches = approaches;
        this.digestAlgo = digestAlgo;
    }

    void seeding() {
        if (seeds.isEmpty()) {// This node is the bootstrap seed
            bootstrap();
            return;
        }
        Entropy.secureShuffle(seeds);
        log.info("Seeding view: {} context: {} with seeds: {} started on: {}", view.currentView(), this.context.getId(),
                 seeds.size(), node.getId());

        var seeding = new CompletableFuture<Redirect>();
        var timer = metrics == null ? null : metrics.seedDuration().time();
        seeding.whenComplete(join(duration, scheduler, timer));

        var bootstrappers = seeds.stream()
                                 .map(this::seedFor)
                                 .map(nw -> view.new Participant(nw))
                                 .filter(p -> !node.getId().equals(p.getId()))
                                 .collect(Collectors.toList());
        view.phase1Validation(bootstrappers); // Phase 1 can only rely upon the kindness of our initial configuration
        var seedlings = new SliceIterator<>("Seedlings", node, bootstrappers, approaches);
        AtomicReference<Runnable> reseed = new AtomicReference<>();
        reseed.set(() -> {
            final var registration = registration();
            seedlings.iterate((link, m) -> {
                log.debug("Requesting Seeding from: {} on: {}", link.getMember().getId(), node.getId());
                return link.seed(registration);
            }, (futureSailor, link, m) -> complete(seeding, futureSailor, m), () -> {
                if (!seeding.isDone()) {
                    scheduler.schedule(Utils.wrapped(reseed.get(), log), params.retryDelay().toNanos(),
                                       TimeUnit.NANOSECONDS);
                }
            }, scheduler, params.retryDelay());
        });
        reseed.get().run();
    }

    private void bootstrap() {
        log.info("Bootstrapping seed node view: {} context: {} on: {}", view.currentView(), this.context.getId(),
                 node.getId());
        var nw = node.getNote();

        view.bootstrap(nw, scheduler, duration);
    }

    private boolean complete(CompletableFuture<Redirect> redirect, Optional<Redirect> futureSailor, Member m) {
        if (futureSailor.isEmpty()) {
            return true;
        }
        if (redirect.isDone()) {
            return true;
        }
        final var r = futureSailor.get();
        if (redirect.complete(r)) {
            log.info("Redirect to view: {} context: {} from: {} on: {}", Digest.from(r.getView()), this.context.getId(),
                     m.getId(), node.getId());
        }
        return false;
    }

    private boolean completeGateway(Participant member, CompletableFuture<Bound> gateway,
                                    Optional<Gateway> futureSailor, HashMultiset<BootstrapTrust> trusts,
                                    Set<SignedNote> initialSeedSet, Digest v, int majority) {
        if (futureSailor.isEmpty()) {
            return true;
        }
        if (gateway.isDone()) {
            return false;
        }

        Gateway g = futureSailor.get();

        if (g.equals(Gateway.getDefaultInstance())) {
            return true;
        }
        if (g.getInitialSeedSetCount() == 0) {
            log.warn("No seeds in gateway returned from: {} on: {}", member.getId(), node.getId());
            return true;
        }

        if (g.getTrust().equals(BootstrapTrust.getDefaultInstance()) || g.getTrust()
                                                                         .getDiadem()
                                                                         .equals(HexBloome.getDefaultInstance())) {
            log.trace("Empty bootstrap trust in join returned from: {} on: {}", member.getId(), node.getId());
            return true;
        }
        trusts.add(g.getTrust());
        initialSeedSet.addAll(g.getInitialSeedSetList());
        log.trace("Initial seed set count: {} view: {} from: {} on: {}", g.getInitialSeedSetCount(), v, member.getId(),
                  node.getId());

        var trust = trusts.entrySet()
                          .stream()
                          .filter(e -> e.getCount() >= majority)
                          .map(e -> e.getElement())
                          .findFirst()
                          .orElse(null);
        if (trust != null) {
            validate(trust, gateway, initialSeedSet);
        } else {
            log.debug("Gateway received, trust count: {} majority: {} from: {} view: {} context: {} on: {}",
                      trusts.size(), majority, member.getId(), v, this.context.getId(), node.getId());
        }
        return true;
    }

    private Join join(Digest v) {
        return Join.newBuilder().setView(v.toDigeste()).setNote(node.getNote().getWrapped()).build();
    }

    private BiConsumer<? super Redirect, ? super Throwable> join(Duration duration, ScheduledExecutorService scheduler,
                                                                 Timer.Context timer) {
        return (r, t) -> {
            if (t != null) {
                log.error("Failed seeding on: {}", node.getId(), t);
                return;
            }
            if (!r.isInitialized()) {
                log.error("Empty seeding response on: {}", node.getId(), t);
                return;
            }
            var view = Digest.from(r.getView());
            log.info("Rebalancing to cardinality: {} (validate) for: {} context: {} on: {}", r.getCardinality(), view,
                     context.getId(), node.getId());
            this.context.rebalance(r.getCardinality());
            node.nextNote(view);

            log.debug("Completing redirect to view: {} context: {} successors: {} on: {}", view, this.context.getId(),
                      r.getSuccessorsCount(), node.getId());
            if (timer != null) {
                timer.close();
            }
            join(r, view, duration, scheduler);
        };
    }

    private void join(Redirect redirect, Digest v, Duration duration, ScheduledExecutorService scheduler) {
        var successors = redirect.getSuccessorsList()
                                 .stream()
                                 .map(sn -> new NoteWrapper(sn.getNote(), digestAlgo))
                                 .map(nw -> view.new Participant(nw))
                                 .collect(Collectors.toList());
        log.info("Redirecting to: {} context: {} successors: {} on: {}", v, this.context.getId(), successors.size(),
                 node.getId());
        var gateway = new CompletableFuture<Bound>();
        var timer = metrics == null ? null : metrics.joinDuration().time();
        gateway.whenComplete(view.join(scheduler, duration, timer));

        var regate = new AtomicReference<Runnable>();
        var retries = new AtomicInteger();

        HashMultiset<BootstrapTrust> trusts = HashMultiset.create();
        HashSet<SignedNote> initialSeedSet = new HashSet<>();

        final var cardinality = redirect.getCardinality();

        log.info("Rebalancing to cardinality: {} (join) for: {} context: {} on: {}", cardinality, v, context.getId(),
                 node.getId());
        this.context.rebalance(cardinality);
        node.nextNote(v);

        final var redirecting = new SliceIterator<>("Gateways", node, successors, approaches);
        var majority = redirect.getBootstrap() ? 1 : Context.minimalQuorum(redirect.getRings(), this.context.getBias());
        final var join = join(v);
        final var abandon = new AtomicInteger();
        regate.set(() -> {
            redirecting.iterate((link, m) -> {
                log.debug("Joining: {} contacting: {} on: {}", v, link.getMember().getId(), node.getId());
                try {
                    return link.join(join, params.seedingTimeout());
                } catch (StatusRuntimeException sre) {
                    if (sre.getStatus().getCode().equals(Status.OUT_OF_RANGE.getCode())) {
                        log.debug("Gateway view: {} invalid: {} from: {} on: {}", v, sre.getMessage(),
                                  link.getMember().getId(), node.getId());
                        abandon.incrementAndGet();
                    } else {
                        log.debug("Join view: {} error: {} from: {} on: {}", v, sre.getMessage(),
                                  link.getMember().getId(), node.getId());
                    }
                    return null;
                }
            }, (futureSailor, link, m) -> completeGateway((Participant) m, gateway, futureSailor, trusts,
                                                          initialSeedSet, v, majority), () -> {
                if (gateway.isDone()) {
                    return;
                }
                if (abandon.get() >= majority) {
                    log.debug("Abandoning Gateway view: {} reseeding on: {}", v, node.getId());
                    seeding();
                } else {
                    abandon.set(0);
                    if (retries.get() < params.joinRetries()) {
                        log.debug("Failed to join view: {} retry: {} out of: {} on: {}", v, retries.incrementAndGet(),
                                  params.joinRetries(), node.getId());
                        trusts.clear();
                        initialSeedSet.clear();
                        scheduler.schedule(Utils.wrapped(regate.get(), log),
                                           Entropy.nextBitsStreamLong(params.retryDelay().toNanos()),
                                           TimeUnit.NANOSECONDS);
                    } else {
                        log.error("Failed to join view: {} cannot obtain majority on: {}", view, node.getId());
                        view.stop();
                    }
                }
            }, scheduler, params.retryDelay());
        });
        regate.get().run();
    }

    private Registration registration() {
        return Registration.newBuilder()
                           .setView(view.currentView().toDigeste())
                           .setNote(node.note.getWrapped())
                           .build();
    }

    private NoteWrapper seedFor(Seed seed) {
        SignedNote seedNote = SignedNote.newBuilder()
                                        .setNote(Note.newBuilder()
                                                     .setHost(seed.endpoint().getHostName())
                                                     .setPort(seed.endpoint().getPort())
                                                     .setCoordinates(seed.coordinates().toEventCoords())
                                                     .setEpoch(-1)
                                                     .setMask(ByteString.copyFrom(
                                                     Node.createInitialMask(context).toByteArray())))
                                        .setSignature(
                                        SignatureAlgorithm.NULL_SIGNATURE.sign(ULong.MIN, null, new byte[0]).toSig())
                                        .build();
        return new NoteWrapper(seedNote, digestAlgo);
    }

    private void validate(BootstrapTrust trust, CompletableFuture<Bound> gateway, Set<SignedNote> initialSeedSet) {
        final var hexBloom = new HexBloom(trust.getDiadem());
        if (gateway.complete(
        new Bound(hexBloom, trust.getSuccessorsList().stream().map(sn -> new NoteWrapper(sn, digestAlgo)).toList(),
                  initialSeedSet.stream().map(sn -> new NoteWrapper(sn, digestAlgo)).toList()))) {
            log.info("Gateway acquired: {} context: {} on: {}", hexBloom.compact(), this.context.getId(), node.getId());
        }
    }

    record Bound(HexBloom view, List<NoteWrapper> successors, List<NoteWrapper> initialSeedSet) {
    }
}
