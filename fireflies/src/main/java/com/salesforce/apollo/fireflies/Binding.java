/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import com.codahale.metrics.Timer;
import com.google.common.collect.HashMultiset;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.fireflies.proto.*;
import com.salesfoce.apollo.utils.proto.HexBloome;
import com.salesforce.apollo.archipelago.RouterImpl.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.HexBloom;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.fireflies.View.Node;
import com.salesforce.apollo.fireflies.View.Participant;
import com.salesforce.apollo.fireflies.View.Seed;
import com.salesforce.apollo.fireflies.View.Service;
import com.salesforce.apollo.fireflies.comm.entrance.Entrance;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.ring.SliceIterator;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.Utils;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
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
    private final static Logger log = LoggerFactory.getLogger(Binding.class);
    private final CommonCommunications<Entrance, Service> approaches;
    private final Context<Participant> context;
    private final DigestAlgorithm digestAlgo;
    private final Duration duration;
    private final Executor exec;
    private final FireflyMetrics metrics;
    private final Node node;
    private final Parameters params;
    private final ScheduledExecutorService scheduler;
    private final List<Seed> seeds;
    private final View view;
    public Binding(View view, List<Seed> seeds, Duration duration, ScheduledExecutorService scheduler,
                   Context<Participant> context, CommonCommunications<Entrance, Service> approaches, Node node,
                   Parameters params, FireflyMetrics metrics, Executor exec, DigestAlgorithm digestAlgo) {
        this.view = view;
        this.duration = duration;
        this.seeds = new ArrayList<>(seeds);
        this.scheduler = scheduler;
        this.context = context;
        this.node = node;
        this.params = params;
        this.metrics = metrics;
        this.exec = exec;
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

        var seedlings = new SliceIterator<>("Seedlings", node,
                seeds.stream()
                        .map(s -> seedFor(s))
                        .map(nw -> view.new Participant(
                                nw))
                        .filter(p -> !node.getId().equals(p.getId()))
                        .collect(Collectors.toList()),
                approaches, exec);
        AtomicReference<Runnable> reseed = new AtomicReference<>();
        reseed.set(() -> {
            final var registration = registration();
            seedlings.iterate((link, m) -> {
                log.debug("Requesting Seeding from: {} on: {}", link.getMember().getId(), node.getId());
                return link.seed(registration);
            }, (futureSailor, link, m) -> complete(seeding, futureSailor, m), () -> {
                if (!seeding.isDone()) {
                    scheduler.schedule(exec(() -> reseed.get().run()), params.retryDelay().toNanos(),
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
        final var sched = scheduler;
        final var dur = duration;

        view.bootstrap(nw, sched, dur);
    }

    private boolean complete(CompletableFuture<Redirect> redirect, Optional<ListenableFuture<Redirect>> futureSailor,
                             Member m) {
        if (futureSailor.isEmpty()) {
            return true;
        }
        try {
            final var r = futureSailor.get().get();
            if (redirect.complete(r)) {
                log.info("Redirect to view: {} context: {} from: {} on: {}", Digest.from(r.getView()),
                        this.context.getId(), m.getId(), node.getId());
            }
            return false;
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof StatusRuntimeException sre) {
                switch (sre.getStatus().getCode()) {
                    case RESOURCE_EXHAUSTED:
                        log.trace("SRE in redirect: {} on: {}", sre.getStatus(), node.getId());
                        break;
                    default:
                        log.trace("SRE in redirect: {} on: {}", sre.getStatus(), node.getId());
                }
            } else {
                log.error("Error in redirect: {} on: {}", ex.getCause(), node.getId());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (CancellationException e) {
            // noop
        }
        return true;
    }

    private boolean completeGateway(Participant member, CompletableFuture<Bound> gateway,
                                    Optional<ListenableFuture<Gateway>> futureSailor, HashMultiset<HexBloome> diadems,
                                    Set<SignedNote> initialSeedSet, Digest v, int majority) {
        if (futureSailor.isEmpty()) {
            return true;
        }
        if (gateway.isDone()) {
            return false;
        }

        Gateway g;
        try {
            g = futureSailor.get().get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof StatusRuntimeException sre) {
                switch (sre.getStatus().getCode()) {
                    case RESOURCE_EXHAUSTED:
                        log.trace("Resource exhausted in join: {} with: {} : {} on: {}", v, member.getId(), sre.getStatus(),
                                node.getId());
                        break;
                    case OUT_OF_RANGE:
                        log.debug("View change in join: {} with: {} : {} on: {}", v, member.getId(), sre.getStatus(),
                                node.getId());
                        view.resetBootstrapView();
                        node.reset();
                        exec.execute(() -> seeding());
                        return false;
                    case DEADLINE_EXCEEDED:
                        log.trace("Join timeout for view: {} with: {} : {} on: {}", v, member.getId(), sre.getStatus(),
                                node.getId());
                        break;
                    case UNAUTHENTICATED:
                        log.trace("Join unauthenticated for view: {} with: {} : {} on: {}", v, member.getId(),
                                sre.getStatus(), node.getId());
                        break;
                    default:
                        log.warn("Failure in join: {} with: {} : {} on: {}", v, member.getId(), sre.getStatus(),
                                node.getId());
                }
            } else {
                log.error("Failure in join: {} with: {} on: {}", v, member.getId(), node.getId(), e.getCause());
            }
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (CancellationException e) {
            return true;
        }

        if (g.equals(Gateway.getDefaultInstance())) {
            return true;
        }
        if (g.getInitialSeedSetCount() == 0) {
            log.warn("No seeds in gateway returned from: {} on: {}", member.getId(), node.getId());
            return true;
        }

        if (g.getDiadem().equals(HexBloome.getDefaultInstance())) {
            log.trace("Empty view in join returned from: {} on: {}", member.getId(), node.getId());
            return true;
        }
        diadems.add(g.getDiadem());
        initialSeedSet.addAll(g.getInitialSeedSetList());
        log.trace("Initial seed set count: {} view: {} from: {} on: {}", g.getInitialSeedSetCount(), v, member.getId(),
                node.getId());

        var vs = diadems.entrySet()
                .stream()
                .filter(e -> e.getCount() >= majority)
                .map(e -> e.getElement())
                .findFirst()
                .orElse(null);
        if (vs != null) {
            if (validate(v, g, gateway, diadems, initialSeedSet, majority)) {
                return false;
            }
        }
        log.debug("Gateway received, view count: {} majority: {} from: {} view: {} context: {} on: {}", diadems.size(),
                majority, member.getId(), v, this.context.getId(), node.getId());
        return true;
    }

    private Runnable exec(Runnable action) {
        return () -> exec.execute(Utils.wrapped(action, log));
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
                .map(nw -> view.new Participant(
                        nw))
                .collect(Collectors.toList());
        log.info("Redirecting to: {} context: {} successors: {} on: {}", v, this.context.getId(), successors.size(),
                node.getId());
        var gateway = new CompletableFuture<Bound>();
        var timer = metrics == null ? null : metrics.joinDuration().time();
        gateway.whenComplete(view.join(scheduler, duration, timer));

        var regate = new AtomicReference<Runnable>();
        var retries = new AtomicInteger();

        HashMultiset<HexBloome> diadems = HashMultiset.create();
        HashSet<SignedNote> initialSeedSet = new HashSet<>();

        final var cardinality = redirect.getCardinality();

        log.info("Rebalancing to cardinality: {} (join) for: {} context: {} on: {}", cardinality, v, context.getId(),
                node.getId());
        this.context.rebalance(cardinality);
        node.nextNote(v);

        final var redirecting = new SliceIterator<>("Gateways", node, successors, approaches, exec);
        var majority = redirect.getBootstrap() ? 1 : Context.minimalQuorum(redirect.getRings(), this.context.getBias());
        final var join = join(v);
        regate.set(() -> {
            redirecting.iterate((link, m) -> {
                        log.debug("Joining: {} contacting: {} on: {}", v, link.getMember().getId(), node.getId());
                        return link.join(join, params.seedingTimeout());
                    }, (futureSailor, link, m) -> completeGateway((Participant) m, gateway, futureSailor, diadems,
                            initialSeedSet, v, majority),
                    () -> {
                        if (retries.get() < params.joinRetries()) {
                            log.debug("Failed to join view: {} retry: {} out of: {} on: {}", v,
                                    retries.incrementAndGet(), params.joinRetries(), node.getId());
                            diadems.clear();
                            initialSeedSet.clear();
                            scheduler.schedule(exec(() -> regate.get().run()),
                                    Entropy.nextBitsStreamLong(params.retryDelay().toNanos()),
                                    TimeUnit.NANOSECONDS);
                        } else {
                            log.error("Failed to join view: {} cannot obtain majority on: {}", view,
                                    node.getId());
                            view.stop();
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
                        .setMask(ByteString.copyFrom(Node.createInitialMask(context)
                                .toByteArray())))
                .setSignature(SignatureAlgorithm.NULL_SIGNATURE.sign(null, new byte[0]).toSig())
                .build();
        return new NoteWrapper(seedNote, digestAlgo);
    }

    private boolean validate(Digest v, Gateway g, CompletableFuture<Bound> gateway, HashMultiset<HexBloome> hexes,
                             Set<SignedNote> successors, int majority) {
        final var max = hexes.entrySet()
                .stream()
                .filter(e -> e.getCount() >= majority)
                .map(e -> e.getElement())
                .findFirst();
        var hex = max.orElse(null);
        if (hex != null) {
            final var hexBloom = new HexBloom(hex);
            if (gateway.complete(new Bound(hexBloom,
                    successors.stream().map(sn -> new NoteWrapper(sn, digestAlgo)).toList()))) {
                log.info("Gateway acquired: {} context: {} on: {}", hexBloom.compact(), this.context.getId(),
                        node.getId());
            }
            return true;
        }
        log.info("Gateway: {} majority not achieved: {} context: {} on: {}", v, majority, this.context.getId(),
                node.getId());
        return false;
    }

    record Bound(HexBloom view, List<NoteWrapper> successors) {
    }
}
