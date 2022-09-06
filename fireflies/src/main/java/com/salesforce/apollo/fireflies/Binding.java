/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import com.codahale.metrics.Timer;
import com.google.common.collect.HashMultiset;
import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.fireflies.proto.Credentials;
import com.salesfoce.apollo.fireflies.proto.Gateway;
import com.salesfoce.apollo.fireflies.proto.Join;
import com.salesfoce.apollo.fireflies.proto.Redirect;
import com.salesfoce.apollo.fireflies.proto.SignedNote;
import com.salesfoce.apollo.utils.proto.Biff;
import com.salesforce.apollo.comm.SliceIterator;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.fireflies.View.Participant;
import com.salesforce.apollo.fireflies.View.Seed;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;

import io.grpc.StatusRuntimeException;

/**
 * Embodiment of the client side join protocol
 *
 */
class Binding {

    private final Duration                 duration;
    private final ScheduledExecutorService scheduler;
    private final List<Seed>               seeds;
    private final View                     view;

    public Binding(View view, List<Seed> seeds, Duration duration, ScheduledExecutorService scheduler) {
        this.view = view;
        this.duration = duration;
        this.seeds = new ArrayList<>(seeds);
        this.scheduler = scheduler;
    }

    void seeding() {
        if (seeds.isEmpty()) {// This node is the bootstrap seed
            bootstrap();
            return;
        }
        Entropy.secureShuffle(seeds);
        View.log.info("Seeding view: {} context: {} with seeds: {} started on: {}", this.view.currentView(),
                      this.view.context.getId(), seeds.size(), this.view.node.getId());

        var seeding = new CompletableFuture<Redirect>();
        var timer = this.view.metrics == null ? null : this.view.metrics.seedDuration().time();
        seeding.whenComplete(redirect(duration, scheduler, timer));

        var seedlings = new SliceIterator<>("Seedlings", this.view.node,
                                            seeds.stream()
                                                 .map(s -> this.view.seedFor(s))
                                                 .map(nw -> this.view.new Participant(
                                                                                      nw))
                                                 .filter(p -> !this.view.node.getId().equals(p.getId()))
                                                 .collect(Collectors.toList()),
                                            this.view.approaches, this.view.exec);
        AtomicReference<Runnable> reseed = new AtomicReference<>();
        reseed.set(() -> {
            seedlings.iterate((link, m) -> {
                View.log.debug("Requesting Seeding from: {} on: {}", link.getMember().getId(), this.view.node.getId());
                return link.seed(credentials(this.view.bootstrapView()));
            }, (futureSailor, link, m) -> complete(seeding, futureSailor, m), () -> {
                if (!seeding.isDone()) {
                    scheduler.schedule(this.view.exec(() -> reseed.get().run()),
                                       this.view.params.retryDelay().toNanos(), TimeUnit.NANOSECONDS);
                }
            }, scheduler, this.view.params.retryDelay());
        });
        reseed.get().run();
    }

    private void bootstrap() {
        View.log.info("Bootstrapping seed node view: {} context: {} on: {}", this.view.currentView(),
                      this.view.context.getId(), this.view.node.getId());
        var nw = this.view.node.getNote();
        final var sched = scheduler;
        final var dur = duration;

        this.view.viewManagement.bootstrap(nw, sched, dur);
    }

    private boolean complete(CompletableFuture<Redirect> redirect, Optional<ListenableFuture<Redirect>> futureSailor,
                             Member m) {
        if (futureSailor.isEmpty()) {
            return true;
        }
        try {
            final var r = futureSailor.get().get();
            if (redirect.complete(r)) {
                View.log.info("Redirect to view: {} context: {} from: {} on: {}", Digest.from(r.getView()),
                              this.view.context.getId(), m.getId(), this.view.node.getId());
            }
            return false;
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof StatusRuntimeException sre) {
                switch (sre.getStatus().getCode()) {
                case RESOURCE_EXHAUSTED:
                    View.log.trace("SRE in redirect: {} on: {}", sre.getStatus(), this.view.node.getId());
                    break;
                default:
                    View.log.trace("SRE in redirect: {} on: {}", sre.getStatus(), this.view.node.getId());
                }
            } else {
                View.log.error("Error in redirect: {} on: {}", ex.getCause(), this.view.node.getId());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (CancellationException e) {
            // noop
        }
        return true;
    }

    private boolean completeGateway(Participant member, CompletableFuture<Bound> gateway,
                                    Optional<ListenableFuture<Gateway>> futureSailor, HashMultiset<Biff> memberships,
                                    HashMultiset<Digest> views, HashMultiset<Integer> cards, Set<SignedNote> seeds,
                                    Digest view, int majority, Set<SignedNote> joined) {
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
                    View.log.trace("Resource exhausted in join: {} with: {} : {} on: {}", view, member.getId(),
                                   sre.getStatus(), this.view.node.getId());
                    break;
                case OUT_OF_RANGE:
                    View.log.debug("View change in join: {} with: {} : {} on: {}", view, member.getId(),
                                   sre.getStatus(), this.view.node.getId());
                    this.view.viewManagement.resetBootstrapView();
                    this.view.node.reset();
                    this.view.exec.execute(() -> seeding());
                    return false;
                case DEADLINE_EXCEEDED:
                    View.log.trace("Join timeout for view: {} with: {} : {} on: {}", view, member.getId(),
                                   sre.getStatus(), this.view.node.getId());
                    break;
                case UNAUTHENTICATED:
                    View.log.trace("Join unauthenticated for view: {} with: {} : {} on: {}", view, member.getId(),
                                   sre.getStatus(), this.view.node.getId());
                    break;
                default:
                    View.log.warn("Failure in join: {} with: {} : {} on: {}", view, member.getId(), sre.getStatus(),
                                  this.view.node.getId());
                }
            } else {
                View.log.error("Failure in join: {} with: {} on: {}", view, member.getId(), this.view.node.getId(),
                               e.getCause());
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
            View.log.warn("No seeds in join returned from: {} on: {}", member.getId(), this.view.node.getId());
            return true;
        }
        if (g.getMembers().equals(Biff.getDefaultInstance())) {
            View.log.warn("No membership in join returned from: {} on: {}", member.getId(), this.view.node.getId());
            return true;
        }
        var gatewayView = Digest.from(g.getView());
        if (gatewayView.equals(Digest.NONE)) {
            View.log.trace("Empty view in join returned from: {} on: {}", member.getId(), this.view.node.getId());
            return true;
        }
        views.add(gatewayView);
        cards.add(g.getCardinality());
        memberships.add(g.getMembers());
        seeds.addAll(g.getInitialSeedSetList());
        joined.addAll(g.getJoiningList());

        var v = views.entrySet()
                     .stream()
                     .filter(e -> e.getCount() >= majority)
                     .map(e -> e.getElement())
                     .findFirst()
                     .orElse(null);
        if (v != null) {
            if (validate(g, v, gateway, memberships, cards, seeds, majority, joined)) {
                return false;
            }
        }
        View.log.debug("Gateway received, view count: {} cardinality: {} memberships: {} majority: {} from: {} view: {} context: {} on: {}",
                       views.size(), cards.size(), memberships.size(), majority, member.getId(), gatewayView,
                       this.view.context.getId(), this.view.node.getId());
        return true;
    }

    private Credentials credentials(Digest view) {
        var join = Join.newBuilder()
                       .setView(view.toDigeste())
                       .setNote(this.view.node.getNote().getWrapped())
                       .setKerl(this.view.node.kerl())
                       .build();
        var credentials = Credentials.newBuilder().setContext(this.view.context.getId().toDigeste()).build();
        return credentials;
    }

    private BiConsumer<? super Redirect, ? super Throwable> redirect(Duration duration,
                                                                     ScheduledExecutorService scheduler,
                                                                     Timer.Context timer) {
        return (r, t) -> {
            if (t != null) {
                View.log.error("Failed seeding on: {}", this.view.node.getId(), t);
                return;
            }
            if (!r.isInitialized()) {
                View.log.error("Empty seeding response on: {}", this.view.node.getId(), t);
                return;
            }
            var view = Digest.from(r.getView());
            this.view.context.rebalance(r.getCardinality());
            this.view.node.nextNote(view);

            View.log.debug("Completing redirect to view: {} context: {} successors: {} on: {}", view,
                           this.view.context.getId(), r.getSuccessorsCount(), this.view.node.getId());
            if (timer != null) {
                timer.close();
            }
            redirect(r, duration, scheduler);
        };
    }

    private void redirect(Redirect redirect, Duration duration, ScheduledExecutorService scheduler) {
        var view = Digest.from(redirect.getView());
        var succsesors = redirect.getSuccessorsList()
                                 .stream()
                                 .map(sn -> new NoteWrapper(sn.getNote(), this.view.digestAlgo))
                                 .map(nw -> this.view.new Participant(
                                                                      nw))
                                 .collect(Collectors.toList());
        View.log.info("Redirecting to: {} context: {} successors: {} on: {}", view, this.view.context.getId(),
                      succsesors.size(), this.view.node.getId());
        var gateway = new CompletableFuture<Bound>();
        var timer = this.view.metrics == null ? null : this.view.metrics.joinDuration().time();
        gateway.whenComplete(this.view.viewManagement.join(scheduler, duration, timer));

        var regate = new AtomicReference<Runnable>();
        var retries = new AtomicInteger();

        HashMultiset<Biff> biffs = HashMultiset.create();
        HashSet<SignedNote> seeds = new HashSet<>();
        HashMultiset<Digest> views = HashMultiset.create();
        HashMultiset<Integer> cards = HashMultiset.create();
        Set<SignedNote> joined = new HashSet<>();

        this.view.context.rebalance(redirect.getCardinality());
        this.view.node.nextNote(view);

        final var redirecting = new SliceIterator<>("Gateways", this.view.node, succsesors, this.view.approaches,
                                                    this.view.exec);
        var majority = redirect.getBootstrap() ? 1 : Context.minimalQuorum(redirect.getRings(),
                                                                           this.view.context.getBias());
        regate.set(() -> {
            redirecting.iterate((link, m) -> {
                View.log.debug("Joining: {} contacting: {} on: {}", view, link.getMember().getId(),
                               this.view.node.getId());
                return link.join(credentials(view), this.view.params.seedingTimeout());
            }, (futureSailor, link, m) -> completeGateway((Participant) m, gateway, futureSailor, biffs, views, cards,
                                                          seeds, view, majority, joined),
                                () -> {
                                    if (retries.get() < this.view.params.joinRetries()) {
                                        View.log.debug("Failed to join view: {} retry: {} out of: {} on: {}", view,
                                                       retries.incrementAndGet(), this.view.params.joinRetries(),
                                                       this.view.node.getId());
                                        biffs.clear();
                                        views.clear();
                                        cards.clear();
                                        seeds.clear();
                                        scheduler.schedule(this.view.exec(() -> regate.get().run()),
                                                           Entropy.nextBitsStreamLong(this.view.params.retryDelay()
                                                                                                      .toNanos()),
                                                           TimeUnit.NANOSECONDS);
                                    } else {
                                        View.log.error("Failed to join view: {} cannot obtain majority on: {}", view,
                                                       this.view.node.getId());
                                        this.view.stop();
                                    }
                                }, scheduler, this.view.params.retryDelay());
        });
        regate.get().run();
    }

    private boolean validate(Gateway g, Digest view, CompletableFuture<Bound> gateway, HashMultiset<Biff> memberships,
                             HashMultiset<Integer> cards, Set<SignedNote> seeds, int majority, Set<SignedNote> joined) {
        final var max = cards.entrySet()
                             .stream()
                             .filter(e -> e.getCount() >= majority)
                             .mapToInt(e -> e.getElement())
                             .findFirst();
        var cardinality = max.orElse(-1);
        if (cardinality > 0) {
            var members = memberships.entrySet()
                                     .stream()
                                     .filter(e -> e.getCount() >= majority)
                                     .map(e -> e.getElement())
                                     .findFirst()
                                     .orElse(null);
            if (members != null) {
                if (gateway.complete(new Bound(view,
                                               seeds.stream()
                                                    .map(sn -> new NoteWrapper(sn, this.view.digestAlgo))
                                                    .toList(),
                                               cardinality, joined, BloomFilter.from(g.getMembers())))) {
                    View.log.info("Gateway acquired: {} context: {} on: {}", view, this.view.context.getId(),
                                  this.view.node.getId());
                }
                return true;
            }
        }
        View.log.info("Gateway: {} majority not achieved: {} required: {} count: {} context: {} on: {}", view,
                      cardinality, majority, cards.size(), this.view.context.getId(), this.view.node.getId());
        return false;
    }
}
