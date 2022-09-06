/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.common.base.Objects;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multiset.Entry;
import com.google.common.collect.Ordering;
import com.salesfoce.apollo.fireflies.proto.Gateway;
import com.salesfoce.apollo.fireflies.proto.Join;
import com.salesfoce.apollo.fireflies.proto.JoinGossip;
import com.salesfoce.apollo.fireflies.proto.Redirect;
import com.salesfoce.apollo.fireflies.proto.Registration;
import com.salesfoce.apollo.fireflies.proto.SignedNote;
import com.salesfoce.apollo.fireflies.proto.SignedViewChange;
import com.salesfoce.apollo.fireflies.proto.ViewChange;
import com.salesfoce.apollo.utils.proto.Biff;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.fireflies.View.Participant;
import com.salesforce.apollo.membership.ReservoirSampler;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.Utils;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class ViewManagement {
    private record Ballot(Digest view, List<Digest> leaving, List<Digest> joining, int hash) {

        private Ballot(Digest view, List<Digest> leaving, List<Digest> joining, DigestAlgorithm algo) {
            this(view, leaving, joining,
                 view.xor(leaving.stream()
                                 .reduce((a, b) -> a.xor(b))
                                 .orElse(algo.getOrigin())
                                 .xor(joining.stream().reduce((a, b) -> a.xor(b)).orElse(algo.getOrigin())))
                     .hashCode());
        }

        @Override
        public boolean equals(Object obj) {
            if (obj != null && obj instanceof Ballot b) {
                return Objects.equal(view, b.view) && Objects.equal(leaving, b.leaving) &&
                       Objects.equal(joining, b.joining);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public String toString() {
            return String.format("{h: %s, j: %s, l: %s}", hash, joining.size(), leaving.size());
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ViewManagement.class);

    boolean                                                bootstrap;
    final AtomicReference<Digest>                          currentView  = new AtomicReference<>();
    final ConcurrentMap<Digest, NoteWrapper>               joins        = new ConcurrentSkipListMap<>();
    CompletableFuture<Void>                                onJoined;
    private final AtomicInteger                            attempt      = new AtomicInteger();
    private final AtomicReference<Digest>                  crown;
    private final AtomicBoolean                            joined       = new AtomicBoolean();
    private final Map<Digest, Consumer<List<NoteWrapper>>> pendingJoins = new ConcurrentSkipListMap<>();
    private final View                                     view;
    private final AtomicReference<ViewChange>              vote         = new AtomicReference<>();

    ViewManagement(View view) {
        this.view = view;
        crown = new AtomicReference<>(view.digestAlgo.getOrigin());
    }

    public Digest currentView() {
        return currentView.get();
    }

    void bootstrap(NoteWrapper nw, final ScheduledExecutorService sched, final Duration dur) {
        joins.put(nw.getId(), nw);
        view.context.activate(view.node);

        crown.set(view.digestAlgo.getOrigin());
        view.viewChange(() -> install(new Ballot(view.currentView(), Collections.emptyList(),
                                                 Collections.singletonList(view.node.getId()), view.digestAlgo)));

        scheduleViewChange();

        view.futureGossip = sched.schedule(Utils.wrapped(() -> view.gossip(dur, sched), log),
                                           Entropy.nextBitsStreamLong(dur.toNanos()), TimeUnit.NANOSECONDS);

        log.info("Bootstrapped view: {} cardinality: {} count: {} context: {} on: {}", view.currentView(),
                 view.context.cardinality(), view.context.activeCount(), view.context.getId(), view.node.getId());
        onJoined.complete(null);
        joined.set(true);
        if (view.metrics != null) {
            view.metrics.viewChanges().mark();
        }
    }

    void clear() {
        joins.clear();
        resetBootstrapView();
    }

    /**
     * @param seed
     * @param p
     * @return the bloom filter containing the digests of known joins
     */
    BloomFilter<Digest> getJoinsBff(long seed, double p) {
        BloomFilter<Digest> bff = new BloomFilter.DigestBloomFilter(seed, Math.max(view.params.minimumBiffCardinality(),
                                                                                   joins.size() * 2),
                                                                    p);
        joins.keySet().forEach(d -> bff.add(d));
        return bff;
    }

    /**
     * Formally join the view. Calculate the HEX-BLOOM crown and view, fail and stop
     * if does not match currentView
     */
    synchronized void join() {
        assert view.context.totalCount() == view.context.cardinality();
        if (onJoined.isDone()) {
            return;
        }
        crown.set(view.digestAlgo.getOrigin());
        view.context.allMembers().forEach(p -> crown.accumulateAndGet(p.id, (a, b) -> a.xor(b)));
        if (!currentView().equals(crown.get().rehash())) {
            log.error("Crown: {} does not produce view: {} cardinality: {} count: {} on: {}", crown.get(),
                      currentView(), view.context.cardinality(), view.context.totalCount(), view.node.getId());
            view.stop();
            throw new IllegalStateException("Invalid crown");
        }
        view.notifyListeners(view.context.allMembers().map(p -> p.getId()).toList(), Collections.emptyList());
        onJoined.complete(null);
        joined.set(true);

        scheduleViewChange();

        if (view.metrics != null) {
            view.metrics.viewChanges().mark();
        }
        log.info("Joined view: {} cardinality: {} count: {} on: {}", currentView(), view.context.cardinality(),
                 view.context.totalCount(), view.node.getId());
    }

    void join(Join join, Digest from, StreamObserver<Gateway> responseObserver, Timer.Context timer) {
        if (!joined.get()) {
            log.trace("Not joined, ignored join of view: {} from: {} on: {}", Digest.from(join.getView()), from,
                      view.node.getId());
            responseObserver.onNext(Gateway.getDefaultInstance());
            responseObserver.onCompleted();
            return;
        }
        view.stable(() -> {
            var note = new NoteWrapper(join.getNote(), view.digestAlgo);
            if (!from.equals(note.getId())) {
                responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Member not match note")));
                return;
            }
            var requestView = Digest.from(join.getView());
            final var current = view.membership;
            if (current.contains(from)) {
                log.trace("Already a member: {} view: {}  context: {} cardinality: {} on: {}", from, currentView(),
                          view.context.getId(), view.context.cardinality(), view.node.getId());
                joined(Collections.emptyList(), from, responseObserver, timer);
                return;
            }
            if (!currentView().equals(requestView)) {
                responseObserver.onError(new StatusRuntimeException(Status.OUT_OF_RANGE.withDescription("View: "
                + requestView + " does not match: " + currentView())));
                return;
            }
            if (pendingJoins.size() >= view.params.maxPending()) {
                responseObserver.onError(new StatusRuntimeException(Status.RESOURCE_EXHAUSTED.withDescription("No room at the inn")));
                return;
            }
            pendingJoins.put(from, joining -> {
                log.info("Gateway established for: {} view: {}  context: {} cardinality: {} on: {}", from,
                         currentView(), view.context.getId(), view.context.cardinality(), view.node.getId());
                joined(joining.stream().map(nw -> nw.getWrapped()).limit(view.params.maximumTxfr()).toList(), from,
                       responseObserver, timer);
            });
            joins.put(note.getId(), note);
            log.debug("Member pending join: {} view: {} context: {} on: {}", from, currentView(), view.context.getId(),
                      view.node.getId());
        });
    }

    BiConsumer<? super Bound, ? super Throwable> join(ScheduledExecutorService scheduler, Duration duration,
                                                      Timer.Context timer) {
        return (bound, t) -> {
            view.viewChange(() -> {
                if (t != null) {
                    log.error("Failed to join view: {}on: {}", bound.view(), view.node.getId(), t);
                    view.stop();
                    return;
                }

                view.context.rebalance(bound.cardinality());
                view.context.activate(view.node);

                view.membership = bound.bff();
                currentView.set(bound.view());

                bound.seeds().forEach(nw -> view.addToView(nw));
                bound.joined().forEach(nw -> view.addToView(new NoteWrapper(nw, view.digestAlgo)));

                view.gossiper.reset();
                view.roundTimers.setRoundDuration(view.context.timeToLive());
                view.node.nextNote();

                view.context.allMembers().forEach(p -> p.clearAccusations());

                view.futureGossip = scheduler.schedule(() -> view.gossip(duration, scheduler),
                                                       Entropy.nextBitsStreamLong(view.params.retryDelay().toNanos()),
                                                       TimeUnit.NANOSECONDS);

                if (timer != null) {
                    timer.stop();
                }
                joined.set(true);

                log.info("Currently joining view: {} seeds: {} cardinality: {} count: {} on: {}", bound.view(),
                         bound.seeds().size(), view.context.cardinality(), view.context.totalCount(),
                         view.node.getId());
                if (view.context.totalCount() == view.context.cardinality()) {
                    join();
                }
            });
        };
    }

    JoinGossip.Builder processJoins(BloomFilter<Digest> bff) {
        JoinGossip.Builder builder = JoinGossip.newBuilder();

        // Add all updates that this view has that aren't reflected in the inbound bff
        joins.entrySet()
             .stream()
             .filter(m -> !bff.contains(m.getKey()))
             .map(m -> m.getValue())
             .collect(new ReservoirSampler<>(view.params.maximumTxfr(), Entropy.bitsStream()))
             .forEach(n -> builder.addUpdates(n.getWrapped()));
        return builder;
    }

    /**
     * Process the inbound joins from the gossip. Reconcile the differences between
     * the view's state and the digests of the gossip. Update the reply with the
     * list of digests the view requires, as well as proposed updates based on the
     * inbound digests that the view has more recent information
     *
     * @param p
     * @param from
     * @param digests
     */
    JoinGossip processJoins(BloomFilter<Digest> bff, double p) {
        JoinGossip.Builder builder = processJoins(bff);
        builder.setBff(getJoinsBff(Entropy.nextSecureLong(), p).toBff());
        JoinGossip gossip = builder.build();
        if (builder.getUpdatesCount() != 0) {
            log.trace("process joins produced updates: {} on: {}", builder.getUpdatesCount(), view.node.getId());
        }
        return gossip;
    }

    void resetBootstrapView() {
        crown.set(view.digestAlgo.getOrigin());
        currentView.set(view.bootstrapView());
    }

    Redirect seed(Registration registration, Digest from) {
        final var requestView = Digest.from(registration.getView());

        if (!joined.get()) {
            log.warn("Not joined, ignored seed view: {} from: {} on: {}", requestView, from, view.node.getId());
            return Redirect.getDefaultInstance();
        }
        if (!view.bootstrapView().equals(requestView)) {
            log.warn("Invalid bootstrap view: {} from: {} on: {}", requestView, from, view.node.getId());
            return Redirect.getDefaultInstance();
        }
        var note = new NoteWrapper(registration.getNote(), view.digestAlgo);
        if (!from.equals(note.getId())) {
            log.warn("Invalid bootstrap note: {} from: {} claiming: {} on: {}", requestView, from, note.getId(),
                     view.node.getId());
            return Redirect.getDefaultInstance();
        }
        return view.stable(() -> {
            var newMember = view.newParticipant(note.getId());
            final var successors = new TreeSet<Participant>(view.context.successors(newMember,
                                                                                    m -> view.context.isActive(m)));

            log.debug("Member seeding: {} view: {} context: {} successors: {} on: {}", newMember.getId(), currentView(),
                      view.context.getId(), successors.size(), view.node.getId());
            return Redirect.newBuilder()
                           .setView(currentView().toDigeste())
                           .addAllSuccessors(successors.stream().filter(p -> p != null).map(p -> p.getSeed()).toList())
                           .setCardinality(view.context.cardinality())
                           .setBootstrap(bootstrap)
                           .setRings(view.context.getRingCount())
                           .build();
        });
    }

    /**
     * Finalize the view change
     */
    private void finalizeViewChange() {
        view.viewChange(() -> {
            final var cardinality = view.context.memberCount();
            final var superMajority = cardinality - ((cardinality - 1) / 4);
            if (view.observations.size() < superMajority) {
                log.trace("Do not have supermajority: {} required: {} local joins: {} leaving: {} for: {} on: {}",
                          view.observations.size(), superMajority, joins.size(), view.context.offlineCount(),
                          view.currentView(), view.node.getId());
                scheduleFinalizeViewChange(2);
                return;
            }
            HashMultiset<Ballot> ballots = HashMultiset.create();
            view.observations.values().forEach(vc -> {
                final var leaving = new ArrayList<>(vc.getChange()
                                                      .getLeavesList()
                                                      .stream()
                                                      .map(d -> Digest.from(d))
                                                      .collect(Collectors.toSet()));
                final var joining = new ArrayList<>(vc.getChange()
                                                      .getJoinsList()
                                                      .stream()
                                                      .map(d -> Digest.from(d))
                                                      .collect(Collectors.toSet()));
                leaving.sort(Ordering.natural());
                joining.sort(Ordering.natural());
                ballots.add(new Ballot(Digest.from(vc.getChange().getCurrent()), leaving, joining, view.digestAlgo));
            });
            var max = ballots.entrySet()
                             .stream()
                             .max(Ordering.natural().onResultOf(Multiset.Entry::getCount))
                             .orElse(null);
            if (max != null && max.getCount() >= superMajority) {
                log.info("Fast path consensus successful: {} required: {} cardinality: {} for: {} on: {}", max,
                         superMajority, view.context.cardinality(), view.currentView(), view.node.getId());
                install(max.getElement());
            } else {
                @SuppressWarnings("unchecked")
                final var reversed = Comparator.comparing(e -> ((Entry<Ballot>) e).getCount()).reversed();
                log.info("Fast path consensus failed: {}, required: {} cardinality: {} ballots: {} for: {} on: {}",
                         view.observations.size(), superMajority, view.context.cardinality(),
                         ballots.entrySet().stream().sorted(reversed).limit(1).toList(), view.currentView(),
                         view.node.getId());
            }

            scheduleViewChange();
            view.timers.remove(View.FINALIZE_VIEW_CHANGE);
            vote.set(null);
            view.observations.clear();
        });
    }

    /**
     * Initiate the view change
     */
    private void initiateViewChange() {
        view.stable(() -> {
            if (vote.get() != null) {
                log.trace("Vote already cast for: {} on: {}", view.currentView(), view.node.getId());
                return;
            }
            // Use pending rebuttals as a proxy for stability
            if (!view.pendingRebuttals.isEmpty()) {
                log.debug("Pending rebuttals: {} view: {} on: {}", view.pendingRebuttals.size(), view.currentView(),
                          view.node.getId());
                scheduleViewChange(1); // 1 TTL round to check again
                return;
            }
            scheduleFinalizeViewChange();
            final var builder = ViewChange.newBuilder()
                                          .setObserver(view.node.getId().toDigeste())
                                          .setCurrent(view.currentView().toDigeste())
                                          .setAttempt(attempt.getAndIncrement())
                                          .addAllLeaves(view.shunned.stream()
                                                                    .map(id -> id.toDigeste())
                                                                    .collect(Collectors.toSet()))
                                          .addAllJoins(joins.keySet().stream().map(id -> id.toDigeste()).toList());
            ViewChange change = builder.build();
            vote.set(change);
            var signature = view.node.sign(change.toByteString());
            view.observations.put(view.node.getId(),
                                  SignedViewChange.newBuilder()
                                                  .setChange(change)
                                                  .setSignature(signature.toSig())
                                                  .build());
            log.info("View change vote: {} joins: {} leaves: {} on: {}", view.currentView(), change.getJoinsCount(),
                     change.getLeavesCount(), view.node.getId());
        });
    }

    /**
     * Install the new view
     * 
     * @param view
     */
    private void install(Ballot ballot) {
        log.debug("View change: {}, pending: {} joining: {} leaving: {} local joins: {} leaving: {} on: {}",
                  currentView(), pendingJoins.size(), ballot.joining.size(), ballot.leaving.size(), joins.size(),
                  view.context.offlineCount(), view.node.getId());
        attempt.set(0);
        ballot.leaving.stream().filter(d -> !view.node.getId().equals(d)).forEach(p -> view.remove(p));
        view.context.rebalance(view.context.totalCount() + ballot.joining.size());

        // Compute the new membership
        view.membership = new DigestBloomFilter(view.currentView().fold(),
                                                Math.max(view.params.minimumBiffCardinality(),
                                                         (view.context.memberCount() + ballot.joining.size())),
                                                View.MEMBERSHIP_FPR);
        view.context.allMembers().forEach(p -> view.membership.add(p.getId()));
        ballot.joining().forEach(id -> view.membership.add(id));

        // Gather joining notes and members joining on this node
        var joiningNotes = new ArrayList<NoteWrapper>();
        var pending = ballot.joining()
                            .stream()
                            .map(d -> joins.remove(d))
                            .filter(sn -> sn != null)
                            .peek(sn -> joiningNotes.add(sn))
                            .peek(nw -> view.addToView(nw))
                            .peek(nw -> {
                                if (view.metrics != null) {
                                    view.metrics.joins().mark();
                                }
                            })
                            .map(nw -> pendingJoins.remove(nw.getId()))
                            .filter(p -> p != null)
                            .toList();

        // The circle of life
        var previousView = view.currentView();

        // HEX-BLOOM authentication maintenance
        ballot.joining.forEach(d -> crown.accumulateAndGet(d, (a, b) -> a.xor(b)));
        ballot.leaving.forEach(d -> crown.accumulateAndGet(d, (a, b) -> a.xor(b)));
        currentView.set(crown.get().rehash());

        // Tune
        view.gossiper.reset();
        view.roundTimers.setRoundDuration(view.context.timeToLive());

        // Regenerate for new epoch
        view.node.nextNote();

        // complete all pending joins
        pending.forEach(r -> {
            try {
                final var shuffled = new ArrayList<>(joiningNotes);
                Entropy.secureShuffle(shuffled);
                r.accept(shuffled);
            } catch (Throwable t) {
                log.error("Exception in pending join on: {}", view.node.getId(), t);
            }
        });
        if (view.metrics != null) {
            view.metrics.viewChanges().mark();
        }

        log.info("Installed view: {} from: {} crown: {} for context: {} cardinality: {} count: {} pending: {} leaving: {} joining: {} on: {}",
                 currentView(), previousView, crown.get(), view.context.getId(), view.context.cardinality(),
                 view.context.allMembers().count(), pending.size(), ballot.leaving.size(), ballot.joining.size(),
                 view.node.getId());

        view.notifyListeners(ballot.joining, ballot.leaving);
    }

    private void joined(List<SignedNote> joined, Digest from, StreamObserver<Gateway> responseObserver,
                        Timer.Context timer) {
        var seedSet = new TreeSet<Participant>();
        view.context.successors(from, m -> view.context.isActive(m)).forEach(p -> seedSet.add(p));
        final var current = view.membership;
        var gateway = Gateway.newBuilder()
                             .setView(currentView().toDigeste())
                             .addAllInitialSeedSet(seedSet.stream()
                                                          .map(p -> p.getNote().getWrapped())
                                                          .collect(new ReservoirSampler<>(view.params.maximumTxfr(),
                                                                                          Entropy.bitsStream())))
                             .setMembers(current.toBff())
                             .setCardinality(view.context.cardinality())
                             .addAllJoining(joined.stream()
                                                  .collect(new ReservoirSampler<>(view.params.maximumTxfr(),
                                                                                  Entropy.bitsStream())))
                             .build();
        assert gateway.getInitialSeedSetCount() != 0 : "No seeds";
        assert !gateway.getMembers().equals(Biff.getDefaultInstance()) : "Empty membership";
        responseObserver.onNext(gateway);
        responseObserver.onCompleted();
        if (timer != null) {
            var serializedSize = gateway.getSerializedSize();
            view.metrics.outboundBandwidth().mark(serializedSize);
            view.metrics.outboundGateway().update(serializedSize);
            timer.stop();
        }
    }

    /**
     * start a view change if there's any offline members or joining members
     */
    private void maybeViewChange() {
        if (view.context.offlineCount() > 0 || joins.size() > 0) {
            initiateViewChange();
        } else {
            scheduleViewChange();
        }
    }

    private void scheduleFinalizeViewChange() {
        scheduleFinalizeViewChange(view.params.finalizeViewRounds());
    }

    private void scheduleFinalizeViewChange(final int finalizeViewRounds) {
//        log.trace("View change finalization scheduled: {} rounds for: {} joining: {} leaving: {} on: {}",
//                  finalizeViewRounds, currentView(), joins.size(), view.context.getOffline().size(), view.node.getId());
        view.timers.put(View.FINALIZE_VIEW_CHANGE,
                        view.roundTimers.schedule(View.FINALIZE_VIEW_CHANGE, () -> finalizeViewChange(),
                                                  finalizeViewRounds));
    }

    private void scheduleViewChange() {
        scheduleViewChange(view.params.viewChangeRounds());
    }

    private void scheduleViewChange(final int viewChangeRounds) {
//        log.trace("Schedule view change: {} rounds for: {}   on: {}", viewChangeRounds, currentView(),
//                  view.node.getId());
        view.timers.put(View.SCHEDULED_VIEW_CHANGE,
                        view.roundTimers.schedule(View.SCHEDULED_VIEW_CHANGE, () -> maybeViewChange(),
                                                  viewChangeRounds));
    }
}
