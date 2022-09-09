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
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
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
import com.google.protobuf.Timestamp;
import com.salesfoce.apollo.fireflies.proto.Gateway;
import com.salesfoce.apollo.fireflies.proto.Join;
import com.salesfoce.apollo.fireflies.proto.JoinGossip;
import com.salesfoce.apollo.fireflies.proto.Nonce;
import com.salesfoce.apollo.fireflies.proto.Redirect;
import com.salesfoce.apollo.fireflies.proto.Registration;
import com.salesfoce.apollo.fireflies.proto.SignedNonce;
import com.salesfoce.apollo.fireflies.proto.SignedNote;
import com.salesfoce.apollo.fireflies.proto.SignedViewChange;
import com.salesfoce.apollo.fireflies.proto.Update.Builder;
import com.salesfoce.apollo.fireflies.proto.ViewChange;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.utils.proto.Biff;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.fireflies.View.Node;
import com.salesforce.apollo.fireflies.View.Participant;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.ReservoirSampler;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * 
 * Management of the view state logic
 *
 * @author hal.hildebrand
 *
 */
public class ViewManagement {
    record Ballot(Digest view, List<Digest> leaving, List<Digest> joining, int hash) {

        Ballot(Digest view, List<Digest> leaving, List<Digest> joining, DigestAlgorithm algo) {
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

    static final double MEMBERSHIP_FPR = 0.0000125;

    private static final Logger log = LoggerFactory.getLogger(ViewManagement.class);

    private static <T> CompletableFuture<T> complete(T value) {
        var fs = new CompletableFuture<T>();
        fs.complete(value);
        return fs;
    }

    private final AtomicInteger                            attempt      = new AtomicInteger();
    private boolean                                        bootstrap;
    private final Context<Participant>                     context;
    private final AtomicReference<Digest>                  crown;
    private final AtomicReference<Digest>                  currentView  = new AtomicReference<>();
    private final DigestAlgorithm                          digestAlgo;
    private final AtomicBoolean                            joined       = new AtomicBoolean();
    private final ConcurrentMap<Digest, NoteWrapper>       joins        = new ConcurrentSkipListMap<>();
    private final FireflyMetrics                           metrics;
    private final Node                                     node;
    private CompletableFuture<Void>                        onJoined;
    private final Parameters                               params;
    private final Map<Digest, Consumer<List<NoteWrapper>>> pendingJoins = new ConcurrentSkipListMap<>();

    private final View view;

    private final AtomicReference<ViewChange> vote = new AtomicReference<>();

    ViewManagement(View view, Context<Participant> context, Parameters params, FireflyMetrics metrics, Node node,
                   DigestAlgorithm digestAlgo) {
        this.node = node;
        this.view = view;
        this.context = context;
        this.params = params;
        this.metrics = metrics;
        this.digestAlgo = digestAlgo;
        crown = new AtomicReference<>(digestAlgo.getOrigin());
    }

    boolean addJoin(Digest id, NoteWrapper note) {
        return joins.put(id, note) == null;
    }

    void bootstrap(NoteWrapper nw, final ScheduledExecutorService sched, final Duration dur) {
        joins.put(nw.getId(), nw);
        context.activate(node);

        crown.set(digestAlgo.getOrigin());
        view.viewChange(() -> install(new Ballot(view.currentView(), Collections.emptyList(),
                                                 Collections.singletonList(node.getId()), digestAlgo)));

        view.scheduleViewChange();
        view.schedule(dur, sched);

        log.info("Bootstrapped view: {} cardinality: {} count: {} context: {} on: {}", view.currentView(),
                 context.cardinality(), context.activeCount(), context.getId(), node.getId());
        onJoined.complete(null);
        joined.set(true);
        if (metrics != null) {
            metrics.viewChanges().mark();
        }
    }

    void clear() {
        joins.clear();
        resetBootstrapView();
    }

    void clearVote() {
        vote.set(null);
    }

    Digest currentView() {
        return currentView.get();
    }

    /**
     * @param seed
     * @param p
     * @return the bloom filter containing the digests of known joins
     */
    BloomFilter<Digest> getJoinsBff(long seed, double p) {
        BloomFilter<Digest> bff = new BloomFilter.DigestBloomFilter(seed, Math.max(params.minimumBiffCardinality(),
                                                                                   joins.size() * 2),
                                                                    p);
        joins.keySet().forEach(d -> bff.add(d));
        return bff;
    }

    /**
     * Install the new view
     * 
     * @param view
     */
    void install(Ballot ballot) {
        log.debug("View change: {}, pending: {} joining: {} leaving: {} local joins: {} leaving: {} on: {}",
                  currentView(), pendingJoins.size(), ballot.joining.size(), ballot.leaving.size(), joins.size(),
                  context.offlineCount(), node.getId());
        attempt.set(0);
        ballot.leaving.stream().filter(d -> !node.getId().equals(d)).forEach(p -> view.remove(p));
        context.rebalance(context.totalCount() + ballot.joining.size());

        // Compute the new membership
        var membership = new DigestBloomFilter(view.currentView().fold(),
                                               Math.max(params.minimumBiffCardinality(),
                                                        (context.memberCount() + ballot.joining.size())),
                                               MEMBERSHIP_FPR);
        context.allMembers().forEach(p -> membership.add(p.getId()));
        ballot.joining().forEach(id -> membership.add(id));

        view.setMembership(membership);

        // Gather joining notes and members joining on this node
        var joiningNotes = new ArrayList<NoteWrapper>();
        var pending = ballot.joining()
                            .stream()
                            .map(d -> joins.remove(d))
                            .filter(sn -> sn != null)
                            .peek(sn -> joiningNotes.add(sn))
                            .peek(nw -> view.addToView(nw))
                            .peek(nw -> {
                                if (metrics != null) {
                                    metrics.joins().mark();
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

        view.reset();

        // complete all pending joins
        pending.forEach(r -> {
            try {
                final var shuffled = new ArrayList<>(joiningNotes);
                Entropy.secureShuffle(shuffled);
                r.accept(shuffled);
            } catch (Throwable t) {
                log.error("Exception in pending join on: {}", node.getId(), t);
            }
        });
        if (metrics != null) {
            metrics.viewChanges().mark();
        }

        log.info("Installed view: {} from: {} crown: {} for context: {} cardinality: {} count: {} pending: {} leaving: {} joining: {} on: {}",
                 currentView(), previousView, crown.get(), context.getId(), context.cardinality(),
                 context.allMembers().count(), pending.size(), ballot.leaving.size(), ballot.joining.size(),
                 node.getId());

        view.notifyListeners(ballot.joining, ballot.leaving);
    }

    boolean isJoined() {
        return onJoined.isDone();
    }

    /**
     * Formally join the view. Calculate the HEX-BLOOM crown and view, fail and stop
     * if does not match currentView
     */
    synchronized void join() {
        assert context.totalCount() == context.cardinality();
        if (onJoined.isDone()) {
            return;
        }
        crown.set(digestAlgo.getOrigin());
        context.allMembers().forEach(p -> crown.accumulateAndGet(p.id, (a, b) -> a.xor(b)));
        if (!currentView().equals(crown.get().rehash())) {
            log.error("Crown: {} does not produce view: {} cardinality: {} count: {} on: {}", crown.get(),
                      currentView(), context.cardinality(), context.totalCount(), node.getId());
            view.stop();
            throw new IllegalStateException("Invalid crown");
        }
        view.notifyListeners(context.allMembers().map(p -> p.getId()).toList(), Collections.emptyList());
        onJoined.complete(null);
        joined.set(true);

        view.scheduleViewChange();

        if (metrics != null) {
            metrics.viewChanges().mark();
        }
        log.info("Joined view: {} cardinality: {} count: {} on: {}", currentView(), context.cardinality(),
                 context.totalCount(), node.getId());
    }

    void join(Join join, Digest from, StreamObserver<Gateway> responseObserver, Timer.Context timer) {
        if (!joined.get()) {
            log.trace("Not joined, ignored join of view: {} from: {} on: {}", Digest.from(join.getView()), from,
                      node.getId());
            responseObserver.onNext(Gateway.getDefaultInstance());
            responseObserver.onCompleted();
            return;
        }
        view.stable(() -> {
            var note = new NoteWrapper(join.getNote(), digestAlgo);
            if (!from.equals(note.getId())) {
                responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Member not match note")));
                return;
            }
            var requestView = Digest.from(join.getView());
            final var current = view.getMembership();
            if (current.contains(from)) {
                log.trace("Already a member: {} view: {}  context: {} cardinality: {} on: {}", from, currentView(),
                          context.getId(), context.cardinality(), node.getId());
                joined(Collections.emptyList(), from, responseObserver, timer);
                return;
            }
            if (!currentView().equals(requestView)) {
                responseObserver.onError(new StatusRuntimeException(Status.OUT_OF_RANGE.withDescription("View: "
                + requestView + " does not match: " + currentView())));
                return;
            }
            if (!View.isValidMask(note.getMask(), context)) {
                log.warn("Invalid join mask: {} majority: {} from member: {} view: {}  context: {} cardinality: {} on: {}",
                         note.getMask(), context.majority(), from, currentView(), context.getId(),
                         context.cardinality(), node.getId());
            }
            if (pendingJoins.size() >= params.maxPending()) {
                responseObserver.onError(new StatusRuntimeException(Status.RESOURCE_EXHAUSTED.withDescription("No room at the inn")));
                return;
            }
            pendingJoins.put(from, joining -> {
                log.info("Gateway established for: {} view: {}  context: {} cardinality: {} on: {}", from,
                         currentView(), context.getId(), context.cardinality(), node.getId());
                joined(joining.stream().map(nw -> nw.getWrapped()).limit(params.maximumTxfr()).toList(), from,
                       responseObserver, timer);
            });
            joins.put(note.getId(), note);
            log.debug("Member pending join: {} view: {} context: {} on: {}", from, currentView(), context.getId(),
                      node.getId());
        });
    }

    BiConsumer<? super Bound, ? super Throwable> join(ScheduledExecutorService scheduler, Duration duration,
                                                      Timer.Context timer) {
        return (bound, t) -> {
            view.viewChange(() -> {
                if (t != null) {
                    log.error("Failed to join view: {}on: {}", bound.view(), node.getId(), t);
                    view.stop();
                    return;
                }

                log.info("Rebalancing to cardinality: {} (pre join) for: {} context: {} on: {}", context.totalCount(),
                         bound.view(), context.getId(), node.getId());
                context.rebalance(bound.cardinality());
                context.activate(node);

                view.setMembership(bound.bff());
                currentView.set(bound.view());

                bound.seeds().forEach(nw -> view.addToView(nw));
                bound.joined().forEach(nw -> view.addToView(new NoteWrapper(nw, digestAlgo)));

                view.reset();

                context.allMembers().forEach(p -> p.clearAccusations());

                view.schedule(duration, scheduler);

                if (timer != null) {
                    timer.stop();
                }
                joined.set(true);

                log.info("Currently joining view: {} seeds: {} cardinality: {} count: {} on: {}", bound.view(),
                         bound.seeds().size(), context.cardinality(), context.totalCount(), node.getId());
                if (context.totalCount() == context.cardinality()) {
                    join();
                }
            });
        };
    }

    void joinUpdatesFor(BloomFilter<Digest> joinBff, Builder builder) {
        joins.entrySet()
             .stream()
             .filter(e -> !joinBff.contains(e.getKey()))
             .collect(new ReservoirSampler<>(params.maximumTxfr(), Entropy.bitsStream()))
             .forEach(e -> builder.addJoins(e.getValue().getWrapped()));
    }

    /**
     * start a view change if there's any offline members or joining members
     */
    void maybeViewChange() {
        if (context.offlineCount() > 0 || joins.size() > 0) {
            initiateViewChange();
        } else {
            view.scheduleViewChange();
        }
    }

    JoinGossip.Builder processJoins(BloomFilter<Digest> bff) {
        JoinGossip.Builder builder = JoinGossip.newBuilder();

        // Add all updates that this view has that aren't reflected in the inbound bff
        joins.entrySet()
             .stream()
             .filter(m -> !bff.contains(m.getKey()))
             .map(m -> m.getValue())
             .collect(new ReservoirSampler<>(params.maximumTxfr(), Entropy.bitsStream()))
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
            log.trace("process joins produced updates: {} on: {}", builder.getUpdatesCount(), node.getId());
        }
        return gossip;
    }

    void resetBootstrapView() {
        crown.set(digestAlgo.getOrigin());
        currentView.set(view.bootstrapView());
    }

    CompletableFuture<Redirect> seed(Registration registration, Digest from) {
        final var requestView = Digest.from(registration.getView());

        if (!joined.get()) {
            log.warn("Not joined, ignored seed view: {} from: {} on: {}", requestView, from, node.getId());
            return complete(Redirect.getDefaultInstance());
        }
        if (!view.bootstrapView().equals(requestView)) {
            log.warn("Invalid bootstrap view: {} from: {} on: {}", requestView, from, node.getId());
            return complete(Redirect.getDefaultInstance());
        }
        var note = new NoteWrapper(registration.getNote(), digestAlgo);
        if (!from.equals(note.getId())) {
            log.warn("Invalid bootstrap note: {} from: {} claiming: {} on: {}", requestView, from, note.getId(),
                     node.getId());
            return complete(Redirect.getDefaultInstance());
        }
        return view.stable(() -> {
            var newMember = view.new Participant(
                                                 note.getId());
            final var successors = new TreeSet<Participant>(context.successors(newMember, m -> context.isActive(m)));

            log.debug("Member seeding: {} view: {} context: {} successors: {} on: {}", newMember.getId(), currentView(),
                      context.getId(), successors.size(), node.getId());
            return generateNonce(registration).thenApply(sn -> Redirect.newBuilder()
                                                                       .setView(currentView().toDigeste())
                                                                       .addAllSuccessors(successors.stream()
                                                                                                   .filter(p -> p != null)
                                                                                                   .map(p -> p.getSeed())
                                                                                                   .toList())
                                                                       .setCardinality(context.cardinality())
                                                                       .setBootstrap(bootstrap)
                                                                       .setRings(context.getRingCount())
                                                                       .setNonce(sn)
                                                                       .build());
        });
    }

    void start(CompletableFuture<Void> onJoin, boolean bootstrap) {
        this.onJoined = onJoin;
        this.bootstrap = bootstrap;
    }

    private CompletableFuture<SignedNonce> generateNonce(Registration registration) {
        var now = params.gorgoneion().clock().instant();
        final var identifier = identifier(registration);
        if (identifier == null) {
            var fs = new CompletableFuture<SignedNonce>();
            fs.completeExceptionally(new IllegalArgumentException("No identifier"));
            return fs;
        }
        var nonce = Nonce.newBuilder()
                         .setMember(identifier)
                         .setDuration(com.google.protobuf.Duration.newBuilder()
                                                                  .setSeconds(params.gorgoneion()
                                                                                    .registrationTimeout()
                                                                                    .toSecondsPart())
                                                                  .setNanos(params.gorgoneion()
                                                                                  .registrationTimeout()
                                                                                  .toNanosPart())
                                                                  .build())
                         .setIssuer(node.getIdentifier().getLastEstablishmentEvent().toEventCoords())
                         .setNoise(digestAlgo.random().toDigeste())
                         .setTimestamp(Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()))
                         .build();
        return node.getIdentifier()
                   .getSigner()
                   .thenApply(s -> SignedNonce.newBuilder()
                                              .setNonce(nonce)
                                              .setSignature(s.sign(nonce.toByteString()).toSig())
                                              .build());
    }

    private Ident identifier(Registration registration) {
        final var kerl = registration.getKerl();
        if (ProtobufEventFactory.from(kerl.getEvents(kerl.getEventsCount() - 1))
                                .event() instanceof EstablishmentEvent establishment) {
            return establishment.getIdentifier().toIdent();
        }
        return null;
    }

    /**
     * Initiate the view change
     */
    private void initiateViewChange() {
        view.stable(() -> {
            if (vote.get() != null) {
                log.trace("Vote already cast for: {} on: {}", view.currentView(), node.getId());
                return;
            }
            // Use pending rebuttals as a proxy for stability
            if (!view.hasPendingRebuttals()) {
                log.debug("Pending rebuttals in view: {} on: {}", view.currentView(), node.getId());
                view.scheduleViewChange(1); // 1 TTL round to check again
                return;
            }
            view.scheduleFinalizeViewChange();
            final var builder = ViewChange.newBuilder()
                                          .setObserver(node.getId().toDigeste())
                                          .setCurrent(view.currentView().toDigeste())
                                          .setAttempt(attempt.getAndIncrement())
                                          .addAllLeaves(view.streamShunned()
                                                            .map(id -> id.toDigeste())
                                                            .collect(Collectors.toSet()))
                                          .addAllJoins(joins.keySet().stream().map(id -> id.toDigeste()).toList());
            ViewChange change = builder.build();
            vote.set(change);
            var signature = node.sign(change.toByteString());
            final var viewChange = SignedViewChange.newBuilder()
                                                   .setChange(change)
                                                   .setSignature(signature.toSig())
                                                   .build();
            view.initiate(viewChange);
            log.info("View change vote: {} joins: {} leaves: {} on: {}", view.currentView(), change.getJoinsCount(),
                     change.getLeavesCount(), node.getId());
        });
    }

    private void joined(List<SignedNote> joined, Digest from, StreamObserver<Gateway> responseObserver,
                        Timer.Context timer) {
        var seedSet = new TreeSet<Participant>();
        context.successors(from, m -> context.isActive(m)).forEach(p -> seedSet.add(p));
        final var current = view.getMembership();
        var gateway = Gateway.newBuilder()
                             .setView(currentView().toDigeste())
                             .addAllInitialSeedSet(seedSet.stream()
                                                          .map(p -> p.getNote().getWrapped())
                                                          .collect(new ReservoirSampler<>(params.maximumTxfr(),
                                                                                          Entropy.bitsStream())))
                             .setMembers(current.toBff())
                             .setCardinality(context.cardinality())
                             .addAllJoining(joined.stream()
                                                  .collect(new ReservoirSampler<>(params.maximumTxfr(),
                                                                                  Entropy.bitsStream())))
                             .build();
        assert gateway.getInitialSeedSetCount() != 0 : "No seeds";
        assert !gateway.getMembers().equals(Biff.getDefaultInstance()) : "Empty membership";
        responseObserver.onNext(gateway);
        responseObserver.onCompleted();
        if (timer != null) {
            var serializedSize = gateway.getSerializedSize();
            metrics.outboundBandwidth().mark(serializedSize);
            metrics.outboundGateway().update(serializedSize);
            timer.stop();
        }
    }
}
