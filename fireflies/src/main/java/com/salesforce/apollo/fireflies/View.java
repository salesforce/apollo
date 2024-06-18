/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import com.codahale.metrics.Timer;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multiset.Entry;
import com.google.common.collect.Ordering;
import com.google.protobuf.ByteString;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.Router.ServiceRouting;
import com.salesforce.apollo.archipelago.RouterImpl.CommonCommunications;
import com.salesforce.apollo.bloomFilters.BloomFilter;
import com.salesforce.apollo.context.DynamicContext;
import com.salesforce.apollo.context.DynamicContextImpl;
import com.salesforce.apollo.context.ViewChange;
import com.salesforce.apollo.cryptography.*;
import com.salesforce.apollo.cryptography.proto.Biff;
import com.salesforce.apollo.fireflies.Binding.Bound;
import com.salesforce.apollo.fireflies.ViewManagement.Ballot;
import com.salesforce.apollo.fireflies.comm.entrance.Entrance;
import com.salesforce.apollo.fireflies.comm.entrance.EntranceClient;
import com.salesforce.apollo.fireflies.comm.entrance.EntranceServer;
import com.salesforce.apollo.fireflies.comm.entrance.EntranceService;
import com.salesforce.apollo.fireflies.comm.gossip.FFService;
import com.salesforce.apollo.fireflies.comm.gossip.FfServer;
import com.salesforce.apollo.fireflies.comm.gossip.Fireflies;
import com.salesforce.apollo.fireflies.proto.*;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.ReservoirSampler;
import com.salesforce.apollo.membership.RoundScheduler;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.ring.RingCommunications;
import com.salesforce.apollo.stereotomy.EventValidation;
import com.salesforce.apollo.stereotomy.Verifiers;
import com.salesforce.apollo.stereotomy.event.proto.KeyState_;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.utils.BbBackedInputStream;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.Utils;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.salesforce.apollo.fireflies.comm.gossip.FfClient.getCreate;

/**
 * The View is the active representation view of all members - failed and live - known. The View interacts with other
 * members on behalf of its Node and monitors other members, issuing Accusations against failed members that this View
 * is monitoring. Accusations may be rebutted. Then there's discovery and garbage collection. It's all very complicated.
 * These complications and many others are detailed in the wonderful
 * <a href= "https://ymsir.com/papers/fireflies-tocs.pdf">Fireflies paper</a>.
 * <p>
 * This implementation differs significantly from the original Fireflies implementation. This version incorporates the
 * <a href= "https://www.usenix.org/system/files/conference/atc18/atc18-suresh.pdf">Rapid</a> notion of a stable,
 * virtually synchronous membership view, as well as relevant ideas from <a href=
 * "https://www.cs.huji.ac.il/~dolev/pubs/opodis07-DHR-fulltext.pdf">Stable-Fireflies</a>.
 * <p>
 * This implementation is also very closely linked with the KERI Stereotomy implementation of Apollo as the View
 * explicitly uses teh Controlled Identifier form of membership.
 *
 * @author hal.hildebrand
 * @since 220
 */
public class View {
    private static final String FINALIZE_VIEW_CHANGE  = "FINALIZE VIEW CHANGE";
    private static final Logger log                   = LoggerFactory.getLogger(View.class);
    private static final String SCHEDULED_VIEW_CHANGE = "Scheduled View Change";

    final            CommonCommunications<Fireflies, Service>    comm;
    final            AtomicBoolean                               started             = new AtomicBoolean();
    private final    CommonCommunications<Entrance, Service>     approaches;
    private final    DynamicContext<Participant>                 context;
    private final    DigestAlgorithm                             digestAlgo;
    private final    RingCommunications<Participant, Fireflies>  gossiper;
    private final    AtomicBoolean                               introduced          = new AtomicBoolean();
    private final    Map<String, Consumer<ViewChange>>           viewChangeListeners = new HashMap<>();
    private final    Executor                                    viewNotificationQueue;
    private final    FireflyMetrics                              metrics;
    private final    Node                                        node;
    private final    Map<Digest, SignedViewChange>               observations        = new ConcurrentSkipListMap<>();
    private final    Parameters                                  params;
    private final    ConcurrentMap<Digest, RoundScheduler.Timer> pendingRebuttals    = new ConcurrentSkipListMap<>();
    private final    RoundScheduler                              roundTimers;
    private final    Set<Digest>                                 shunned             = new ConcurrentSkipListSet<>();
    private final    Map<String, RoundScheduler.Timer>           timers              = new HashMap<>();
    private final    ReadWriteLock                               viewChange;
    private final    ViewManagement                              viewManagement;
    private final    EventValidation                             validation;
    private final    Verifiers                                   verifiers;
    private volatile ScheduledFuture<?>                          futureGossip;

    public View(DynamicContext<Participant> context, ControlledIdentifierMember member, String endpoint,
                EventValidation validation, Verifiers verifiers, Router communications, Parameters params,
                DigestAlgorithm digestAlgo, FireflyMetrics metrics) {
        this(context, member, endpoint, validation, verifiers, communications, params, communications, digestAlgo,
             metrics);
    }

    public View(DynamicContext<Participant> context, ControlledIdentifierMember member, String endpoint,
                EventValidation validation, Verifiers verifiers, Router communications, Parameters params,
                Router gateway, DigestAlgorithm digestAlgo, FireflyMetrics metrics) {
        this.metrics = metrics;
        this.params = params;
        this.digestAlgo = digestAlgo;
        this.context = context;
        this.roundTimers = new RoundScheduler(String.format("Timers for: %s", context.getId()), context.timeToLive());
        this.node = new Node(member, endpoint);
        viewManagement = new ViewManagement(this, context, params, metrics, node, digestAlgo);
        var service = new Service();
        this.comm = communications.create(node, context.getId(), service,
                                          r -> new FfServer(communications.getClientIdentityProvider(), r, metrics),
                                          getCreate(metrics), Fireflies.getLocalLoopback(node));
        this.approaches = gateway.create(node, context.getId(), service,
                                         service.getClass().getCanonicalName() + ":approach",
                                         r -> new EntranceServer(gateway.getClientIdentityProvider(), r, metrics),
                                         EntranceClient.getCreate(metrics), Entrance.getLocalLoopback(node, service));
        gossiper = new RingCommunications<>(context, node, comm);
        gossiper.allowDuplicates();
        gossiper.ignoreSelf();
        this.validation = validation;
        this.verifiers = verifiers;
        viewNotificationQueue = Executors.newSingleThreadExecutor(Thread.ofVirtual().factory());
        viewChange = new ReentrantReadWriteLock(true);
    }

    /**
     * Check the validity of a mask. A mask is valid if the following conditions are satisfied:
     *
     * <pre>
     * - The mask is of length bias*t+1
     * - the mask has exactly t + 1 enabled elements.
     * </pre>
     *
     * @param mask
     * @return
     */
    public static boolean isValidMask(BitSet mask, DynamicContext<?> context) {
        if (mask.cardinality() == context.majority()) {
            if (mask.length() <= context.getRingCount()) {
                return true;
            } else {
                log.debug("invalid length: {} required: {}", mask.length(), context.getRingCount());
            }
        } else {
            log.debug("invalid cardinality: {} required: {}", mask.cardinality(), context.majority());
        }
        return false;
    }

    /**
     * Deregister the listener
     */
    public void deregister(Consumer<ViewChange> listener) {
        viewChangeListeners.remove(listener);
    }

    /**
     * @return the context of the view
     */
    public DynamicContext<Participant> getContext() {
        return context;
    }

    /**
     * @return the Digest ID of the Node of this View
     */
    public Digest getNodeId() {
        return node.getId();
    }

    /**
     * Register the listener to receive view changes
     */
    public void register(String key, Consumer<ViewChange> listener) {
        viewChangeListeners.put(key, listener);
    }

    /**
     * Start the View
     */
    public void start(CompletableFuture<Void> onJoin, Duration d, List<Seed> seedpods) {
        Objects.requireNonNull(onJoin, "Join completion must not be null");
        if (!started.compareAndSet(false, true)) {
            return;
        }
        var seeds = new ArrayList<>(seedpods);
        Entropy.secureShuffle(seeds);
        viewManagement.start(onJoin, seeds.isEmpty());

        log.info("Starting: {} cardinality: {} tolerance: {} seeds: {} on: {}", context.getId(),
                 viewManagement.cardinality(), context.toleranceLevel(), seeds.size(), node.getId());
        viewManagement.clear();
        roundTimers.reset();
        context.clear();
        node.reset();

        var scheduler = Executors.newScheduledThreadPool(1, Thread.ofVirtual().factory());
        var initial = Entropy.nextBitsStreamLong(d.toNanos());
        scheduler.schedule(() -> Thread.ofVirtual()
                                       .start(Utils.wrapped(
                                       () -> new Binding(this, seeds, d, context, approaches, node, params, metrics,
                                                         digestAlgo).seeding(), log)), initial, TimeUnit.NANOSECONDS);

        log.info("{} started on: {}", context.getId(), node.getId());
    }

    /**
     * Start the View
     */
    public void start(Runnable onJoin, Duration d, List<Seed> seedpods) {
        final var futureSailor = new CompletableFuture<Void>();
        futureSailor.whenComplete((v, t) -> onJoin.run());
        start(futureSailor, d, seedpods);
    }

    /**
     * stop the view from performing gossip and monitoring rounds
     */
    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        roundTimers.reset();
        comm.deregister(context.getId());
        pendingRebuttals.clear();
        context.active().forEach(context::offline);
        final var current = futureGossip;
        futureGossip = null;
        if (current != null) {
            current.cancel(true);
        }
        observations.clear();
        timers.values().forEach(RoundScheduler.Timer::cancel);
        timers.clear();
        viewManagement.clear();
    }

    @Override
    public String toString() {
        return "View[" + node.getId() + "]";
    }

    boolean addToView(NoteWrapper note) {
        var newMember = false;
        NoteWrapper current = null;

        Participant m = context.getMember(note.getId());
        if (m == null) {
            newMember = true;
            if (!verify(note.getIdentifier(), note.getSignature(), note.getWrapped().getNote().toByteString())) {
                log.trace("invalid participant note from: {} on: {}", note.getId(), node.getId());
                if (metrics != null) {
                    metrics.filteredNotes().mark();
                }
                return false;
            }
            m = new Participant(note);
            context.add(m);
        } else {
            current = m.getNote();
            if (!newMember && current != null) {
                long nextEpoch = note.getEpoch();
                long currentEpoch = current.getEpoch();
                if (nextEpoch <= currentEpoch) {
                    if (metrics != null) {
                        metrics.filteredNotes().mark();
                    }
                    return false;
                }
            }
        }

        if (metrics != null) {
            metrics.notes().mark();
        }

        var member = m;
        return stable(() -> {
            if (!member.verify(note.getSignature(), note.getWrapped().getNote().toByteString())) {
                log.trace("Note signature invalid: {} on: {}", note.getId(), node.getId());
                if (metrics != null) {
                    metrics.filteredNotes().mark();
                }
                return false;
            }
            var accused = member.isAccused();
            stopRebuttalTimer(member);
            member.setNote(note);
            recover(member);
            if (accused) {
                checkInvalidations(member);
            }
            if (!viewManagement.joined() && context.size() == viewManagement.cardinality()) {
                assert context.size() == viewManagement.cardinality();
                viewManagement.join();
            } else {
                // This assertion needs to accommodate invalid diadem cardinality during view installation, as the diadem
                // is from the previous view until all joining member have... joined.
                assert context.size() <= Math.max(viewManagement.cardinality(), context.cardinality()) : "total: "
                + context.size() + " card: " + viewManagement.cardinality();
            }
            return true;
        });
    }

    void bootstrap(NoteWrapper nw, Duration dur) {
        viewManagement.bootstrap(nw, dur);
    }

    Digest bootstrapView() {
        return context.getId().prefix(digestAlgo.getOrigin());
    }

    Digest currentView() {
        return viewManagement.currentView();
    }

    /**
     * Finalize the view change
     */
    void finalizeViewChange() {
        if (!started.get()) {
            return;
        }
        viewChange(() -> {
            final var supermajority = context.getRingCount() * 3 / 4;
            final var majority = context.size() == 1 ? 1 : supermajority;
            if (observations.size() < majority) {
                log.trace("Do not have majority: {} required: {} observers: {} for: {} on: {}", observations.size(),
                          majority, viewManagement.observersList(), currentView(), node.getId());
                scheduleFinalizeViewChange(2);
                return;
            }
            log.info("Finalizing view change: {} required: {} observers: {} for: {} on: {}", context.getId(), majority,
                     viewManagement.observersList(), currentView(), node.getId());
            HashMultiset<Ballot> ballots = HashMultiset.create();
            final var current = currentView();
            observations.values()
                        .stream()
                        .filter(vc -> current.equals(Digest.from(vc.getChange().getCurrent())))
                        .forEach(vc -> {
                            final var leaving = new ArrayList<>(
                            vc.getChange().getLeavesList().stream().map(Digest::from).collect(Collectors.toSet()));
                            final var joining = new ArrayList<>(
                            vc.getChange().getJoinsList().stream().map(Digest::from).collect(Collectors.toSet()));
                            leaving.sort(Ordering.natural());
                            joining.sort(Ordering.natural());
                            ballots.add(
                            new Ballot(Digest.from(vc.getChange().getCurrent()), leaving, joining, digestAlgo));
                        });
            observations.clear();
            var max = ballots.entrySet()
                             .stream()
                             .max(Ordering.natural().onResultOf(Multiset.Entry::getCount))
                             .orElse(null);
            if (max != null && max.getCount() >= majority) {
                log.info("View consensus successful: {} required: {} cardinality: {} for: {} on: {}", max, majority,
                         viewManagement.cardinality(), currentView(), node.getId());
                viewManagement.install(max.getElement());
            } else {
                @SuppressWarnings("unchecked")
                final var reversed = Comparator.comparing(e -> ((Entry<Ballot>) e).getCount()).reversed();
                log.info("View consensus failed: {}, required: {} cardinality: {} ballots: {} for: {} on: {}",
                         observations.size(), majority, viewManagement.cardinality(),
                         ballots.entrySet().stream().sorted(reversed).toList(), currentView(), node.getId());
            }

            scheduleViewChange();
            removeTimer(View.FINALIZE_VIEW_CHANGE);
            viewManagement.clearVote();
        });
    }

    /**
     * Test accessible
     *
     * @return The member that represents this View
     */
    Node getNode() {
        return node;
    }

    boolean hasPendingRebuttals() {
        return !pendingRebuttals.isEmpty();
    }

    void initiate(SignedViewChange viewChange) {
        observations.put(node.getId(), viewChange);
    }

    void introduced() {
        introduced.set(true);
    }

    BiConsumer<? super Bound, ? super Throwable> join(Duration duration, com.codahale.metrics.Timer.Context timer) {
        return viewManagement.join(duration, timer);
    }

    void notifyListeners(List<SelfAddressingIdentifier> joining, List<Digest> leaving) {
        final var viewChange = new ViewChange(context.asStatic(), currentView(),
                                              joining.stream().map(SelfAddressingIdentifier::getDigest).toList(),
                                              Collections.unmodifiableList(leaving));
        viewNotificationQueue.execute(Utils.wrapped(() -> {
            viewChangeListeners.entrySet().forEach(entry -> {
                try {
                    log.trace("Notifying: {} view change: {} cardinality: {} joins: {} leaves: {} on: {} ",
                              entry.getKey(), currentView(), context.size(), joining.size(), leaving.size(),
                              node.getId());
                    entry.getValue().accept(viewChange);
                } catch (Throwable e) {
                    log.error("error in view change listener: {} on: {} ", entry.getKey(), node.getId(), e);
                }
            });
        }, log));
    }

    /**
     * Process the updates of the supplied juicy gossip.
     *
     * @param gossip
     */
    void processUpdates(Gossip gossip) {
        processUpdates(gossip.getNotes().getUpdatesList(), gossip.getAccusations().getUpdatesList(),
                       gossip.getObservations().getUpdatesList(), gossip.getJoins().getUpdatesList());
    }

    /**
     * Redirect the receiver to the correct ring, processing any new accusations
     *
     * @param member
     * @param gossip
     * @param ring
     */
    boolean redirect(Participant member, Gossip gossip, int ring) {
        if (gossip.getRedirect().equals(SignedNote.getDefaultInstance())) {
            log.warn("Redirect from: {} on ring: {} did not contain redirect member note on: {}", member.getId(), ring,
                     node.getId());
            return false;
        }
        final var redirect = new NoteWrapper(gossip.getRedirect(), digestAlgo);
        add(redirect);
        processUpdates(gossip);
        log.debug("Redirected from: {} to: {} on ring: {} on: {}", member.getId(), redirect.getId(), ring,
                  node.getId());
        return true;
    }

    /**
     * Remove the participant from the context
     *
     * @param digest
     */
    void remove(Digest digest) {
        var pending = pendingRebuttals.remove(digest);
        if (pending != null) {
            pending.cancel();
        }
        log.info("Permanently removing {} member {} from context: {} view: {} on: {}",
                 context.isActive(digest) ? "active" : "failed", digest, context.getId(), currentView(), node.getId());
        context.remove(digest);
        shunned.remove(digest);
        if (metrics != null) {
            metrics.leaves().mark();
        }
    }

    void removeTimer(String timer) {
        timers.remove(timer);
    }

    void reset() {
        // Tune
        gossiper.reset();
        roundTimers.setRoundDuration(context.timeToLive());

        // Regenerate for new epoch
        node.nextNote();
    }

    void resetBootstrapView() {
        viewManagement.resetBootstrapView();
    }

    void schedule(final Duration duration) {
        var scheduler = Executors.newScheduledThreadPool(1, Thread.ofVirtual().factory());
        futureGossip = scheduler.schedule(
        () -> Thread.ofVirtual().start(Utils.wrapped(() -> gossip(duration, scheduler), log)),
        Entropy.nextBitsStreamLong(duration.toNanos()), TimeUnit.NANOSECONDS);
    }

    void scheduleFinalizeViewChange() {
        scheduleFinalizeViewChange(params.finalizeViewRounds());
    }

    void scheduleFinalizeViewChange(final int finalizeViewRounds) {
        //        log.trace("View change finalization scheduled: {} rounds for: {} joining: {} leaving: {} on: {}",
        //                  finalizeViewRounds, currentView(), joins.size(), context.getOffline().size(), node.getId());
        if (!started.get()) {
            return;
        }
        timers.put(FINALIZE_VIEW_CHANGE,
                   roundTimers.schedule(FINALIZE_VIEW_CHANGE, this::finalizeViewChange, finalizeViewRounds));
    }

    void scheduleViewChange() {
        scheduleViewChange(params.viewChangeRounds());
    }

    void scheduleViewChange(final int viewChangeRounds) {
        //        log.trace("Schedule view change: {} rounds for: {}   on: {}", viewChangeRounds, currentView(),
        //                  node.getId());
        if (!started.get()) {
            return;
        }
        timers.put(SCHEDULED_VIEW_CHANGE,
                   roundTimers.schedule(SCHEDULED_VIEW_CHANGE, viewManagement::maybeViewChange, viewChangeRounds));
    }

    <T> T stable(Callable<T> call) {
        final var lock = viewChange.readLock();
        lock.lock();
        try {
            return call.call();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            lock.unlock();
        }
    }

    void stable(Runnable r) {
        final var lock = viewChange.readLock();
        lock.lock();
        try {
            r.run();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Cancel the timer to track the accused member
     *
     * @param m
     */
    void stopRebuttalTimer(Participant m) {
        m.clearAccusations();
        var timer = pendingRebuttals.remove(m.getId());
        if (timer != null) {
            log.debug("Cancelling accusation of: {} on: {}", m.getId(), node.getId());
            timer.cancel();
        }
    }

    Stream<Digest> streamShunned() {
        return shunned.stream();
    }

    void tick() {
        roundTimers.tick();
    }

    boolean validate(SelfAddressingIdentifier identifier) {
        return validation.validate(identifier);
    }

    void viewChange(Runnable r) {
        //        log.error("Enter view change on: {}", node.getId());
        final var lock = viewChange.writeLock();
        lock.lock();
        try {
            r.run();
            //            log.error("Exit view change on: {}", node.getId());
        } catch (Throwable t) {
            log.error("Error during view change on: {}", node.getId(), t);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Gossip with the member
     *
     * @param ring - the index of the gossip ring the gossip is originating from in this view
     * @param link - the outbound communications to the paired member
     * @param ring
     */
    protected Gossip gossip(Fireflies link, int ring) {
        tick();
        if (shunned.contains(link.getMember().getId())) {
            if (metrics != null) {
                metrics.shunnedGossip().mark();
            }
            return null;
        }

        final var p = (Participant) link.getMember();
        final SayWhat gossip = stable(() -> SayWhat.newBuilder()
                                                   .setView(currentView().toDigeste())
                                                   .setNote(node.getNote().getWrapped())
                                                   .setRing(ring)
                                                   .setGossip(commonDigests())
                                                   .build());
        try {
            return link.gossip(gossip);
        } catch (StatusRuntimeException sre) {
            switch (sre.getStatus().getCode()) {
            case PERMISSION_DENIED:
                log.trace("Rejected gossip: {} view: {} from: {} on: {}", sre.getStatus(), currentView(), p.getId(),
                          node.getId());
                break;
            case FAILED_PRECONDITION:
                log.trace("Failed gossip: {} view: {} from: {} on: {}", sre.getStatus(), currentView(), p.getId(),
                          node.getId());
                break;
            case RESOURCE_EXHAUSTED:
                log.trace("Resource exhausted for gossip: {} view: {} from: {} on: {}", sre.getStatus(), currentView(),
                          p.getId(), node.getId());
                break;
            case CANCELLED:
                log.trace("Communication cancelled for gossip view: {} from: {} on: {}", currentView(), p.getId(),
                          node.getId());
                break;
            case UNAVAILABLE:
                log.trace("Communication cancelled for gossip view: {} from: {} on: {}", currentView(), p.getId(),
                          node.getId(), sre);
                accuse(p, ring, sre);
                break;
            default:
                log.debug("Error gossiping: {} view: {} from: {} on: {}", sre.getStatus(), currentView(), p.getId(),
                          node.getId());
                accuse(p, ring, sre);
                break;

            }
            return null;
        } catch (Throwable e) {
            log.debug("Exception gossiping joined: {} with: {} view: {} on: {}", viewManagement.joined(), p.getId(),
                      currentView(), node.getId(), e);
            accuse(p, ring, e);
            return null;
        }

    }

    /**
     * Accuse the member on the ring
     *
     * @param member
     * @param ring
     */
    private void accuse(Participant member, int ring, Throwable e) {
        if (member.isAccusedOn(ring) || member.isDisabled(ring)) {
            return; // Don't issue multiple accusations
        }
        member.addAccusation(node.accuse(member, ring));
        pendingRebuttals.computeIfAbsent(member.getId(),
                                         d -> roundTimers.schedule(() -> gc(member), params.rebuttalTimeout()));
        log.debug("Accuse: {} on ring: {} view: {} (timer started): {} on: {}", member.getId(), ring, currentView(),
                  e.getMessage(), node.getId());
    }

    /**
     * Add an inbound accusation to the view.
     *
     * @param accusation
     */
    private boolean add(AccusationWrapper accusation) {
        Participant accuser = context.getMember(accusation.getAccuser());
        Participant accused = context.getMember(accusation.getAccused());
        if (accuser == null || accused == null) {
            log.trace("Accusation discarded, accused: {} or accuser: {} do not exist in view on: {}",
                      accusation.getAccused(), accusation.getAccuser(), node.getId());
            return false;
        }

        if (!context.validRing(accusation.getRingNumber())) {
            log.trace("Accusation discarded, invalid ring: {} on: {}", accusation.getRingNumber(), node.getId());
            return false;
        }

        if (accused.getEpoch() >= 0 && accused.getEpoch() != accusation.getEpoch()) {
            log.trace("Accusation discarded, epoch: {}  for: {} != epoch: {} on: {}", accusation.getEpoch(),
                      accused.getId(), accused.getEpoch(), node.getId());
            return false;
        }

        if (accused.isDisabled(accusation.getRingNumber())) {
            log.trace("Accusation discarded, Member: {} accused on disabled ring: {} by: {} on: {}", accused.getId(),
                      accusation.getRingNumber(), accuser.getId(), node.getId());
            return false;
        }

        return add(accusation, accuser, accused);
    }

    /**
     * Add an accusation into the view,
     *
     * @param accusation
     * @param accuser
     * @param accused
     */
    private boolean add(AccusationWrapper accusation, Participant accuser, Participant accused) {
        if (node.equals(accused)) {
            node.clearAccusations();
            node.nextNote();
            return false;
        }
        if (!context.validRing(accusation.getRingNumber())) {
            return false;
        }

        if (accused.isAccusedOn(accusation.getRingNumber())) {
            Participant currentAccuser = context.getMember(
            accused.getAccusation(accusation.getRingNumber()).getAccuser());
            if (!currentAccuser.equals(accuser)) {
                if (context.isBetween(accusation.getRingNumber(), currentAccuser, accuser, accused)) {
                    if (!accused.verify(accusation.getSignature(),
                                        accusation.getWrapped().getAccusation().toByteString())) {
                        log.trace("Accusation discarded, accusation by: {} accused:{} signature invalid on: {}",
                                  accuser.getId(), accused.getId(), node.getId());
                        return false;
                    }
                    accused.addAccusation(accusation);
                    pendingRebuttals.computeIfAbsent(accused.getId(), d -> roundTimers.schedule(() -> gc(accused),
                                                                                                params.rebuttalTimeout()));
                    log.debug("{} accused by: {} on ring: {} (replacing: {}) on: {}", accused.getId(), accuser.getId(),
                              accusation.getRingNumber(), currentAccuser.getId(), node.getId());
                    if (metrics != null) {
                        metrics.accusations().mark();
                    }
                    return true;
                } else {
                    log.debug("{} accused by: {} on ring: {} discarded as not closer than: {} on: {}", accused.getId(),
                              accuser.getId(), accusation.getRingNumber(), currentAccuser.getId(), node.getId());
                    return false;
                }
            } else {
                log.debug("{} accused by: {} on ring: {} discarded as redundant: {} on: {}", accused.getId(),
                          accuser.getId(), accusation.getRingNumber(), currentAccuser.getId(), node.getId());
                return false;
            }
        } else {
            if (shunned.contains(accused.getId())) {
                accused.addAccusation(accusation);
                if (metrics != null) {
                    metrics.accusations().mark();
                }
                return false;
            }
            Participant predecessor = context.predecessor(accusation.getRingNumber(), accused,
                                                          m -> (!m.isAccused()) || (m.equals(accuser)));
            if (accuser.equals(predecessor)) {
                accused.addAccusation(accusation);
                if (!accused.equals(node) && !pendingRebuttals.containsKey(accused.getId())) {
                    log.debug("{} accused by: {} on ring: {} (timer started) on: {}", accused.getId(), accuser.getId(),
                              accusation.getRingNumber(), node.getId());
                    pendingRebuttals.computeIfAbsent(accused.getId(), d -> roundTimers.schedule(() -> gc(accused),
                                                                                                params.rebuttalTimeout()));
                }
                if (metrics != null) {
                    metrics.accusations().mark();
                }
                return true;
            } else {
                log.debug("{} accused by: {} on ring: {} discarded as not predecessor: {} on: {}", accused.getId(),
                          accuser.getId(), accusation.getRingNumber(), predecessor.getId(), node.getId());
                return false;
            }
        }
    }

    private boolean add(NoteWrapper note) {
        if (shunned.contains(note.getId())) {
            log.trace("Note: {} is shunned on: {}", note.getId(), node.getId());
            if (metrics != null) {
                metrics.filteredNotes().mark();
            }
            return false;
        }
        if (!viewManagement.contains(note.getId())) {
            log.debug("Note: {} is not a member  on: {}", note.getId(), node.getId());
            if (metrics != null) {
                metrics.filteredNotes().mark();
            }
            return false;
        }

        if (!isValidMask(note.getMask(), context)) {
            log.debug("Invalid mask of: {} cardinality: {} on: {}", note.getId(), note.getMask().cardinality(),
                      node.getId());
            if (metrics != null) {
                metrics.filteredNotes().mark();
            }
            return false;
        }

        return addToView(note);
    }

    /**
     * Add an observation if it is for the current view and has not been previously observed by the observer
     *
     * @param observation
     */
    private boolean add(SignedViewChange observation) {
        final Digest observer = Digest.from(observation.getChange().getObserver());
        if (!viewManagement.isObserver(observer)) {
            log.trace("Invalid observer: {} current: {} on: {}", observer, currentView(), node.getId());
            return false;
        }
        final var inView = Digest.from(observation.getChange().getCurrent());
        if (!currentView().equals(inView)) {
            log.trace("Invalid view change: {} current: {} from {} on: {}", inView, currentView(), observer,
                      node.getId());
            return false;
        }
        var currentObservation = observations.get(observer);
        if (currentObservation != null) {
            if (observation.getChange().getAttempt() < currentObservation.getChange().getAttempt()) {
                log.trace("Stale observation: {} current: {} view change: {} current: {} offline: {} on: {}",
                          observation.getChange().getAttempt(), currentObservation.getChange().getAttempt(), inView,
                          currentView(), observer, node.getId());
                return false;
            } else if (observation.getChange().getAttempt() < currentObservation.getChange().getAttempt()) {
                return false;
            }
        }
        final var member = context.getActiveMember(observer);
        if (member == null) {
            log.trace("Cannot validate view change: {} current: {} offline: {} on: {}", inView, currentView(), observer,
                      node.getId());
            return false;
        }
        return observations.computeIfAbsent(observer.prefix(observation.getChange().getAttempt()), p -> {
            final var signature = JohnHancock.from(observation.getSignature());
            if (!member.verify(signature, observation.getChange().toByteString())) {
                return null;
            }
            return observation;
        }) != null;
    }

    private boolean addJoin(SignedNote sn) {
        final var note = new NoteWrapper(sn, digestAlgo);

        if (!currentView().equals(note.currentView())) {
            log.trace("Invalid join note view: {} current: {} from: {} on: {}", note.currentView(), currentView(),
                      note.getId(), node.getId());
            return false;
        }

        if (viewManagement.contains(note.getId())) {
            log.trace("Already a member, ignoring join note from: {} on: {}", note.currentView(), currentView(),
                      note.getId(), node.getId());
            return false;
        }

        if (!isValidMask(note.getMask(), context)) {
            log.warn("Invalid join note from: {} mask invalid: {} majority: {} on: {}", note.getId(), note.getMask(),
                     context.majority(), node.getId());
            return false;
        }

        if (!validation.validate(note.getIdentifier())) {
            log.trace("Invalid join note from {} on: {}", note.getId(), node.getId());
            return false;
        }

        return viewManagement.addJoin(note.getId(), note);
    }

    /**
     * add an inbound note to the view
     *
     * @param note
     */
    private boolean addToCurrentView(NoteWrapper note) {
        if (!currentView().equals(note.currentView())) {
            log.trace("Ignoring note in invalid view: {} current: {} from {} on: {}", note.currentView(), currentView(),
                      note.getId(), node.getId());
            if (metrics != null) {
                metrics.filteredNotes().mark();
            }
            return false;
        }
        if (shunned.contains(note.getId())) {
            if (metrics != null) {
                metrics.filteredNotes().mark();
            }
            log.trace("Note shunned: {} on: {}", note.getId(), node.getId());
            return false;
        }
        return add(note);
    }

    /**
     * If we monitor the target and haven't issued an alert, do so
     *
     * @param target
     */
    private void amplify(Participant target) {
        context.rings()
               .filter(
               ring -> !target.isDisabled(ring.getIndex()) && target.equals(ring.successor(node, context::isActive)))
               .forEach(ring -> {
                   log.trace("amplifying: {} ring: {} on: {}", target.getId(), ring.getIndex(), node.getId());
                   accuse(target, ring.getIndex(), new IllegalStateException("Amplifying accusation"));
               });
    }

    /**
     * <pre>
     * The member goes from an accused to not accused state. As such,
     * it may invalidate other accusations.
     * Let m_j be m's first live successor on ring r.
     * All accusations for members q between m and m_j:
     *   If q between accuser and accused: invalidate accusation.
     *   If accused now is cleared, rerun for this member.
     * </pre>
     *
     * @param m
     */
    private void checkInvalidations(Participant m) {
        Deque<Participant> check = new ArrayDeque<>();
        check.add(m);
        while (!check.isEmpty()) {
            Participant checked = check.pop();
            context.rings().forEach(ring -> {
                for (Participant q : ring.successors(checked, member -> !member.isAccused())) {
                    if (q.isAccusedOn(ring.getIndex())) {
                        invalidate(q, ring, check);
                    }
                }
            });
        }
    }

    /**
     * @return the digests common for gossip with all neighbors
     */
    private Digests commonDigests() {
        return Digests.newBuilder()
                      .setAccusationBff(getAccusationsBff(Entropy.nextSecureLong(), params.fpr()).toBff())
                      .setNoteBff(getNotesBff(Entropy.nextSecureLong(), params.fpr()).toBff())
                      .setJoinBiff(viewManagement.getJoinsBff(Entropy.nextSecureLong(), params.fpr()).toBff())
                      .setObservationBff(getObservationsBff(Entropy.nextSecureLong(), params.fpr()).toBff())
                      .build();
    }

    private Verifier.Filtered filtered(SelfAddressingIdentifier id, SigningThreshold threshold, JohnHancock signature,
                                       InputStream message) {
        var verifier = verifiers.verifierFor(id);
        if (verifier.isEmpty()) {
            return new Verifier.Filtered(false, 0, null);
        }
        return verifier.get().filtered(threshold, signature, message);
    }

    /**
     * Garbage collect the member. Member is now shunned and cannot recover
     *
     * @param member
     */
    private void gc(Participant member) {
        var pending = pendingRebuttals.remove(member.getId());
        if (pending != null) {
            pending.cancel();
        }
        if (context.isActive(member)) {
            amplify(member);
        }
        log.debug("Garbage collecting: {} view: {} on: {}", member.getId(), viewManagement.currentView(), node.getId());
        context.offline(member);
        shunned.add(member.getId());
        viewManagement.gc(member);
    }

    /**
     * @param seed
     * @param p
     * @return the bloom filter containing the digests of known accusations
     */
    private BloomFilter<Digest> getAccusationsBff(long seed, double p) {
        BloomFilter<Digest> bff = new BloomFilter.DigestBloomFilter(seed, Math.max(params.minimumBiffCardinality(),
                                                                                   context.cardinality() * 2), p);
        context.allMembers()
               .flatMap(Participant::getAccusations)
               .filter(Objects::nonNull)
               .forEach(m -> bff.add(m.getHash()));
        return bff;
    }

    /**
     * @param seed
     * @param p
     * @return the bloom filter containing the digests of known notes
     */
    private BloomFilter<Digest> getNotesBff(long seed, double p) {
        BloomFilter<Digest> bff = new BloomFilter.DigestBloomFilter(seed, Math.max(params.minimumBiffCardinality(),
                                                                                   context.cardinality() * 2), p);
        context.allMembers().map(m -> m.getNote()).filter(e -> e != null).forEach(n -> bff.add(n.getHash()));
        return bff;
    }

    /**
     * @param seed
     * @param p
     * @return the bloom filter containing the digests of known observations
     */
    private BloomFilter<Digest> getObservationsBff(long seed, double p) {
        BloomFilter<Digest> bff = new BloomFilter.DigestBloomFilter(seed, Math.max(params.minimumBiffCardinality(),
                                                                                   context.cardinality() * 2), p);
        observations.keySet().forEach(d -> bff.add(d));
        return bff;
    }

    /**
     * Execute one round of gossip
     *
     * @param duration
     * @param scheduler
     */
    private void gossip(Duration duration, ScheduledExecutorService scheduler) {
        if (!started.get()) {
            return;
        }

        if (context.activeCount() == 1) {
            tick();
        }
        gossiper.execute((link, ring) -> gossip(link, ring),
                         (result, destination) -> gossip(result, destination, duration, scheduler));
    }

    /**
     * Handle the gossip response from the destination
     *
     * @param result
     * @param destination
     * @param duration
     * @param scheduler
     */
    private void gossip(Optional<Gossip> result, RingCommunications.Destination<Participant, Fireflies> destination,
                        Duration duration, ScheduledExecutorService scheduler) {
        try {
            if (result.isPresent()) {
                final var member = destination.member();
                try {
                    Gossip gossip = result.get();
                    if (!gossip.getRedirect().equals(SignedNote.getDefaultInstance())) {
                        stable(() -> redirect(member, gossip, destination.ring()));
                    } else if (viewManagement.joined()) {
                        try {
                            Update update = stable(() -> response(gossip));
                            if (update != null && !update.equals(Update.getDefaultInstance())) {
                                log.trace("Update for: {} notes: {} accusations: {} joins: {} observations: {} on: {}",
                                          destination.member().getId(), update.getNotesCount(),
                                          update.getAccusationsCount(), update.getJoinsCount(),
                                          update.getObservationsCount(), node.getId());
                                destination.link()
                                           .update(State.newBuilder()
                                                        .setView(currentView().toDigeste())
                                                        .setRing(destination.ring())
                                                        .setUpdate(update)
                                                        .build());
                            }
                        } catch (StatusRuntimeException e) {
                            handleSRE("update", destination, member, e);
                        }
                    } else {
                        stable(() -> processUpdates(gossip));
                    }
                } catch (NoSuchElementException e) {
                    if (!viewManagement.joined()) {
                        log.debug("Null bootstrap gossiping with: {} view: {} on: {}", member.getId(), currentView(),
                                  node.getId());
                    } else {
                        if (e.getCause() instanceof StatusRuntimeException sre) {
                            handleSRE("gossip", destination, member, sre);
                        } else {
                            log.debug("Exception gossiping with: {} view: {} on: {}", member.getId(), currentView(),
                                      node.getId(), e);
                            accuse(member, destination.ring(), e);
                        }
                    }
                }
            }

        } finally {
            futureGossip = scheduler.schedule(
            () -> Thread.ofVirtual().start(Utils.wrapped(() -> gossip(duration, scheduler), log)), duration.toNanos(),
            TimeUnit.NANOSECONDS);
        }
    }

    private void handleSRE(String type, RingCommunications.Destination<Participant, Fireflies> destination,
                           final Participant member, StatusRuntimeException sre) {
        switch (sre.getStatus().getCode()) {
        case PERMISSION_DENIED:
            log.trace("Rejected: {}: {} view: {} from: {} on: {}", type, sre.getStatus(), currentView(), member.getId(),
                      node.getId());
            break;
        case RESOURCE_EXHAUSTED:
            log.trace("Unavailable for: {}: {} view: {} from: {} on: {}", type, sre.getStatus(), currentView(),
                      member.getId(), node.getId());
            break;
        case CANCELLED:
            log.trace("Cancelled: {} view: {} from: {} on: {}", type, currentView(), member.getId(), node.getId());
            break;
        default:
            log.debug("Error {}: {} from: {} on: {}", type, sre.getStatus(), member.getId(), node.getId());
            accuse(member, destination.ring(), sre);
            break;
        }
    }

    /**
     * If member currently is accused on ring, keep the new accusation only if it is from a closer predecessor.
     *
     * @param q
     * @param ring
     * @param check
     */
    private void invalidate(Participant q, DynamicContextImpl.Ring<Participant> ring, Deque<Participant> check) {
        AccusationWrapper qa = q.getAccusation(ring.getIndex());
        Participant accuser = context.getMember(qa.getAccuser());
        Participant accused = context.getMember(qa.getAccused());
        if (ring.isBetween(accuser, q, accused)) {
            assert q.isAccused();
            q.invalidateAccusationOnRing(ring.getIndex());
            if (!q.isAccused()) {
                stopRebuttalTimer(q);
                if (context.isOffline(q)) {
                    recover(q);
                } else {
                    log.debug("Member: {} rebuts (accusation invalidated) ring: {} on: {}", q.getId(), ring.getIndex(),
                              node.getId());
                    check.add(q);
                }
            } else {
                log.debug("Invalidated accusation on ring: {} for member: {} on: {}", ring.getIndex(), q.getId(),
                          node.getId());
            }
        }
    }

    private AccusationGossip.Builder processAccusations(BloomFilter<Digest> bff) {
        AccusationGossip.Builder builder = AccusationGossip.newBuilder();
        // Add all updates that this view has that aren't reflected in the inbound
        // bff
        var current = currentView();
        context.allMembers()
               .flatMap(m -> m.getAccusations())
               .filter(m -> current.equals(m.currentView()))
               .filter(a -> !bff.contains(a.getHash()))
               //               .limit(params.maximumTxfr())
               .collect(new ReservoirSampler<>(params.maximumTxfr(), Entropy.bitsStream()))
               .forEach(a -> builder.addUpdates(a.getWrapped()));
        return builder;
    }

    /**
     * Process the inbound accusations from the gossip. Reconcile the differences between the view's state and the
     * digests of the gossip. Update the reply with the list of digests the view requires, as well as proposed updates
     * based on the inbound digets that the view has more recent information. Do not forward accusations from crashed
     * members
     *
     * @param p
     * @param bff
     * @return
     */
    private AccusationGossip processAccusations(BloomFilter<Digest> bff, double p) {
        AccusationGossip.Builder builder = processAccusations(bff);
        builder.setBff(getAccusationsBff(Entropy.nextSecureLong(), p).toBff());
        if (builder.getUpdatesCount() != 0) {
            log.trace("process accusations produced updates: {} on: {}", builder.getUpdatesCount(), node.getId());
        }
        return builder.build();
    }

    private NoteGossip.Builder processNotes(BloomFilter<Digest> bff) {
        NoteGossip.Builder builder = NoteGossip.newBuilder();

        // Add all updates that this view has that aren't reflected in the inbound
        // bff
        final var current = currentView();
        context.active()
               .filter(m -> m.getNote() != null)
               .filter(m -> current.equals(m.getNote().currentView()))
               .filter(m -> !shunned.contains(m.getId()))
               .filter(m -> !bff.contains(m.getNote().getHash()))
               .map(m -> m.getNote())
               //               .limit(params.maximumTxfr()) // Always in sorted order with this method
               .collect(new ReservoirSampler<>(params.maximumTxfr() * 2, Entropy.bitsStream()))
               .forEach(n -> builder.addUpdates(n.getWrapped()));
        return builder;
    }

    /**
     * Process the inbound notes from the gossip. Reconcile the differences between the view's state and the digests of
     * the gossip. Update the reply with the list of digests the view requires, as well as proposed updates based on the
     * inbound digests that the view has more recent information
     *
     * @param from
     * @param p
     * @param bff
     */
    private NoteGossip processNotes(Digest from, BloomFilter<Digest> bff, double p) {
        NoteGossip.Builder builder = processNotes(bff);
        builder.setBff(getNotesBff(Entropy.nextSecureLong(), p).toBff());
        if (builder.getUpdatesCount() != 0) {
            log.trace("process notes produced updates: {} on: {}", builder.getUpdatesCount(), node.getId());
        }
        return builder.build();
    }

    private ViewChangeGossip.Builder processObservations(BloomFilter<Digest> bff) {
        ViewChangeGossip.Builder builder = ViewChangeGossip.newBuilder();

        // Add all updates that this view has that aren't reflected in the inbound bff
        final var current = currentView();
        observations.entrySet()
                    .stream()
                    .filter(e -> Digest.from(e.getValue().getChange().getCurrent()).equals(current))
                    .filter(m -> !bff.contains(m.getKey()))
                    .map(m -> m.getValue())
                    //                    .limit(params.maximumTxfr())
                    .collect(new ReservoirSampler<>(params.maximumTxfr(), Entropy.bitsStream()))
                    .forEach(n -> builder.addUpdates(n));
        return builder;
    }

    /**
     * Process the inbound observer from the gossip. Reconcile the differences between the view's state and the digests
     * of the gossip. Update the reply with the list of digests the view requires, as well as proposed updates based on
     * the inbound digests that the view has more recent information
     *
     * @param p
     * @param bff
     */
    private ViewChangeGossip processObservations(BloomFilter<Digest> bff, double p) {
        ViewChangeGossip.Builder builder = processObservations(bff);
        builder.setBff(getObservationsBff(Entropy.nextSecureLong(), p).toBff());
        if (builder.getUpdatesCount() != 0) {
            log.trace("process view change produced updates: {} on: {}", builder.getUpdatesCount(), node.getId());
        }
        return builder.build();
    }

    /**
     * Process the updates of the supplied juicy gossip.
     *
     * @param notes
     * @param accusations
     */
    private void processUpdates(List<SignedNote> notes, List<SignedAccusation> accusations,
                                List<SignedViewChange> observe, List<SignedNote> joins) {
        var nCount = notes.stream()
                          .map(s -> new NoteWrapper(s, digestAlgo))
                          .filter(note -> addToCurrentView(note))
                          .count();
        var aCount = accusations.stream()
                                .map(s -> new AccusationWrapper(s, digestAlgo))
                                .filter(accusation -> add(accusation))
                                .count();
        var oCount = observe.stream().filter(observation -> add(observation)).count();
        var jCount = joins.stream().filter(j -> addJoin(j)).count();
        if (notes.size() + accusations.size() + observe.size() + joins.size() != 0) {
            log.trace("Updating, members: {} notes: {}:{} accusations: {}:{} observations: {}:{} joins: {}:{} on: {}",
                      context.size(), nCount, notes.size(), aCount, accusations.size(), oCount, observe.size(), jCount,
                      joins.size(), node.getId());
        }
    }

    /**
     * recover a member from the failed state
     *
     * @param member
     */
    private void recover(Participant member) {
        if (shunned.contains(member.id)) {
            log.debug("Not recovering shunned: {} on: {}", member.getId(), node.getId());
            return;
        }
        if (context.activate(member)) {
            log.trace("Recovering: {} cardinality: {} count: {} on: {}", member.getId(), viewManagement.cardinality(),
                      context.size(), node.getId());
        }
    }

    /**
     * Process the gossip response, providing the updates requested by the the other member and processing the updates
     * provided by the other member
     *
     * @param gossip
     * @return the Update based on the processing of the reply from the other member
     */
    private Update response(Gossip gossip) {
        processUpdates(gossip);
        return updatesForDigests(gossip);
    }

    /**
     * Process the gossip reply. Return the gossip with the updates determined from the inbound digests.
     *
     * @param gossip
     * @return
     */
    private Update updatesForDigests(Gossip gossip) {
        Update.Builder builder = Update.newBuilder();

        final var current = currentView();
        var biff = gossip.getNotes().getBff();
        if (!biff.equals(Biff.getDefaultInstance())) {
            BloomFilter<Digest> notesBff = BloomFilter.from(biff);
            context.activeMembers()
                   .stream()
                   .filter(m -> m.getNote() != null)
                   .filter(m -> current.equals(m.getNote().currentView()))
                   .filter(m -> !notesBff.contains(m.getNote().getHash()))
                   .map(m -> m.getNote().getWrapped())
                   .collect(new ReservoirSampler<>(params.maximumTxfr(), Entropy.bitsStream()))
                   .forEach(n -> builder.addNotes(n));
        }

        biff = gossip.getAccusations().getBff();
        if (!biff.equals(Biff.getDefaultInstance())) {
            BloomFilter<Digest> accBff = BloomFilter.from(biff);
            context.allMembers()
                   .flatMap(m -> m.getAccusations())
                   .filter(a -> a.currentView().equals(current))
                   .filter(a -> !accBff.contains(a.getHash()))
                   .collect(new ReservoirSampler<>(params.maximumTxfr(), Entropy.bitsStream()))
                   .forEach(a -> builder.addAccusations(a.getWrapped()));
        }

        biff = gossip.getObservations().getBff();
        if (!biff.equals(Biff.getDefaultInstance())) {
            BloomFilter<Digest> obsvBff = BloomFilter.from(biff);
            observations.entrySet()
                        .stream()
                        .filter(e -> Digest.from(e.getValue().getChange().getCurrent()).equals(current))
                        .filter(e -> !obsvBff.contains(e.getKey()))
                        .collect(new ReservoirSampler<>(params.maximumTxfr(), Entropy.bitsStream()))
                        .forEach(e -> builder.addObservations(e.getValue()));
        }

        biff = gossip.getJoins().getBff();
        if (!biff.equals(Biff.getDefaultInstance())) {
            BloomFilter<Digest> joinBff = BloomFilter.from(biff);
            viewManagement.joinUpdatesFor(joinBff, builder);
        }

        return builder.build();
    }

    private void validate(Digest from, final int ring, Digest requestView, String type) {
        if (shunned.contains(from)) {
            log.trace("Member is shunned: {} cannot {} on: {}", type, from, node.getId());
            throw new StatusRuntimeException(Status.UNKNOWN.withDescription("Member is shunned"));
        }
        if (!started.get()) {
            log.trace("Currently offline, cannot {}, send unknown to: {} on: {}", type, from, node.getId());
            throw new StatusRuntimeException(Status.UNKNOWN.withDescription("Considered offline"));
        }
        if (!requestView.equals(currentView())) {
            log.debug("Invalid {}, view: {} current: {} ring: {} from: {} on: {}", type, requestView, currentView(),
                      ring, from, node.getId());
            throw new StatusRuntimeException(
            Status.PERMISSION_DENIED.withDescription("Invalid view: " + requestView + " current: " + currentView()));
        }
    }

    private void validate(Digest from, NoteWrapper note, Digest requestView, final int ring) {
        if (!from.equals(note.getId())) {
            log.debug("Invalid {} note: {} from: {} ring: {} on: {}", "gossip", note.getId(), from, ring, node.getId());
            throw new StatusRuntimeException(Status.UNAUTHENTICATED.withDescription("Note member does not match"));
        }
        validate(from, ring, requestView, "gossip");
    }

    private void validate(Digest from, SayWhat request) {
        var valid = false;
        var note = new NoteWrapper(request.getNote(), digestAlgo);
        var requestView = Digest.from(request.getView());
        final int ring = request.getRing();
        try {
            validate(from, note, requestView, ring);
            valid = true;
        } finally {
            if (!valid && metrics != null) {
                metrics.shunnedGossip().mark();
            }
        }
    }

    private void validate(Digest from, State request) {
        var valid = false;
        try {
            validate(from, request.getRing(), Digest.from(request.getView()), "update");
            valid = true;
        } finally {
            if (!valid && metrics != null) {
                metrics.shunnedGossip().mark();
            }
        }
    }

    private boolean verify(SelfAddressingIdentifier identifier, JohnHancock signature, ByteString byteString) {
        return verify(identifier, signature, BbBackedInputStream.aggregate(byteString));
    }

    private boolean verify(SelfAddressingIdentifier id, JohnHancock signature, InputStream message) {
        return verifiers.verifierFor(id).map(value -> value.verify(signature, message)).orElse(false);
    }

    private boolean verify(SelfAddressingIdentifier id, SigningThreshold threshold, JohnHancock signature,
                           InputStream message) {
        return verifiers.verifierFor(id).map(value -> value.verify(threshold, signature, message)).orElse(false);
    }

    public record Seed(SelfAddressingIdentifier identifier, String endpoint) {
    }

    public class Node extends Participant implements SigningMember {
        private final ControlledIdentifierMember wrapped;

        public Node(ControlledIdentifierMember wrapped, String endpoint) {
            super(wrapped.getId());
            this.wrapped = wrapped;
            var n = Note.newBuilder()
                        .setEpoch(0)
                        .setEndpoint(endpoint)
                        .setIdentifier(wrapped.getIdentifier().getIdentifier().toIdent())
                        .setMask(ByteString.copyFrom(nextMask().toByteArray()))
                        .build();
            var signedNote = SignedNote.newBuilder()
                                       .setNote(n)
                                       .setSignature(wrapped.sign(n.toByteString()).toSig())
                                       .build();
            note = new NoteWrapper(signedNote, digestAlgo);
            log.info("Endpoint: {} on: {}", endpoint, wrapped.getId());
        }

        /**
         * Create a mask of length DynamicContext.majority() randomly disabled rings
         *
         * @return the mask
         */
        public static BitSet createInitialMask(DynamicContext<?> context) {
            int nbits = context.getRingCount();
            BitSet mask = new BitSet(nbits);
            List<Boolean> random = new ArrayList<>();
            for (int i = 0; i < context.majority(); i++) {
                random.add(true);
            }
            for (int i = 0; i < context.toleranceLevel(); i++) {
                random.add(false);
            }
            Entropy.secureShuffle(random);
            for (int i = 0; i < nbits; i++) {
                if (random.get(i)) {
                    mask.set(i);
                }
            }
            return mask;
        }

        @Override
        public SignatureAlgorithm algorithm() {
            return wrapped.algorithm();
        }

        public SelfAddressingIdentifier getIdentifier() {
            return wrapped.getIdentifier().getIdentifier();
        }

        @Override
        public JohnHancock sign(InputStream message) {
            return wrapped.sign(message);
        }

        @Override
        public String toString() {
            return "Node[" + getId() + "]";
        }

        AccusationWrapper accuse(Participant m, int ringNumber) {
            var accusation = Accusation.newBuilder()
                                       .setEpoch(m.getEpoch())
                                       .setRingNumber(ringNumber)
                                       .setAccuser(getId().toDigeste())
                                       .setAccused(m.getId().toDigeste())
                                       .setCurrentView(currentView().toDigeste())
                                       .build();
            return new AccusationWrapper(SignedAccusation.newBuilder()
                                                         .setAccusation(accusation)
                                                         .setSignature(wrapped.sign(accusation.toByteString()).toSig())
                                                         .build(), digestAlgo);
        }

        /**
         * @return a new mask based on the previous mask and previous accusations.
         */
        BitSet nextMask() {
            final var current = note;
            if (current == null) {
                BitSet mask = createInitialMask(context);
                assert isValidMask(mask, context) : "Invalid mask: " + mask + " majority: " + context.majority()
                + " for node: " + getId();
                return mask;
            }

            BitSet mask = new BitSet(context.getRingCount());
            mask.flip(0, context.getRingCount());
            final var accusations = validAccusations;

            // disable current accusations
            for (int i = 0; i < context.getRingCount() && i < accusations.length; i++) {
                if (accusations[i] != null) {
                    mask.set(i, false);
                }
            }
            // clear masks from previous note
            BitSet previous = BitSet.valueOf(current.getMask().toByteArray());
            for (int index = 0; index < context.getRingCount() && index < accusations.length; index++) {
                if (!previous.get(index) && accusations[index] == null) {
                    mask.set(index, true);
                }
            }

            // Fill the rest of the mask with randomly set index

            while (mask.cardinality() != ((context.getBias() - 1) * context.toleranceLevel()) + 1) {
                int index = Entropy.nextBitsStreamInt(context.getRingCount());
                if (index < accusations.length) {
                    if (accusations[index] != null) {
                        continue;
                    }
                }
                if (mask.cardinality() > context.toleranceLevel() + 1 && mask.get(index)) {
                    mask.set(index, false);
                } else if (mask.cardinality() < context.toleranceLevel() && !mask.get(index)) {
                    mask.set(index, true);
                }
            }
            assert isValidMask(mask, context) : "Invalid mask: " + mask + " t: " + context.toleranceLevel()
            + " for node: " + getId();
            return mask;
        }

        /**
         * Generate a new note for the member based on any previous note and previous accusations. The new note has a
         * larger epoch number the the current note.
         */
        void nextNote() {
            nextNote(currentView());
        }

        void nextNote(Digest view) {
            NoteWrapper current = note;
            long newEpoch = current == null ? 0 : note.getEpoch() + 1;
            nextNote(newEpoch, view);
        }

        /**
         * Generate a new note using the new epoch
         *
         * @param newEpoch
         */
        void nextNote(long newEpoch, Digest view) {
            final var current = note;
            var n = current.newBuilder()
                           .setIdentifier(note.getIdentifier().toIdent())
                           .setEpoch(newEpoch)
                           .setMask(ByteString.copyFrom(nextMask().toByteArray()))
                           .setCurrentView(view.toDigeste())
                           .build();
            var signedNote = SignedNote.newBuilder()
                                       .setNote(n)
                                       .setSignature(wrapped.sign(n.toByteString()).toSig())
                                       .build();
            note = new NoteWrapper(signedNote, digestAlgo);
        }

        KeyState_ noteState() {
            return wrapped.getIdentifier().toKeyState_();
        }

        @Override
        void reset() {
            final var current = note;
            super.reset();
            var n = Note.newBuilder()
                        .setEpoch(0)
                        .setCurrentView(currentView().toDigeste())
                        .setEndpoint(current.getEndpoint())
                        .setIdentifier(current.getIdentifier().toIdent())
                        .setMask(ByteString.copyFrom(nextMask().toByteArray()))
                        .build();
            SignedNote signedNote = SignedNote.newBuilder()
                                              .setNote(n)
                                              .setSignature(wrapped.sign(n.toByteString()).toSig())
                                              .build();
            note = new NoteWrapper(signedNote, digestAlgo);
        }
    }

    public class Participant implements Member {

        private static final Logger log = LoggerFactory.getLogger(Participant.class);

        protected final    Digest              id;
        protected volatile NoteWrapper         note;
        protected volatile AccusationWrapper[] validAccusations;

        public Participant(Digest identity) {
            assert identity != null;
            this.id = identity;
            validAccusations = new AccusationWrapper[context.getRingCount()];
        }

        public Participant(NoteWrapper nw) {
            this(nw.getId());
            note = nw;
        }

        @Override
        public int compareTo(Member o) {
            return id.compareTo(o.getId());
        }

        public String endpoint() {
            final var current = note;
            if (current == null) {
                return null;
            }
            return current.getEndpoint();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Member m) {
                return compareTo(m) == 0;
            }
            return false;
        }

        @Override
        public Filtered filtered(SigningThreshold threshold, JohnHancock signature, InputStream message) {
            final var current = note;
            return View.this.filtered(getIdentifier(), threshold, signature, message);
        }

        public int getAccusationCount() {
            var count = 0;
            for (var acc : validAccusations) {
                if (acc != null) {
                    count++;
                }
            }
            return count;
        }

        public Iterable<? extends SignedAccusation> getEncodedAccusations() {
            return getAccusations().map(AccusationWrapper::getWrapped).toList();
        }

        @Override
        public Digest getId() {
            return id;
        }

        public SelfAddressingIdentifier getIdentifier() {
            return note.getIdentifier();
        }

        public SignedNote getSignedNote() {
            return note.getWrapped();
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }

        public boolean isDisabled(int ringNumber) {
            final var current = note;
            if (current != null) {
                return !current.getMask().get(ringNumber);
            }
            return false;
        }

        @Override
        public String toString() {
            return "Member[" + getId() + "]";
        }

        @Override
        public boolean verify(JohnHancock signature, InputStream message) {
            final var current = note;
            if (current == null) {
                return true;
            }
            return View.this.verify(getIdentifier(), signature, message);
        }

        @Override
        public boolean verify(SigningThreshold threshold, JohnHancock signature, InputStream message) {
            final var current = note;
            return View.this.verify(getIdentifier(), threshold, signature, message);
        }

        /**
         * Add an accusation to the member
         *
         * @param accusation
         */
        void addAccusation(AccusationWrapper accusation) {
            Integer ringNumber = accusation.getRingNumber();
            if (accusation.getRingNumber() >= validAccusations.length) {
                return;
            }
            NoteWrapper n = getNote();
            if (n == null) {
                validAccusations[ringNumber] = accusation;
                return;
            }
            if (n.getEpoch() != accusation.getEpoch()) {
                log.trace("Invalid epoch discarding accusation from: {} context: {} ring {} on: {}",
                          accusation.getAccuser(), getId(), ringNumber, node.getId());
                return;
            }
            if (n.getMask().get(ringNumber)) {
                validAccusations[ringNumber] = accusation;
                if (log.isDebugEnabled()) {
                    log.debug("Member: {} is accusing: {} context: {} ring: {} on: {}", accusation.getAccuser(),
                              accusation.getAccused(), getId(), ringNumber, node.getId());
                }
            }
        }

        /**
         * clear all accusations for the member
         */
        void clearAccusations() {
            for (var acc : validAccusations) {
                if (acc != null) {
                    log.trace("Clearing accusations for: {} context: {} on: {}", acc.getAccused(), getId(),
                              node.getId());
                    break;
                }
            }
            Arrays.fill(validAccusations, null);
        }

        AccusationWrapper getAccusation(int ring) {
            return validAccusations[ring];
        }

        Stream<AccusationWrapper> getAccusations() {
            return Arrays.stream(validAccusations).filter(Objects::nonNull);
        }

        long getEpoch() {
            NoteWrapper current = note;
            if (current == null) {
                return -1;
            }
            return current.getEpoch();
        }

        NoteWrapper getNote() {
            final var current = note;
            return current;
        }

        void invalidateAccusationOnRing(int index) {
            validAccusations[index] = null;
            log.trace("Invalidating accusations context: {} ring: {} on: {}", getId(), index, node.getId());
        }

        boolean isAccused() {
            for (var acc : validAccusations) {
                if (acc != null) {
                    return true;
                }
            }
            return false;
        }

        boolean isAccusedOn(int index) {
            if (index >= validAccusations.length) {
                return false;
            }
            return validAccusations[index] != null;
        }

        void reset() {
            note = null;
            validAccusations = new AccusationWrapper[context.getRingCount()];
        }

        boolean setNote(NoteWrapper next) {
            note = next;
            if (!shunned.contains(id)) {
                clearAccusations();
            }
            return true;
        }
    }

    public class Service implements EntranceService, FFService, ServiceRouting {

        /**
         * Asynchronously add a member to the next view
         */
        @Override
        public void join(Join join, Digest from, StreamObserver<Gateway> responseObserver, Timer.Context timer) {
            if (!started.get()) {
                responseObserver.onError(
                new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription("Not started")));
                return;
            }
            viewManagement.join(join, from, responseObserver, timer);
        }

        /**
         * The first message in the anti-entropy protocol. Process any digests from the inbound gossip digest. Respond
         * with the Gossip that represents the digests newer or not known in this view, as well as updates from this
         * node based on out-of-date information in the supplied digests.
         *
         * @param request - the Gossip from our partner
         * @return Teh response for Moar gossip - updates this node has which the sender is out of touch with, and
         * digests from the sender that this node would like updated.
         */
        @Override
        public Gossip rumors(SayWhat request, Digest from) {
            if (!introduced.get()) {
                log.trace("Not introduced; ring: {} from: {}, on: {}", request.getRing(), from, node.getId());
                throw new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription("Not introduced"));
            }
            return stable(() -> {
                final var ring = request.getRing();
                if (!context.validRing(ring)) {
                    log.debug("invalid gossip ring: {} from: {} on: {}", ring, from, node.getId());
                    throw new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription("invalid ring"));
                }
                validate(from, request);

                Participant member = context.getActiveMember(from);
                if (member == null) {
                    add(new NoteWrapper(request.getNote(), digestAlgo));
                    member = context.getActiveMember(from);
                    if (member == null) {
                        log.debug("Not active member: {} on: {}", from, node.getId());
                        throw new StatusRuntimeException(Status.PERMISSION_DENIED.withDescription("Not active member"));
                    }
                }

                Participant successor = context.successor(ring, member, m -> context.isActive(m.getId()));
                if (successor == null) {
                    log.debug("No active successor on ring: {} from: {} on: {}", ring, from, node.getId());
                    throw new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription("No active successor"));
                }

                Gossip g;
                var builder = Gossip.newBuilder();
                final var digests = request.getGossip();
                if (!successor.equals(node)) {
                    builder.setRedirect(successor.getNote().getWrapped());
                    log.debug("Redirected: {} to: {} on: {}", member.getId(), successor.id, node.getId());
                }
                g = builder.setNotes(processNotes(from, BloomFilter.from(digests.getNoteBff()), params.fpr()))
                           .setAccusations(
                           processAccusations(BloomFilter.from(digests.getAccusationBff()), params.fpr()))
                           .setObservations(
                           processObservations(BloomFilter.from(digests.getObservationBff()), params.fpr()))
                           .setJoins(viewManagement.processJoins(BloomFilter.from(digests.getJoinBiff()), params.fpr()))
                           .build();
                if (g.getNotes().getUpdatesCount() + g.getAccusations().getUpdatesCount() + g.getObservations()
                                                                                             .getUpdatesCount()
                + g.getJoins().getUpdatesCount() != 0) {
                    log.trace("Gossip for: {} notes: {} accusations: {} joins: {} observations: {} on: {}", from,
                              g.getNotes().getUpdatesCount(), g.getAccusations().getUpdatesCount(),
                              g.getJoins().getUpdatesCount(), g.getObservations().getUpdatesCount(), node.getId());
                }
                return g;
            });
        }

        @Override
        public Redirect seed(Registration registration, Digest from) {
            if (!started.get()) {
                throw new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription("Not started"));
            }
            return viewManagement.seed(registration, from);
        }

        /**
         * The third and final message in the anti-entropy protocol. Process the inbound update from another member.
         *
         * @param request - update state
         * @param from
         */
        @Override
        public void update(State request, Digest from) {
            if (!introduced.get()) {
                log.trace("Currently still being introduced, send unknown to: {}  on: {}", from, node.getId());
                return;
            }
            stable(() -> {
                validate(from, request);
                final var ring = request.getRing();
                if (!context.validRing(ring)) {
                    log.debug("invalid ring: {} current: {} from: {} on: {}", ring, currentView(), from, node.getId());
                    throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Invalid ring"));
                }
                Participant member = context.getActiveMember(from);
                Participant successor = context.successor(ring, member, m -> context.isActive(m.getId()));
                if (successor == null) {
                    log.debug("No successor, invalid update from: {} on ring: {} on: {}", from, ring, node.getId());
                    throw new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription("No successor"));
                }
                if (!successor.equals(node)) {
                    return;
                }
                final var update = request.getUpdate();
                if (!update.equals(Update.getDefaultInstance())) {
                    processUpdates(update.getNotesList(), update.getAccusationsList(), update.getObservationsList(),
                                   update.getJoinsList());
                }
            });
        }
    }
}
