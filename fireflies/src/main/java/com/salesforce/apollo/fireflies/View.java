/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import static com.salesforce.apollo.fireflies.communications.FfClient.getCreate;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.common.base.Objects;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.fireflies.proto.Accusation;
import com.salesfoce.apollo.fireflies.proto.AccusationGossip;
import com.salesfoce.apollo.fireflies.proto.Digests;
import com.salesfoce.apollo.fireflies.proto.Gossip;
import com.salesfoce.apollo.fireflies.proto.JoinGossip;
import com.salesfoce.apollo.fireflies.proto.Note;
import com.salesfoce.apollo.fireflies.proto.NoteGossip;
import com.salesfoce.apollo.fireflies.proto.SayWhat;
import com.salesfoce.apollo.fireflies.proto.SignedAccusation;
import com.salesfoce.apollo.fireflies.proto.SignedNote;
import com.salesfoce.apollo.fireflies.proto.SignedViewChange;
import com.salesfoce.apollo.fireflies.proto.State;
import com.salesfoce.apollo.fireflies.proto.Update;
import com.salesfoce.apollo.fireflies.proto.ViewChange;
import com.salesfoce.apollo.fireflies.proto.ViewChangeGossip;
import com.salesforce.apollo.comm.RingCommunications;
import com.salesforce.apollo.comm.RingCommunications.Destination;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.SigningThreshold;
import com.salesforce.apollo.fireflies.communications.FfServer;
import com.salesforce.apollo.fireflies.communications.Fireflies;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.Ring;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.EventValidation;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.RoundScheduler;
import com.salesforce.apollo.utils.Utils;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * The View is the active representation view of all members - failed and live -
 * known. The View interacts with other members on behalf of its Node and
 * monitors other members, issuing Accusations against failed members that this
 * View is monitoring. Accusations may be rebutted. Then there's discovery and
 * garbage collection. It's all very complicated. These complications and many
 * others are detailed in the wonderful
 * <a href= "https://ymsir.com/papers/fireflies-tocs.pdf">Fireflies paper</a>.
 *
 * @author hal.hildebrand
 * @since 220
 */
public class View {
    public record Seed(EventCoordinates coordinates, InetSocketAddress endpoint) {}

    /**
     * Used in set reconcillation of Accusation Digests
     */
    public record AccTag(Digest id, int ring) {}

    public interface CertToMember {
        Member from(X509Certificate cert);

        Digest idOf(X509Certificate cert);
    }

    public record CertWithHash(X509Certificate certificate, Digest certificateHash, byte[] derEncoded) {}

    public class Node extends Participant implements SigningMember {

        /**
         * Create a mask of length 2t+1 with t randomly disabled rings
         *
         * @param toleranceLevel - t
         * @return the mask
         */
        public static BitSet createInitialMask(int toleranceLevel) {
            int nbits = 2 * toleranceLevel + 1;
            BitSet mask = new BitSet(nbits);
            List<Boolean> random = new ArrayList<>();
            for (int i = 0; i < toleranceLevel + 1; i++) {
                random.add(true);
            }
            for (int i = 0; i < toleranceLevel; i++) {
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

        private final ControlledIdentifierMember wrapped;

        public Node(ControlledIdentifierMember wrapped, InetSocketAddress endpoint) {
            super(wrapped.getId());
            this.wrapped = wrapped;
            var n = Note.newBuilder()
                        .setEpoch(0)
                        .setHost(endpoint.getHostName())
                        .setPort(endpoint.getPort())
                        .setCoordinates(wrapped.getEvent().getCoordinates().toEventCoords())
                        .build();
            var signedNote = SignedNote.newBuilder()
                                       .setNote(n)
                                       .setSignature(wrapped.sign(n.toByteString()).toSig())
                                       .build();
            note = new NoteWrapper(signedNote, digestAlgo);
        }

        @Override
        public SignatureAlgorithm algorithm() {
            return wrapped.algorithm();
        }

        public JohnHancock sign(byte[] message) {
            return wrapped.sign(message);
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
                                       .build();
            return new AccusationWrapper(SignedAccusation.newBuilder()
                                                         .setAccusation(accusation)
                                                         .setSignature(wrapped.sign(accusation.toByteString()).toSig())
                                                         .build(),
                                         digestAlgo);
        }

        /**
         * @return a new mask based on the previous mask and previous accusations.
         */
        BitSet nextMask() {
            NoteWrapper current = note;
            if (current == null) {
                BitSet mask = createInitialMask(context.toleranceLevel());
                assert View.isValidMask(mask, context.toleranceLevel()) : "Invalid initial mask: " + mask + "for node: "
                + getId();
                return mask;
            }

            BitSet mask = new BitSet(context.getRingCount());
            mask.flip(0, context.getRingCount());
            for (int i = 0; i < context.getRingCount(); i++) {
                if (validAccusations[i] == null) {
                    continue;
                }
                if (mask.cardinality() <= context.toleranceLevel() + 1) {
                    assert isValidMask(mask, context.toleranceLevel()) : "Invalid mask: " + mask + "for node: "
                    + getId();
                    return mask;
                }
                mask.set(i, false);
            }
            if (current.getEpoch() % 2 == 1) {
                BitSet previous = BitSet.valueOf(current.getMask().toByteArray());
                for (int index = 0; index < context.getRingCount(); index++) {
                    if (mask.cardinality() <= context.toleranceLevel() + 1) {
                        assert View.isValidMask(mask, context.toleranceLevel()) : "Invalid mask: " + mask + "for node: "
                        + getId();
                        return mask;
                    }
                    if (!previous.get(index)) {
                        mask.set(index, false);
                    }
                }
            } else {
                // Fill the rest of the mask with randomly set index
                while (mask.cardinality() > context.toleranceLevel() + 1) {
                    int index = Entropy.nextSecureInt(context.getRingCount());
                    if (mask.get(index)) {
                        mask.set(index, false);
                    }
                }
            }
            assert isValidMask(mask, context.toleranceLevel()) : "Invalid mask: " + mask + "for node: " + getId();
            return mask;
        }

        /**
         * Generate a new note for the member based on any previous note and previous
         * accusations. The new note has a larger epoch number the the current note.
         */
        void nextNote() {
            NoteWrapper current = note;
            long newEpoch = current == null ? 1 : note.getEpoch() + 1;
            nextNote(newEpoch);
        }

        /**
         * Generate a new note using the new epoch
         *
         * @param newEpoch
         */
        void nextNote(long newEpoch) {
            var n = note.newBuilder()
                        .setCoordinates(wrapped.getEvent().getCoordinates().toEventCoords())
                        .setEpoch(newEpoch)
                        .setMask(ByteString.copyFrom(nextMask().toByteArray()))
                        .build();
            var signedNote = SignedNote.newBuilder()
                                       .setNote(n)
                                       .setSignature(wrapped.sign(n.toByteString()).toSig())
                                       .build();
            note = new NoteWrapper(signedNote, digestAlgo);
        }
    }

    public class Participant implements Member {

        private static final Logger log = LoggerFactory.getLogger(Participant.class);

        protected final Digest              id;
        protected volatile NoteWrapper      note;
        protected final AccusationWrapper[] validAccusations;

        public Participant(Digest identity) {
            assert identity != null;
            this.id = identity;
            validAccusations = new AccusationWrapper[context.getRingCount()];
        }

        @Override
        public int compareTo(Member o) {
            return id.compareTo(o.getId());
        }

        public SocketAddress endpoint() {
            if (note == null) {
                return null;
            }
            return new InetSocketAddress(note.getHost(), note.getPort());
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
            return validation.filtered(note.getCoordinates(), threshold, signature, message);
        }

        @Override
        public Digest getId() {
            return id;
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }

        @Override
        public String toString() {
            return "Member[" + getId() + "]";
        }

        @Override
        public boolean verify(JohnHancock signature, InputStream message) {
            return validation.verify(note.getCoordinates(), signature, message);
        }

        @Override
        public boolean verify(SigningThreshold threshold, JohnHancock signature, InputStream message) {
            return validation.verify(note.getCoordinates(), threshold, signature, message);
        }

        /**
         * Add an accusation to the member
         *
         * @param accusation
         */
        void addAccusation(AccusationWrapper accusation) {
            Integer ringNumber = accusation.getRingNumber();
            NoteWrapper n = getNote();
            if (n == null) {
                validAccusations[ringNumber] = accusation;
                return;
            }
            if (n.getEpoch() != accusation.getEpoch()) {
                log.trace("Invalid epoch discarding accusation from {} on {} ring {}", accusation.getAccuser(), getId(),
                          ringNumber);
                return;
            }
            if (n.getMask().get(ringNumber)) {
                validAccusations[ringNumber] = accusation;
                if (log.isDebugEnabled()) {
                    log.debug("Member {} is accusing {} on {}", accusation.getAccuser(), getId(), ringNumber);
                }
            }
        }

        /**
         * clear all accusations for the member
         */
        void clearAccusations() {
            Arrays.fill(validAccusations, null);
            log.trace("Clearing accusations for {} on: {}", getId(), node.getId());
        }

        AccusationWrapper getAccusation(int ring) {
            return validAccusations[ring];
        }

        Stream<AccusationWrapper> getAccusations() {
            var returned = new ArrayList<AccusationWrapper>();
            for (var acc : validAccusations) {
                if (acc != null) {
                    returned.add(acc);
                }
            }
            return returned.stream();
        }

        List<AccTag> getAccusationTags() {
            var returned = new ArrayList<AccTag>();
            for (int ring = 0; ring < validAccusations.length; ring++) {
                if (validAccusations[ring] != null) {
                    returned.add(new AccTag(getId(), ring));
                }
            }
            return returned;
        }

        AccusationWrapper getEncodedAccusation(Integer ring) {
            return validAccusations[ring];
        }

        List<SignedAccusation> getEncodedAccusations(int rings) {
            return IntStream.range(0, rings)
                            .mapToObj(i -> getEncodedAccusation(i))
                            .filter(e -> e != null)
                            .map(e -> e.getWrapped())
                            .collect(Collectors.toList());
        }

        long getEpoch() {
            NoteWrapper current = note;
            if (current == null) {
                return 0;
            }
            return current.getEpoch();
        }

        NoteWrapper getNote() {
            return note;
        }

        void invalidateAccusationOnRing(int index) {
            validAccusations[index] = null;
            log.trace("Invalidating accusations of {} on {}", getId(), index);
        }

        boolean isAccused() {
            for (int ring = 0; ring < validAccusations.length; ring++) {
                if (validAccusations[ring] != null) {
                    return true;
                }
            }
            return false;
        }

        boolean isAccusedOn(int index) {
            return validAccusations[index] != null;
        }

        void reset() {
            note = null;
            Arrays.fill(validAccusations, null);
            log.trace("Reset {}", getId());
        }

        boolean setNote(NoteWrapper next) {
            note = next;
            clearAccusations();
            return true;
        }
    }

    public class Service {

        /**
         * The first message in the anti-entropy protocol. Process any digests from the
         * inbound gossip digest. Respond with the Gossip that represents the digests
         * newer or not known in this view, as well as updates from this node based on
         * out of date information in the supplied digests.
         *
         * @param ring    - the index of the gossip ring the inbound member is gossiping
         *                on
         * @param request - the Gossip from our partner
         * @return Teh response for Moar gossip - updates this node has which the sender
         *         is out of touch with, and digests from the sender that this node
         *         would like updated.
         */
        public Gossip rumors(SayWhat request, Digest from) {
            final var ring = request.getRing();
            final var requestView = Digest.from(request.getView());
            if (!requestView.equals(currentView.get())) {
                log.debug("invalid view: {} current: {} from: {} on: {}", requestView, currentView.get(), ring, from,
                          node.getId());
                return Gossip.getDefaultInstance();
            }
            if (ring >= context.getRingCount() || ring < 0) {
                log.debug("invalid ring: {} from: {} on: {}", ring, from, node.getId());
                return Gossip.getDefaultInstance();
            }
            var wrapper = new NoteWrapper(request.getNote(), digestAlgo);
            if (!from.equals(wrapper.getId())) {
                log.debug("invalid identity on ring: {} from: {} on: {}", ring, from, node.getId());
                return Gossip.getDefaultInstance();
            }

            return stable(() -> {
                Participant member;
                if (!add(wrapper)) {
                    member = new Participant(wrapper.getId());
                } else {
                    member = context.getMember(from);
                }

                Participant successor = context.ring(ring).successor(member, m -> context.isActive(m.getId()));
                if (successor == null) {
                    log.debug("No active successor on ring: {} from: {} on: {}", ring, from, node.getId());
                    return Gossip.getDefaultInstance();
                }
                if (!successor.equals(node)) {
                    return redirectTo(member, ring, successor);
                }
                long seed = Entropy.nextSecureLong();
                final var digests = request.getGossip();
                return Gossip.newBuilder()
                             .setRedirect(false)
                             .setNotes(processNotes(from, BloomFilter.from(digests.getNoteBff()), seed, fpr))
                             .setAccusations(processAccusations(BloomFilter.from(digests.getAccusationBff()), seed,
                                                                fpr))
                             .setObservations(processObservations(BloomFilter.from(digests.getObservationBff()), seed,
                                                                  fpr))
                             .setJoins(processJoins(BloomFilter.from(digests.getJoinBiff()), seed, fpr))
                             .build();
            });
        }

        /**
         * The third and final message in the anti-entropy protocol. Process the inbound
         * update from another member.
         *
         * @param state - update state
         * @param from
         */
        public void update(State request, Digest from) {
            Participant member = context.getActiveMember(from);
            final var ring = request.getRing();
            if (member == null) {
                log.debug("No member, invalid update from: {} on ring: {} on: {}", from, ring, from);
                return;
            }
            final var requestView = Digest.from(request.getView());
            if (!requestView.equals(currentView.get())) {
                log.debug("invalid view: {} current: {} from: {} on: {}", requestView, currentView.get(), ring, from,
                          node.getId());
                return;
            }
            Participant successor = context.ring(ring).successor(member, m -> context.isActive(m.getId()));
            if (successor == null) {
                log.debug("No successor, invalid update from: {} on ring: {} on: {}", from, ring, node.getId());
                return;
            }
            if (!successor.equals(node)) {
                log.debug("Incorrect predecessor, invalid update from: {} on ring: {} on: {}", from, ring,
                          member.getId());
                return;
            }
            final var update = request.getUpdate();
            stable(() -> {
                processUpdates(update.getNotesList(), update.getAccusationsList(), update.getObservationsList(),
                               update.getJoinsList());
            });
        }
    }

    private record Ballot(Digest view, List<Digest> leaving, List<Digest> joining, int hash) {

        private Ballot(Digest view, List<Digest> leaving, List<Digest> joining, DigestAlgorithm algo) {
            this(view, leaving, joining,
                 Objects.hashCode(view, leaving.stream().reduce((a, b) -> a.xor(b)).orElse(algo.getOrigin()),
                                  joining.stream().reduce((a, b) -> a.xor(b)).orElse(algo.getOrigin())));
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
    }

    private static final String FINALIZE_VIEW_CHANGE = "FINALIZE VIEW CHANGE";

    private static final Logger log = LoggerFactory.getLogger(View.class);

    private static final int    REBUTAL_TIMEOUT       = 2;
    private static final String SCHEDULED_VIEW_CHANGE = "Scheduled View Change";

    /**
     * Check the validity of a mask. A mask is valid if the following conditions are
     * satisfied:
     *
     * <pre>
     * - The mask is of length 2t+1
     * - the mask has exactly t + 1 enabled elements.
     * </pre>
     *
     * @param mask
     * @return
     */
    public static boolean isValidMask(BitSet mask, int toleranceLevel) {
        return mask.cardinality() == toleranceLevel + 1;
    }

    private final CommonCommunications<Fireflies, Service>    comm;
    private final Context<Participant>                        context;
    private final AtomicReference<Digest>                     currentView         = new AtomicReference<>();
    private final DigestAlgorithm                             digestAlgo;
    private final Executor                                    exec;
    private final AtomicReference<RoundScheduler.Timer>       finalizeViewChange  = new AtomicReference<>();
    private final double                                      fpr;
    private volatile ScheduledFuture<?>                       futureGossip;
    private final RingCommunications<Participant, Fireflies>  gossiper;
    private final Map<Digest, SignedNote>                     joins               = new ConcurrentSkipListMap<>();
    private final FireflyMetrics                              metrics;
    private final Node                                        node;
    private final Map<Digest, SignedViewChange>               observations        = new ConcurrentSkipListMap<>();
    private final ConcurrentMap<Digest, RoundScheduler.Timer> pendingRebutals     = new ConcurrentSkipListMap<>();
    private final RoundScheduler                              roundTimers;
    private final AtomicReference<RoundScheduler.Timer>       scheduledViewChange = new AtomicReference<>();
    private final Service                                     service             = new Service();
    private final Set<Digest>                                 shunned             = Collections.newSetFromMap(new ConcurrentSkipListMap<>());
    private final AtomicBoolean                               started             = new AtomicBoolean();
    private final EventValidation                             validation;
    private final ReentrantReadWriteLock                      viewChange          = new ReentrantReadWriteLock();
    private volatile BloomFilter<Digest>                      votesReceived;

    public View(Context<Participant> context, ControlledIdentifierMember member, InetSocketAddress endpoint,
                EventValidation validation, Router communications, double fpr, DigestAlgorithm digestAlgo,
                FireflyMetrics metrics, Executor exec) {
        this.metrics = metrics;
        this.validation = validation;
        this.fpr = fpr;
        this.digestAlgo = digestAlgo;
        this.context = context;
        this.roundTimers = new RoundScheduler(String.format("Timers for: %s", context.getId()), context.timeToLive());
        this.node = new Node(member, endpoint);
        this.comm = communications.create(node, context.getId(), service,
                                          r -> new FfServer(service, communications.getClientIdentityProvider(), r,
                                                            exec, metrics),
                                          getCreate(metrics), Fireflies.getLocalLoopback(node));
        gossiper = new RingCommunications<>(context, node, comm, exec);
        this.exec = exec;
        add(node);
        votesReceived = new BloomFilter.DigestBloomFilter(Entropy.nextBitsStreamLong(), context.cardinality(), 0.00001);
    }

    /**
     * 
     * @return the context of the view
     */
    public Context<Participant> getContext() {
        return context;
    }

    /**
     * Start the View
     */
    public void start(Duration d, List<Seed> seeds, ScheduledExecutorService scheduler) {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        currentView.set(digestAlgo.getOrigin());
        roundTimers.reset();
        comm.register(context.getId(), service);
        context.allMembers().forEach(e -> e.clearAccusations());
        context.activate(node);
        node.nextNote();
        context.activate(node);
        seeds.forEach(m -> addSeed(m));
        roundTimers.schedule(SCHEDULED_VIEW_CHANGE, () -> maybeViewChange(), 7);

        var initial = Entropy.nextBitsStreamLong(d.toNanos());
        futureGossip = scheduler.schedule(() -> gossip(d, scheduler), initial, TimeUnit.NANOSECONDS);

        log.info("{} started, seeds: {}", node.getId(),
                 seeds.stream()
                      .map(s -> s.coordinates.getIdentifier())
                      .map(i -> (SelfAddressingIdentifier) i)
                      .map(sai -> sai.getDigest())
                      .toList());
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
        pendingRebutals.clear();
        context.active().forEach(m -> {
            context.offline(m);
            m.clearAccusations();
        });
        final var current = futureGossip;
        futureGossip = null;
        if (current != null) {
            current.cancel(true);
        }
        shunned.clear();
        observations.clear();
        joins.clear();
        votesReceived.clear();
        if (finalizeViewChange.get() != null) {
            finalizeViewChange.get().cancel();
            finalizeViewChange.set(null);
        }
        if (scheduledViewChange.get() != null) {
            scheduledViewChange.get().cancel();
            scheduledViewChange.set(null);
        }
    }

    @Override
    public String toString() {
        return "View[" + node.getId() + "]";
    }

    /**
     * Test accessible
     * 
     * @return The member that represents this View
     */
    Node getNode() {
        return node;
    }

    /**
     * Accuse the member on the ring
     *
     * @param member
     * @param ring
     */
    private void accuse(Participant member, int ring) {
        if (member.isAccusedOn(ring)) {
            return; // Don't issue multiple accusations
        }
        member.addAccusation(node.accuse(member, ring));
        pendingRebutals.computeIfAbsent(member.getId(), d -> roundTimers.schedule(() -> gc(member), REBUTAL_TIMEOUT));
        if (metrics != null) {
            metrics.accusations().mark();
        }
        log.debug("Accuse {} on ring {} (timer started) on: {}", member.getId(), ring, node.getId());
    }

    /**
     * Add an inbound accusation to the view.
     *
     * @param accusation
     */
    private void add(AccusationWrapper accusation) {
        Participant accuser = context.getMember(accusation.getAccuser());
        Participant accused = context.getMember(accusation.getAccused());
        if (accuser == null || accused == null) {
            log.trace("Accusation discarded, accused or accuser do not exist in view on: {}", node);
            return;
        }

        if (accusation.getRingNumber() > context.getRingCount()) {
            log.trace("Invalid ring in accusation: {} on: {}", accusation.getRingNumber(), node);
            return;
        }

        if (accused.getEpoch() != accusation.getEpoch()) {
            log.trace("Accusation discarded in epoch: {}  for: {} epoch: {} on: {}", accusation.getEpoch(),
                      accused.getId(), accused.getEpoch(), node);
            return;
        }

        if (!accused.getNote().getMask().get(accusation.getRingNumber())) {
            log.trace("Member {} accussed on disabled ring {} by {} on: {}", accused.getId(),
                      accusation.getRingNumber(), accuser.getId(), node);
            return;
        }

        if (!accuser.verify(accusation.getSignature(), accusation.getWrapped().getAccusation().toByteString())) {
            log.trace("Accusation by: {} accused:{} signature invalid on: {}", accuser.getId(), accused.getId(),
                      node.getId());
            return;
        }

        add(accusation, accuser, accused);
    }

    /**
     * Add an accusation into the view,
     *
     * @param accusation
     * @param accuser
     * @param accused
     */
    private void add(AccusationWrapper accusation, Participant accuser, Participant accused) {
        if (node.equals(accused)) {
            node.clearAccusations();
            node.nextNote();
            return;
        }
        Ring<Participant> ring = context.ring(accusation.getRingNumber());

        if (accused.isAccusedOn(ring.getIndex())) {
            Participant currentAccuser = context.getMember(accused.getAccusation(ring.getIndex()).getAccuser());

            if (currentAccuser != null && !currentAccuser.equals(accuser) &&
                ring.isBetween(currentAccuser, accuser, accused)) {
                accused.addAccusation(accusation);
                log.debug("{} accused by {} on ring {} (replacing {}) on: {}", accused.getId(), accuser.getId(),
                          ring.getIndex(), currentAccuser, node.getId());
            }
        } else {
            Participant predecessor = ring.predecessor(accused, m -> (!m.isAccused()) || (m.equals(accuser)));
            if (accuser.equals(predecessor)) {
                accused.addAccusation(accusation);
                if (!accused.equals(node) && !pendingRebutals.containsKey(accused.getId()) &&
                    context.isActive(accused.getId())) {
                    log.debug("{} accused by {} on ring {} (timer started) on: {}", accused.getId(), accuser.getId(),
                              accusation.getRingNumber(), node.getId());
                    pendingRebutals.computeIfAbsent(accused.getId(),
                                                    d -> roundTimers.schedule(() -> gc(accused), REBUTAL_TIMEOUT));
                }
            } else {
                log.debug("{} accused by {} on ring {} discarded as not predecessor {} on: {}", accused.getId(),
                          accuser.getId(), accusation.getRingNumber(), predecessor.getId(), node.getId());
            }
        }
    }

    /**
     * add an inbound note to the view
     *
     * @param note
     */
    private boolean add(NoteWrapper note) {
        if (metrics != null) {
            metrics.notes().mark();
        }
        Participant m = context.getMember(note.getId());
        if (m == null) {
            if (!validation.verify(note.getCoordinates(), note.getSignature(),
                                   note.getWrapped().getNote().toByteString())) {
                log.warn("invalid participant note from {} on: {}", note.getId(), node.getId());
            }
            m = new Participant(note.getId());
            m.setNote(note);
            add(m);
        } else {
            if (m.getEpoch() >= note.getEpoch()) {
                return false;
            }
        }

        if (!isValidMask(note.getMask(), context.toleranceLevel())) {
            log.trace("Note: {} mask invalid {} on: {}", note.getId(), note.getMask(), node.getId());
            return false;
        }

        if (!m.verify(note.getSignature(), note.getWrapped().getNote().toByteString())) {
            log.trace("Note signature invalid: {} on: {}", note.getId(), node.getId());
            return false;
        }

        NoteWrapper current = m.getNote();
        if (current != null) {
            long nextEpoch = note.getEpoch();
            long currentEpoch = current.getEpoch();
            if (currentEpoch > 0 && currentEpoch < nextEpoch - 1) {
                log.trace("discarding note for {} with wrong previous epoch {} current: {} on: {}", m.getId(),
                          nextEpoch, currentEpoch, node.getId());
                return false;
            }
        }

        log.debug("Adding member via note {} on: {}", m, node.getId());

        stopRebutalTimer(m);

        if (m.isAccused()) {
            checkInvalidations(m);
        }

        m.setNote(note);
        recover(m);
        return true;
    }

    /**
     * Add a new member to the view
     *
     * @param member
     */
    private Participant add(Participant member) {
        Participant previous = context.getMember(member.getId());
        if (previous == null) {
            context.add(member);
            return member;
        }
        return previous;
    }

    /**
     * Add an observation if it is for the current view and has not been previously
     * observed by the observer
     *
     * @param observation
     */
    private void add(SignedViewChange observation) {
        var member = context.getMember(Digest.from(observation.getChange().getObserver()));
        if (member == null) {
            return;
        }
        if (!currentView.get().equals(Digest.from(observation.getChange().getCurrent()))) {
            return;
        }
        if (votesReceived.contains(member.getId())) {
            return;
        }
        votesReceived.add(member.getId());
        final var signature = JohnHancock.from(observation.getSignature());
        if (!member.verify(signature, observation.getChange().toByteString())) {
            return;
        }
        observations.put(signature.toDigest(digestAlgo), observation);
    }

    private void addJoin(SignedNote sn) {
        final var note = new NoteWrapper(sn, digestAlgo);
        if (!validation.verify(note.getCoordinates(), note.getSignature(),
                               note.getWrapped().getNote().toByteString())) {
            log.warn("Invalid join note from {} on: {}", note.getId(), node.getId());
            return;
        }
        if (!isValidMask(note.getMask(), context.toleranceLevel())) {
            log.trace("Invalid join note from: {} mask invalid {} on: {}", note.getId(), note.getMask(), node.getId());
            return;
        }

        if (!validation.verify(note.getCoordinates(), note.getSignature(),
                               note.getWrapped().getNote().toByteString())) {
            log.trace("Note signature invalid: {} on: {}", note.getId(), node.getId());
            return;
        }
        joins.put(note.getHash(), note.getWrapped());
    }

    /**
     * For bootstrap, add the seed as a fake, non crashed member. The note is signed
     * by this node, and so will be rejected by any other node as invalid. The
     * gossip protocol of Fireflies provides redirects to what actual members
     * believe the view should be gossiping with, and so new members quickly
     * converge on a common, valid view of the membership
     *
     * @param seed
     */
    private void addSeed(Seed seed) {
        SignedNote seedNote = SignedNote.newBuilder()
                                        .setNote(Note.newBuilder()
                                                     .setHost(seed.endpoint.getHostName())
                                                     .setPort(seed.endpoint.getPort())
                                                     .setCoordinates(seed.coordinates.toEventCoords())
                                                     .setEpoch(-1)
                                                     .setMask(ByteString.copyFrom(Node.createInitialMask(context.toleranceLevel())
                                                                                      .toByteArray())))
                                        .setSignature(SignatureAlgorithm.NULL_SIGNATURE.sign(null, new byte[0]).toSig())
                                        .build();
        var participant = new Participant(((SelfAddressingIdentifier) seed.coordinates.getIdentifier()).getDigest());
        participant.setNote(new NoteWrapper(seedNote, digestAlgo));
        context.activate(participant);
    }

    /**
     * If we monitor the target and haven't issued an alert, do so
     * 
     * @param sa
     */
    private void amplify(Participant target) {
        context.rings().filter(ring -> target.equals(ring.successor(node, m -> context.isActive(m)))).forEach(ring -> {
            log.trace("amplifying: {} ring: {} on: {}", target.getId(), ring.getIndex(), node.getId());
            accuse(target, ring.getIndex());
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
        long seed = Entropy.nextSecureLong();
        return Digests.newBuilder()
                      .setAccusationBff(getAccusationsBff(seed, fpr).toBff())
                      .setNoteBff(getNotesBff(seed, fpr).toBff())
                      .build();
    }

    /**
     * Fallback consensus driven view change using Ethereal
     */
    private void consensusViewChange() {
        log.trace("Fast path view change failed, starting consensus view change on: {}", node.getId());
        // TODO
    }

    /**
     * Finalize the view change
     */
    private void finalizeViewChange() {
        final var lock = viewChange.writeLock();
        lock.lock();
        try {
            HashMultiset<Ballot> ballots = HashMultiset.create();
            observations.values().forEach(vc -> {
                final var leaving = vc.getChange()
                                      .getLeavesList()
                                      .stream()
                                      .map(d -> Digest.from(d))
                                      .collect(Collectors.toList());
                final var joining = vc.getChange()
                                      .getJoinsList()
                                      .stream()
                                      .map(d -> Digest.from(d))
                                      .collect(Collectors.toList());
                leaving.sort(Ordering.natural());
                joining.sort(Ordering.natural());
                ballots.add(new Ballot(Digest.from(vc.getChange().getCurrent()), leaving, joining, digestAlgo));
            });
            var max = ballots.entrySet()
                             .stream()
                             .max(Ordering.natural().onResultOf(Multiset.Entry::getCount))
                             .orElse(null);
            final var superMajority = context.cardinality() - ((context.cardinality() - 1) / 4);
            if (max != null && max.getCount() >= superMajority) {
                install(max.getElement());
            } else {
                consensusViewChange();
            }
        } finally {
            roundTimers.schedule(SCHEDULED_VIEW_CHANGE, () -> maybeViewChange(), 7);
            observations.clear();
            finalizeViewChange.set(null);
            lock.unlock();
        }
    }

    /**
     * Garbage collect the member. Member is now shunned and cannot recover
     *
     * @param member
     */
    private void gc(Participant member) {
        if (shunned.add(member.getId())) {
            amplify(member);
        }
        log.trace("Garbage collecting: {} on: {}", member.getId(), node.getId());
        context.offline(member);
    }

    /**
     * @param seed
     * @param p
     * @return the bloom filter containing the digests of known accusations
     */
    private BloomFilter<Digest> getAccusationsBff(long seed, double p) {
        BloomFilter<Digest> bff = new BloomFilter.DigestBloomFilter(seed, context.cardinality(), p);
        context.allMembers().flatMap(m -> m.getAccusations()).filter(e -> e != null).forEach(m -> bff.add(m.getHash()));
        return bff;
    }

    /**
     * @param seed
     * @param p
     * @return the bloom filter containing the digests of known joins
     */
    private BloomFilter<Digest> getJoinsBff(long seed, double p) {
        BloomFilter<Digest> bff = new BloomFilter.DigestBloomFilter(seed, context.cardinality(), p);
        joins.keySet().forEach(d -> bff.add(d));
        return bff;
    }

    /**
     * @param seed
     * @param p
     * @return the bloom filter containing the digests of known notes
     */
    private BloomFilter<Digest> getNotesBff(long seed, double p) {
        BloomFilter<Digest> bff = new BloomFilter.DigestBloomFilter(seed, context.cardinality() * 2, p);
        context.active().map(m -> m.getNote()).filter(e -> e != null).forEach(n -> bff.add(n.getHash()));
        return bff;
    }

    /**
     * @param seed
     * @param p
     * @return the bloom filter containing the digests of known observations
     */
    private BloomFilter<Digest> getObservationsBff(long seed, double p) {
        BloomFilter<Digest> bff = new BloomFilter.DigestBloomFilter(seed, context.cardinality() * 2, p);
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

        exec.execute(Utils.wrapped(() -> {
            roundTimers.tick();
            stable(() -> {
                var timer = metrics == null ? null : metrics.gossipRoundDuration().time();
                gossiper.execute((link, ring) -> gossip(link, ring),
                                 (futureSailor, destination) -> gossip(futureSailor, destination, duration, scheduler,
                                                                       timer));
            });
        }, log));
    }

    /**
     * Gossip with the member
     *
     * @param ring       - the index of the gossip ring the gossip is originating
     *                   from in this view
     * @param link       - the outbound communications to the paired member
     * @param completion
     * @throws Exception
     */
    private ListenableFuture<Gossip> gossip(Fireflies link, int ring) {
        NoteWrapper n = node.getNote();
        if (n == null) {
            return null;
        }
        SignedNote signedNote = n.getWrapped();
        Digests outbound = commonDigests();
        return link.gossip(SayWhat.newBuilder()
                                  .setContext(context.getId().toDigeste())
                                  .setView(currentView.get().toDigeste())
                                  .setNote(signedNote)
                                  .setRing(ring)
                                  .setGossip(outbound)
                                  .build());
    }

    /**
     * Handle the gossip response from the destination
     *
     * @param futureSailor
     * @param destination
     * @param duration
     * @param scheduler
     * @param timer
     */
    private void gossip(Optional<ListenableFuture<Gossip>> futureSailor,
                        Destination<Participant, Fireflies> destination, Duration duration,
                        ScheduledExecutorService scheduler, Timer.Context timer) {
        if (timer != null) {
            timer.close();
        }
        if (futureSailor.isEmpty()) {
            if (destination.member() != null) {
                accuse(destination.member(), destination.ring());
            }
            futureGossip = scheduler.schedule(() -> gossip(duration, scheduler), duration.toNanos(),
                                              TimeUnit.NANOSECONDS);
            return;
        }

        try {
            Gossip gossip = futureSailor.get().get();
            if (gossip.getRedirect()) {
                redirect(destination.member(), gossip, destination.ring());
            } else {
                Update update = response(gossip);
                if (!update.equals(Update.getDefaultInstance())) {
                    destination.link()
                               .update(State.newBuilder()
                                            .setContext(context.getId().toDigeste())
                                            .setView(currentView.get().toDigeste())
                                            .setRing(destination.ring())
                                            .setUpdate(update)
                                            .build());
                }
            }
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.NOT_FOUND.getCode() ||
                e.getStatus().getCode() == Status.UNAVAILABLE.getCode() ||
                e.getStatus().getCode() == Status.UNKNOWN.getCode()) {
                log.trace("Cannot find/unknown: {} on: {}", destination.member(), node.getId());
            } else {
                log.warn("Exception gossiping with {} on: {}", destination.member(), node.getId(), e);
            }
            accuse(destination.member(), destination.ring());
        } catch (ExecutionException e) {
            var cause = e.getCause();
            if (cause instanceof StatusRuntimeException sre) {
                if (sre.getStatus().getCode() == Status.NOT_FOUND.getCode() ||
                    sre.getStatus().getCode() == Status.UNAVAILABLE.getCode() ||
                    sre.getStatus().getCode() == Status.UNKNOWN.getCode()) {
                    log.trace("Cannot find/unknown: {} on: {}", destination.member(), node.getId());
                } else {
                    log.warn("Exception gossiping with {} on: {}", destination.member(), node.getId(), e.getCause());
                }
            } else {
                log.warn("Exception gossiping with {} on: {}", destination.member(), node.getId(), cause);
            }
            accuse(destination.member(), destination.ring());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        futureGossip = scheduler.schedule(() -> gossip(duration, scheduler), duration.toNanos(), TimeUnit.NANOSECONDS);

    }

    /**
     * Initiate the view change
     */
    private void initiateViewChange() {
        final var lock = viewChange.writeLock();
        lock.lock();
        try {
            var change = ViewChange.newBuilder()
                                   .setObserver(node.getId().toDigeste())
                                   .setCurrent(currentView.get().toDigeste())
                                   .addAllLeaves(shunned.stream().map(d -> d.toDigeste()).toList())
                                   .addAllJoins(joins.keySet().stream().map(d -> d.toDigeste()).toList())
                                   .build();
            var signature = node.sign(change.toByteString());
            observations.put(signature.toDigest(digestAlgo),
                             SignedViewChange.newBuilder().setChange(change).setSignature(signature.toSig()).build());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Install the new view
     * 
     * @param view
     */
    private void install(Ballot view) {
        final var evicted = new AtomicBoolean();
        view.leaving.stream().filter(d -> !node.getId().equals(d)).forEach(p -> remove(p));
        view.joining.stream()
                    .map(d -> joins.get(d))
                    .filter(sn -> sn != null)
                    .map(sn -> new NoteWrapper(sn, digestAlgo))
                    .forEach(sn -> add(sn));

        final var previousView = currentView.get();
        currentView.set(context.ring(0)
                               .stream()
                               .map(p -> p.getId())
                               .reduce((a, b) -> a.xor(b))
                               .orElse(digestAlgo.getOrigin()));

        log.warn("Installing new view: {} from: {} leaving: {} joining: {} evicted: {} on: {}", currentView.get(),
                 previousView, view.leaving, view.joining, evicted.get(), node.getId());
    }

    /**
     * If member currently is accused on ring, keep the new accusation only if it is
     * from a closer predecessor.
     *
     * @param q
     * @param ring
     * @param check
     */
    private void invalidate(Participant q, Ring<Participant> ring, Deque<Participant> check) {
        AccusationWrapper qa = q.getAccusation(ring.getIndex());
        Participant accuser = context.getMember(qa.getAccuser());
        Participant accused = context.getMember(qa.getAccused());
        if (ring.isBetween(accuser, q, accused)) {
            assert q.isAccused();
            q.invalidateAccusationOnRing(ring.getIndex());
            if (!q.isAccused()) {
                stopRebutalTimer(q);
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

    /**
     * start a view change if there's any offline members or joining members
     */
    private void maybeViewChange() {
        if (shunned.size() > 0 || joins.size() > 0) {
            var timer = roundTimers.schedule(FINALIZE_VIEW_CHANGE, () -> finalizeViewChange(), 7);
            if (finalizeViewChange.compareAndSet(null, timer)) {
                initiateViewChange();
            } else {
                timer.cancel();
            }
        } else {
            scheduledViewChange.set(roundTimers.schedule(SCHEDULED_VIEW_CHANGE, () -> maybeViewChange(), 7));
        }
    }

    /**
     * Process the inbound accusations from the gossip. Reconcile the differences
     * between the view's state and the digests of the gossip. Update the reply with
     * the list of digests the view requires, as well as proposed updates based on
     * the inbound digets that the view has more recent information. Do not forward
     * accusations from crashed members
     *
     * @param digests
     * @param p
     * @param seed
     * @return
     */
    private AccusationGossip processAccusations(BloomFilter<Digest> bff, long seed, double p) {
        AccusationGossip.Builder builder = AccusationGossip.newBuilder();
        // Add all updates that this view has that aren't reflected in the inbound
        // bff
        context.active()
               .flatMap(m -> m.getAccusations())
               .filter(a -> !bff.contains(a.getHash()))
               .forEach(a -> builder.addUpdates(a.getWrapped()));
        builder.setBff(getAccusationsBff(seed, p).toBff());
        if (builder.getUpdatesCount() != 0) {
            log.trace("process accusations produced updates: {} on: {}", builder.getUpdatesCount(), node.getId());
        }
        return builder.build();
    }

    /**
     * Process the inbound joins from the gossip. Reconcile the differences between
     * the view's state and the digests of the gossip. Update the reply with the
     * list of digests the view requires, as well as proposed updates based on the
     * inbound digests that the view has more recent information
     *
     * @param from
     * @param digests
     * @param p
     * @param seed
     */
    private JoinGossip processJoins(BloomFilter<Digest> bff, long seed, double p) {
        JoinGossip.Builder builder = JoinGossip.newBuilder();

        // Add all updates that this view has that aren't reflected in the inbound bff
        joins.entrySet()
             .stream()
             .filter(m -> !bff.contains(m.getKey()))
             .map(m -> m.getValue())
             .forEach(n -> builder.addUpdates(n));
        builder.setBff(getJoinsBff(seed, p).toBff());
        JoinGossip gossip = builder.build();
        return gossip;
    }

    /**
     * Process the inbound notes from the gossip. Reconcile the differences between
     * the view's state and the digests of the gossip. Update the reply with the
     * list of digests the view requires, as well as proposed updates based on the
     * inbound digests that the view has more recent information
     *
     * @param from
     * @param digests
     * @param p
     * @param seed
     */
    private NoteGossip processNotes(Digest from, BloomFilter<Digest> bff, long seed, double p) {
        NoteGossip.Builder builder = NoteGossip.newBuilder();

        // Add all updates that this view has that aren't reflected in the inbound
        // bff
        context.active()
               .filter(m -> m.getNote() != null)
               .filter(m -> !bff.contains(m.getNote().getHash()))
               .map(m -> m.getNote())
               .forEach(n -> builder.addUpdates(n.getWrapped()));
        builder.setBff(getNotesBff(seed, p).toBff());
        if (builder.getUpdatesCount() != 0) {
            log.trace("process notes produced updates: {} on: {}", builder.getUpdatesCount(), node.getId());
        }
        return builder.build();
    }

    /**
     * Process the inbound observer from the gossip. Reconcile the differences
     * between the view's state and the digests of the gossip. Update the reply with
     * the list of digests the view requires, as well as proposed updates based on
     * the inbound digests that the view has more recent information
     *
     * @param from
     * @param digests
     * @param p
     * @param seed
     */
    private ViewChangeGossip processObservations(BloomFilter<Digest> bff, long seed, double p) {
        ViewChangeGossip.Builder builder = ViewChangeGossip.newBuilder();

        // Add all updates that this view has that aren't reflected in the inbound bff
        observations.entrySet()
                    .stream()
                    .filter(m -> !bff.contains(m.getKey()))
                    .map(m -> m.getValue())
                    .forEach(n -> builder.addUpdates(n));
        builder.setBff(getObservationsBff(seed, p).toBff());
        if (builder.getUpdatesCount() != 0) {
            log.trace("process view change produced updates: {} on: {}", builder.getUpdatesCount(), node.getId());
        }
        return builder.build();
    }

    /**
     * Process the updates of the supplied juicy gossip.
     *
     * @param gossip
     */
    private void processUpdates(Gossip gossip) {
        processUpdates(gossip.getNotes().getUpdatesList(), gossip.getAccusations().getUpdatesList(),
                       gossip.getObservations().getUpdatesList(), gossip.getJoins().getUpdatesList());
    }

    /**
     * Process the updates of the supplied juicy gossip.
     * 
     * @param notes
     * @param accusations
     */
    private void processUpdates(List<SignedNote> notes, List<SignedAccusation> accusations,
                                List<SignedViewChange> observe, List<SignedNote> joins) {
        notes.stream().map(s -> new NoteWrapper(s, digestAlgo)).forEach(note -> add(note));
        accusations.stream().map(s -> new AccusationWrapper(s, digestAlgo)).forEach(accusation -> add(accusation));
        observe.forEach(observation -> add(observation));
        joins.forEach(j -> addJoin(j));
    }

    /**
     * recover a member from the failed state
     *
     * @param member
     */
    private void recover(Participant member) {
        if (shunned.contains(member.getId())) {
            log.debug("Not recovering shunned: {} on: {}", member.getId(), node.getId());
            return;
        }
        if (context.activate(member)) {
            member.clearAccusations();
            log.debug("Recovering: {} on: {}", member.getId(), node.getId());
        }
    }

    /**
     * Redirect the receiver to the correct ring, processing any new accusations
     * 
     * @param member
     * @param gossip
     * @param ring
     */
    private void redirect(Participant member, Gossip gossip, int ring) {
        if (gossip.getNotes().getUpdatesCount() != 1) {
            log.warn("Redirect response from {} on ring {} did not contain redirect member note on: {}", member.getId(),
                     ring, node.getId());
            return;
        }
        if (gossip.getAccusations().getUpdatesCount() > 0) {
            // Reset our epoch to whatever the group has recorded for this recovering node
            long max = gossip.getAccusations()
                             .getUpdatesList()
                             .stream()
                             .map(signed -> new AccusationWrapper(signed, digestAlgo))
                             .mapToLong(a -> a.getEpoch())
                             .max()
                             .orElse(-1);
            node.nextNote(max + 1);
        }
        if (gossip.getNotes().getUpdatesCount() == 1) {
            var note = new NoteWrapper(gossip.getNotes().getUpdatesList().get(0), digestAlgo);
            if (validation.validate(note.getCoordinates())) {
                add(note);
                gossip.getAccusations().getUpdatesList().forEach(s -> add(new AccusationWrapper(s, digestAlgo)));
                log.debug("Redirected from {} to {} on ring {} on: {}", member.getId(), note.getId(), ring,
                          node.getId());
            } else {
                log.warn("Redirect identity from {} on ring {} is invalid on: {}", member.getId(), ring, node.getId());
            }
        } else {
            log.warn("Redirect identity from {} on ring {} is invalid on: {}", member.getId(), ring, node.getId());
        }
    }

    /**
     * Redirect the member to the successor from this view's perspective
     *
     * @param member
     * @param ring
     * @param successor
     * @return the Gossip containing the successor's Identity and Note from this
     *         view
     */
    private Gossip redirectTo(Participant member, int ring, Participant successor) {
        assert member != null;
        assert successor != null;
        if (successor.getNote() == null) {
            log.debug("Cannot redirect from {} to {} on ring: {} as note is null on: {}", node, successor, ring,
                      node.getId());
            return Gossip.getDefaultInstance();
        }

        var identity = successor.getNote();
        if (identity == null) {
            log.debug("Cannot redirect from {} to {} on ring: {} as note is null on: {}", node, successor, ring,
                      node.getId());
            return Gossip.getDefaultInstance();
        }

        log.debug("Redirecting from {} to {} on ring {} on: {}", member, successor, ring, node.getId());
        return Gossip.newBuilder()
                     .setRedirect(true)
                     .setNotes(NoteGossip.newBuilder().addUpdates(successor.getNote().getWrapped()).build())
                     .setAccusations(AccusationGossip.newBuilder()
                                                     .addAllUpdates(member.getEncodedAccusations(context.getRingCount())))
                     .build();
    }

    /**
     * Remove the participant from the context
     * 
     * @param digest
     */
    private void remove(Digest digest) {
        if (context.isActive(digest)) {
            log.warn("Shunned but active: {} context: {} view: {} on: {}", digest, context.getId(), currentView.get(),
                     node.getId());
        }
        log.info("Permanently removing {} from context: {} view: {} on: {}", digest, context.getId(), currentView.get(),
                 node.getId());
        context.remove(digest);
        shunned.remove(digest);
        // TODO
    }

    /**
     * Process the gossip response, providing the updates requested by the the other
     * member and processing the updates provided by the other member
     *
     * @param gossip
     * @return the Update based on the processing of the reply from the other member
     */
    private Update response(Gossip gossip) {
        processUpdates(gossip);
        return updatesForDigests(gossip);
    }

    private <T> T stable(Callable<T> call) {
        final var lock = viewChange.readLock();
        lock.lock();
        try {
            return call.call();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            lock.unlock();
        }
    }

    private void stable(Runnable r) {
        final var lock = viewChange.readLock();
        lock.lock();
        try {
            r.run();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Cancel the timer to track the accussed member
     *
     * @param m
     */
    private void stopRebutalTimer(Participant m) {
        m.clearAccusations();
        var timer = pendingRebutals.remove(m.getId());
        if (timer != null) {
            timer.cancel();
        }
    }

    /**
     * Process the gossip reply. Return the gossip with the updates determined from
     * the inbound digests.
     *
     * @param gossip
     * @return
     */
    private Update updatesForDigests(Gossip gossip) {
        Update.Builder builder = Update.newBuilder();

        BloomFilter<Digest> notesBff = BloomFilter.from(gossip.getNotes().getBff());
        context.allMembers()
               .filter(m -> m.getNote() != null)
               .filter(m -> !notesBff.contains(m.getNote().getHash()))
               .map(m -> m.getNote().getWrapped())
               .forEach(n -> builder.addNotes(n));

        BloomFilter<Digest> accBff = BloomFilter.from(gossip.getAccusations().getBff());
        context.active()
               .flatMap(m -> m.getAccusations())
               .filter(a -> !accBff.contains(a.getHash()))
               .forEach(a -> builder.addAccusations(a.getWrapped()));

        BloomFilter<Digest> obsvBff = BloomFilter.from(gossip.getAccusations().getBff());
        observations.entrySet()
                    .stream()
                    .filter(e -> !obsvBff.contains(e.getKey()))
                    .forEach(e -> builder.addObservations(e.getValue()));

        BloomFilter<Digest> joinBff = BloomFilter.from(gossip.getJoins().getBff());
        joins.entrySet()
             .stream()
             .filter(e -> !joinBff.contains(e.getKey()))
             .forEach(e -> builder.addJoins(e.getValue()));

        return builder.build();
    }
}
