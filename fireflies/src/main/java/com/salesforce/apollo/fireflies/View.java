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
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.common.base.Objects;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multiset.Entry;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.fireflies.proto.Accusation;
import com.salesfoce.apollo.fireflies.proto.AccusationGossip;
import com.salesfoce.apollo.fireflies.proto.Digests;
import com.salesfoce.apollo.fireflies.proto.Gateway;
import com.salesfoce.apollo.fireflies.proto.Gossip;
import com.salesfoce.apollo.fireflies.proto.Join;
import com.salesfoce.apollo.fireflies.proto.JoinGossip;
import com.salesfoce.apollo.fireflies.proto.Note;
import com.salesfoce.apollo.fireflies.proto.NoteGossip;
import com.salesfoce.apollo.fireflies.proto.Redirect;
import com.salesfoce.apollo.fireflies.proto.SayWhat;
import com.salesfoce.apollo.fireflies.proto.SignedAccusation;
import com.salesfoce.apollo.fireflies.proto.SignedNote;
import com.salesfoce.apollo.fireflies.proto.SignedViewChange;
import com.salesfoce.apollo.fireflies.proto.State;
import com.salesfoce.apollo.fireflies.proto.Update;
import com.salesfoce.apollo.fireflies.proto.ViewChange;
import com.salesfoce.apollo.fireflies.proto.ViewChangeGossip;
import com.salesfoce.apollo.utils.proto.Biff;
import com.salesfoce.apollo.utils.proto.Digeste;
import com.salesforce.apollo.comm.RingCommunications;
import com.salesforce.apollo.comm.RingCommunications.Destination;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.comm.SliceIterator;
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
import io.grpc.stub.StreamObserver;

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
                        .setCurrentView(currentView.get().toDigeste())
                        .setHost(endpoint.getHostName())
                        .setPort(endpoint.getPort())
                        .setCoordinates(wrapped.getEvent().getCoordinates().toEventCoords())
                        .setMask(ByteString.copyFrom(nextMask().toByteArray()))
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
            final var current = note;
            final var toleranceLevel = context.toleranceLevel();
            if (current == null) {
                BitSet mask = createInitialMask(toleranceLevel);
                assert isValidMask(mask, toleranceLevel) : "Invalid mask: " + mask + " tolerance: " + toleranceLevel
                + " for node: " + getId();
                return mask;
            }

            BitSet mask = new BitSet(2 * toleranceLevel + 1);
            mask.flip(0, context.getRingCount());
            final var accusations = validAccusations;

            // disable current accusations
            for (int i = 0; i < context.getRingCount(); i++) {
                if (accusations[i] != null) {
                    mask.set(i, false);
                    continue;
                }
            }
            // clear masks from previous note
            BitSet previous = BitSet.valueOf(current.getMask().toByteArray());
            for (int index = 0; index < context.getRingCount(); index++) {
                if (!previous.get(index) && accusations[index] == null) {
                    mask.set(index, true);
                }
            }

            // Fill the rest of the mask with randomly set index
            while (mask.cardinality() != toleranceLevel + 1) {
                int index = Entropy.nextBitsStreamInt(context.getRingCount());
                if (accusations[index] != null) {
                    continue;
                }
                if (mask.cardinality() > toleranceLevel + 1 && mask.get(index)) {
                    mask.set(index, false);
                } else if (mask.cardinality() < toleranceLevel && !mask.get(index)) {
                    mask.set(index, true);
                }
            }
            assert isValidMask(mask, toleranceLevel) : "Invalid mask: " + mask + " t: " + toleranceLevel + " for node: "
            + getId();
            return mask;
        }

        /**
         * Generate a new note for the member based on any previous note and previous
         * accusations. The new note has a larger epoch number the the current note.
         */
        void nextNote() {
            nextNote(currentView.get());
        }

        void nextNote(Digest view) {
            NoteWrapper current = note;
            long newEpoch = current == null ? 1 : note.getEpoch() + 1;
            nextNote(newEpoch, view);
        }

        /**
         * Generate a new note using the new epoch
         *
         * @param newEpoch
         */
        void nextNote(long newEpoch, Digest view) {
            var n = note.newBuilder()
                        .setCoordinates(wrapped.getEvent().getCoordinates().toEventCoords())
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

        @Override
        void reset() {
            final var current = note;
            super.reset();
            var n = Note.newBuilder()
                        .setEpoch(0)
                        .setCurrentView(currentView.get().toDigeste())
                        .setHost(current.getHost())
                        .setPort(current.getPort())
                        .setCoordinates(current.getCoordinates().toEventCoords())
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

        protected final Digest                 id;
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

        public int getAccusationCount() {
            var count = 0;
            for (var acc : validAccusations) {
                if (acc != null) {
                    count++;
                }
            }
            return count;
        }

        @Override
        public Digest getId() {
            return id;
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }

        public boolean isDisabled(int ringNumber) {
            if (note != null) {
                return !note.getMask().get(ringNumber);
            }
            return false;
        }

        @Override
        public String toString() {
            return "Member[" + getId() + "]";
        }

        @Override
        public boolean verify(JohnHancock signature, InputStream message) {
            if (note == null) {
                return true;
            }
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
            if (accusation.getRingNumber() >= validAccusations.length) {
                return;
            }
            NoteWrapper n = getNote();
            if (n == null) {
                validAccusations[ringNumber] = accusation;
                return;
            }
            if (n.getEpoch() != accusation.getEpoch()) {
                log.trace("Invalid epoch discarding accusation from {} on {} ring {} on: {}", accusation.getAccuser(),
                          getId(), ringNumber, node.getId());
                return;
            }
            if (n.getMask().get(ringNumber)) {
                validAccusations[ringNumber] = accusation;
                if (log.isDebugEnabled()) {
                    log.debug("Member {} is accusing {} ring: {} on: {}", accusation.getAccuser(), getId(), ringNumber,
                              node.getId());
                }
            }
        }

        /**
         * clear all accusations for the member
         */
        void clearAccusations() {
            Arrays.fill(validAccusations, null);
            log.trace("Clearing accusations for: {} on: {}", getId(), node.getId());
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

        AccusationWrapper getEncodedAccusation(int ring) {
            if (ring >= validAccusations.length) {
                return null;
            }
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
            log.trace("Invalidating accusations of: {} ring: {} on: {}", getId(), index, node.getId());
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
            if (context.isActive(id)) {
                clearAccusations();
            }
            return true;
        }
    }

    public class Service {

        /**
         * Asynchronously add a member to the next view
         *
         * @param request          - request to join
         * @param from             - validated identity of partner
         * @param responseObserver - async response
         * @param timer            - metrics timer
         */
        public void join(Join request, Digest from, StreamObserver<Gateway> responseObserver, Timer.Context timer) {
            if (!started.get() | !joined.get()) {
                responseObserver.onNext(Gateway.getDefaultInstance());
                responseObserver.onCompleted();
                return;
            }
            var note = new NoteWrapper(request.getNote(), digestAlgo);
            if (!from.equals(note.getId())) {
                responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Member not match note")));
                return;
            }
            var requestView = Digest.from(request.getView());
            if (!currentView.get().equals(requestView)) {
                responseObserver.onError(new StatusRuntimeException(Status.OUT_OF_RANGE.withDescription("View id does not match, reseed")));
                return;
            }
            stable(() -> {
                pendingJoins.put(from, membership -> {
                    log.info("Gateway established for: {} view: {}  context: {} cardinality: {} on: {}", from,
                             currentView.get(), context.getId(), context.cardinality(), node.getId());
                    var seedSet = new TreeSet<Participant>();
                    context.predecessors(from).forEach(p -> seedSet.add(p));
                    context.successors(from).forEach(p -> seedSet.add(p));
                    var gateway = Gateway.newBuilder()
                                         .setView(currentView.get().toDigeste())
                                         .addAllMembership(membership)
                                         .addAllInitialSeedSet(seedSet.stream()
                                                                      .map(p -> p.getNote().getWrapped())
                                                                      .toList())
                                         .build();
                    responseObserver.onNext(gateway);
                    responseObserver.onCompleted();
                    if (timer != null) {
                        var serializedSize = gateway.getSerializedSize();
                        metrics.outboundBandwidth().mark(serializedSize);
                        metrics.outboundGateway().update(serializedSize);
                        timer.stop();
                    }
                });
                joiningMembers.compute(note.getId(), (m, h) -> {
                    if (h != null) {
                        joins.remove(h);
                    }
                    joins.put(note.getHash(), note);
                    return note.getHash();
                });
                log.debug("Member pending join: {} view: {} context: {} on: {}", from, currentView.get(),
                          context.getId(), node.getId());
            });
        }

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
            validate(from, request);
            return stable(() -> {
                if (request.getRing() >= context.getRingCount() || request.getRing() < 0) {
                    log.debug("invalid ring: {} from: {} on: {}", request.getRing(), from, node.getId());
                    return Gossip.getDefaultInstance();
                }
                Participant member = context.getActiveMember(from);
                Participant successor = context.ring(request.getRing())
                                               .successor(member, m -> context.isActive(m.getId()));
                if (successor == null) {
                    log.debug("No active successor on ring: {} from: {} on: {}", request.getRing(), from, node.getId());
                    return Gossip.getDefaultInstance();
                }
                if (!successor.equals(node)) {
                    return redirectTo(member, request.getRing(), successor);
                }
                final var digests = request.getGossip();
                final var g = Gossip.newBuilder()
                                    .setRedirect(false)
                                    .setNotes(processNotes(from, BloomFilter.from(digests.getNoteBff()), fpr))
                                    .setAccusations(processAccusations(BloomFilter.from(digests.getAccusationBff()),
                                                                       fpr))
                                    .setObservations(processObservations(BloomFilter.from(digests.getObservationBff()),
                                                                         fpr))
                                    .setJoins(processJoins(BloomFilter.from(digests.getJoinBiff()), fpr))
                                    .build();
                return g;
            });
        }

        public Redirect seed(Join request, Digest from) {
            if (!started.get() | !joined.get()) {
                return Redirect.getDefaultInstance();
            }
            final var requestView = Digest.from(request.getView());
            if (!bootstrapView().equals(requestView)) {
                log.warn("Invalid bootstrap view: {} from: {} on: {}", requestView, from, node.getId());
                return Redirect.getDefaultInstance();
            }
            var note = new NoteWrapper(request.getNote(), digestAlgo);
            if (!from.equals(note.getId())) {
                log.warn("Invalid bootstrap note: {} from: {} claiming: {} on: {}", requestView, from, note.getId(),
                         node.getId());
                return Redirect.getDefaultInstance();
            }
            var newMember = new Participant(note.getId());
            return stable(() -> {
                final var predecessors = new TreeSet<>(context.predecessors(newMember));
                log.debug("Member seeding: {} view: {} context: {} predecessors: {} on: {}", newMember.getId(),
                          currentView.get(), context.getId(), predecessors.stream().map(e -> e.getId()).toList(),
                          node.getId());
                return Redirect.newBuilder()
                               .setView(currentView.get().toDigeste())
                               .addAllPredecessors(predecessors.stream()
                                                               .filter(p -> p != null)
                                                               .map(p -> p.getNote().getWrapped())
                                                               .toList())
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
            validate(from, request);
            stable(() -> {
                final var ring = request.getRing();
                if (ring < 0 || ring > context.getRingCount()) {
                    log.debug("invalid ring: {} current: {} from: {} on: {}", ring, currentView.get(), ring, from,
                              node.getId());
                    return;
                }
                Participant member = context.getActiveMember(from);
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
                processUpdates(update.getNotesList(), update.getAccusationsList(), update.getObservationsList(),
                               update.getJoinsList());
            });
        }
    }

    /**
     * Embodiment of the client side join protocol
     *
     */
    private class Binding {
        private record Bound(Digest view, List<Digest> members, Map<Digest, NoteWrapper> seeds) {}

        private final Runnable trigger;

        public Binding(Runnable trigger) {
            this.trigger = trigger;
        }

        private void bootstrap() {
            log.info("Bootstrapping seed node view: {} context: {} on: {}", currentView.get(), context.getId(),
                     node.getId());

            joined.set(true);
            context.activate(node);
            var nw = node.getNote();
            joiningMembers.put(nw.getId(), nw.getHash());
            joins.put(nw.getHash(), nw);
            viewChange(() -> install(new Ballot(currentView.get(), Collections.emptyList(),
                                                Collections.singletonList(node.getId()), digestAlgo)));

            scheduleViewChange();

            trigger.run();

            log.info("Bootstrapped view: {} cardinality: {} count: {} context: {} on: {}", currentView.get(),
                     context.cardinality(), context.activeCount(), context.getId(), node.getId());

        }

        private boolean complete(CompletableFuture<Redirect> redirect,
                                 Optional<ListenableFuture<Redirect>> futureSailor, Member m) {
            if (futureSailor.isEmpty()) {
                return true;
            }
            try {
                final var r = futureSailor.get().get();
                if (redirect.complete(r)) {
                    log.info("Redirect to view: {} context: {} from: {} on: {}", Digest.from(r.getView()),
                             context.getId(), m.getId(), node.getId());
                }
                return false;
            } catch (ExecutionException e) {
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (CancellationException e) {
                // noop
            }
            return true;
        }

        private boolean completeGateway(Participant member, CompletableFuture<Bound> gateway,
                                        Optional<ListenableFuture<Gateway>> futureSailor, HashMultiset<Digest> view,
                                        HashMultiset<Integer> card, HashMultiset<Digest> ballots,
                                        HashMultiset<SignedNote> seeds) {
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
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } catch (CancellationException e) {
                return true;
            }

            var gatewayView = Digest.from(g.getView());
            view.add(gatewayView);
            g.getMembershipList().forEach(d -> ballots.add(Digest.from(d)));
            card.add(g.getMembershipCount());
            g.getInitialSeedSetList().forEach(nw -> seeds.add(nw));
            var v = view.entrySet()
                        .stream()
                        .filter(e -> e.getCount() == 1 || e.getCount() >= context.majority())
                        .map(e -> e.getElement())
                        .findFirst()
                        .orElse(null);
            if (v != null) {
                var cardinality = card.entrySet()
                                      .stream()
                                      .filter(e -> e.getCount() == 1 || e.getCount() >= context.majority())
                                      .mapToInt(e -> e.getElement())
                                      .findFirst()
                                      .orElse(-1);
                if (cardinality > 0 && ballots.size() >= cardinality) {
                    var members = ballots.entrySet()
                                         .stream()
                                         .filter(e -> e.getCount() == 1 || e.getCount() >= context.majority())
                                         .map(e -> e.getElement())
                                         .sorted()
                                         .toList();

                    if (members.size() == cardinality) {
                        final var crown = members.stream().reduce((a, b) -> a.xor(b)).orElse(digestAlgo.getOrigin());
                        if (v.equals(crown)) {
                            var initialSeedSet = seeds.entrySet()
                                                      .stream()
                                                      .filter(e -> e.getCount() == 1 ||
                                                                   e.getCount() >= context.majority())
                                                      .map(e -> e.getElement())
                                                      .map(sn -> new NoteWrapper(sn, digestAlgo))
                                                      .collect(Collectors.toMap(nw -> nw.getId(), nw -> nw));
                            if (gateway.complete(new Bound(v, members, initialSeedSet))) {
                                log.info("Gateway acquired: {} context: {} on: {}", v, context.getId(), node.getId());
                            }
                        } else {
                            log.error("Gateway crown: {} does not match view: {} cardinality: {} context: {} on: {}",
                                      crown, Digest.from(g.getView()), context.getId(), node.getId());
                            gateway.completeExceptionally(new IllegalStateException(String.format("Gateway crown: %s does not match view: %s cardinality: %s context: %s on: %s",
                                                                                                  crown,
                                                                                                  Digest.from(g.getView()),
                                                                                                  context.getId(),
                                                                                                  node.getId())));
                        }
                        return false;
                    } else {
                        log.info("Gateway ballots: insufficient for view: {} on: {}", ballots.size(), gatewayView,
                                 context.getId(), node.getId());
                        return true;
                    }
                }
            }
            log.info("Gateway received, views: {} cardinality: {} ballots: {} from: {} view: {} context: {} on: {}",
                     view.size(), card.size(), ballots.size(), member.getId(), gatewayView, context.getId(),
                     node.getId());
            return true;
        }

        private void redirect(AtomicReference<Digest> crown, Redirect redirect, Duration duration,
                              ScheduledExecutorService scheduler, Digest view) {
            var join = Join.newBuilder()
                           .setContext(context.getId().toDigeste())
                           .setView(redirect.getView())
                           .setNote(node.getNote().getWrapped())
                           .build();

            var predecessors = redirect.getPredecessorsList()
                                       .stream()
                                       .map(sn -> new NoteWrapper(sn, digestAlgo))
                                       .map(nw -> new Participant(nw))
                                       .peek(p -> context.activate(p))
                                       .collect(Collectors.toList());
            log.info("Redirecting to: {} context: {} predecessors: {} on: {}", view, context.getId(),
                     predecessors.stream().filter(p -> !node.getId().equals(p.getId())).map(p -> p.getId()).toList(),
                     node.getId());
            var gateway = new CompletableFuture<Bound>();
            gateway.whenComplete((bound, t) -> {
                if (t != null) {
                    log.error("Failed to join view: {}on: {}", view, node.getId(), t);
                    stop();
                }

                joined.set(true);

                currentView.set(bound.view);
                context.rebalance(bound.members.size());
                bound.seeds.values().forEach(nw -> context.activate(new Participant(nw)));
                bound.members.forEach(d -> context.activate(new Participant(d)));
                gossiper.reset();
                roundTimers.setRoundDuration(context.timeToLive());
                node.nextNote(view);
                scheduleViewChange();

                log.error("Joined view: {} cardinality: {} count: {} on: {}", view, context.cardinality(),
                          context.activeCount(), node.getId(), t);
                trigger.run();
            });

            var regate = new AtomicReference<Runnable>();
            HashMultiset<Digest> ballots = HashMultiset.create();
            var retries = new AtomicInteger();

            var gateways = new SliceIterator<>("Gateways", node, predecessors, comm, scheduler);
            HashMultiset<Integer> cardinality = HashMultiset.create();
            HashMultiset<Digest> views = HashMultiset.create();
            HashMultiset<SignedNote> seeds = HashMultiset.create();

            regate.set(() -> gateways.iterate((link, m) -> {
                log.debug("Joining: {} contacting: {} on: {}", view, link.getMember().getId(), node.getId());
                return link.join(join);
            }, (futureSailor, link, m) -> completeGateway((Participant) m, gateway, futureSailor, views, cardinality,
                                                          ballots, seeds),
                                              () -> {
                                                  if (retries.get() < MAX_JOIN_RETRIES) {
                                                      log.warn("Failed to join view: {} retry: {} out of: {} on: {}",
                                                               retries.incrementAndGet(), MAX_JOIN_RETRIES,
                                                               node.getId());
                                                      ballots.clear();
                                                      cardinality.clear();
                                                      views.clear();
                                                      regate.get().run();
                                                  } else {
                                                      log.error("Failed to join view: {} cannot obtain majority on: {}",
                                                                view, node.getId());
                                                      stop();
                                                  }
                                              }, scheduler, duration));
            regate.get().run();
        }

        private void seeding(List<Seed> seeds, Duration duration, ScheduledExecutorService scheduler) {
            if (seeds.isEmpty()) {// This node is the bootstrap seed
                bootstrap();
                return;
            }
            seeds = new ArrayList<>(seeds);
            Entropy.secureShuffle(seeds);
            log.info("Seeding view: {} context: {} with: {} started on: {}", currentView.get(), context.getId(),
                     seeds.stream()
                          .map(s -> ((SelfAddressingIdentifier) s.coordinates().getIdentifier()).getDigest())
                          .toList(),
                     node.getId());

            var seeding = new CompletableFuture<Redirect>();
            seeding.whenComplete((r, t) -> {
                if (t != null) {
                    log.error("Failed seeding on: {}", node.getId(), t);
                    return;
                }
                var view = Digest.from(r.getView());
                context.rebalance(r.getCardinality());
                node.nextNote(view);

                log.debug("Completing redirect to view: {} context: {} predecessors: {} on: {}", view, context.getId(),
                          r.getPredecessorsList()
                           .stream()
                           .map(sn -> new NoteWrapper(sn, digestAlgo))
                           .map(nw -> nw.getId())
                           .toList(),
                          node.getId());
                redirect(new AtomicReference<>(digestAlgo.getOrigin()), r, duration, scheduler, view);
            });
            var join = Join.newBuilder()
                           .setContext(context.getId().toDigeste())
                           .setView(currentView.get().toDigeste())
                           .setNote(node.getNote().getWrapped())
                           .build();

            var seedlings = new SliceIterator<>("Seedlings", node,
                                                seeds.stream()
                                                     .map(s -> new Participant(((SelfAddressingIdentifier) s.coordinates.getIdentifier()).getDigest()))
                                                     .filter(p -> !node.getId().equals(p.getId()))
                                                     .collect(Collectors.toList()),
                                                comm, scheduler);
            AtomicReference<Runnable> reseed = new AtomicReference<>();
            reseed.set(() -> {
                seedlings.iterate((link, m) -> {
                    log.debug("Requesting Seeding from: {} on: {}", link.getMember().getId(), node.getId());
                    return link.seed(join);
                }, (futureSailor, link, m) -> complete(seeding, futureSailor, m), () -> {
                    if (!seeding.isDone()) {
                        reseed.get().run();
                    }
                }, scheduler, duration);
            });
            reseed.get().run();
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

        @Override
        public String toString() {
            return String.format("{h: %s, j: %s, l: %s}", hash, joining.size(), leaving.size());
        }
    }

    private static final String FINALIZE_VIEW_CHANGE = "FINALIZE VIEW CHANGE";

    private static final int    FINALIZE_VIEW_ROUNDS  = 3;
    private static final Logger log                   = LoggerFactory.getLogger(View.class);
    private static final int    MAX_JOIN_RETRIES      = 3;
    private static final int    MINIMUM_BIFF_SIZE     = 100;
    private static final int    REBUTTAL_TIMEOUT      = 2;
    private static final String SCHEDULED_VIEW_CHANGE = "Scheduled View Change";
    private static final int    VIEW_CHANGE_ROUNDS    = 7;

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

    private final CommonCommunications<Fireflies, Service> comm;

    private final Context<Participant>                        context;
    private final AtomicReference<Digest>                     currentView      = new AtomicReference<>();
    private final DigestAlgorithm                             digestAlgo;
    private final Executor                                    exec;
    private final double                                      fpr;
    private volatile ScheduledFuture<?>                       futureGossip;
    private final RingCommunications<Participant, Fireflies>  gossiper;
    private final AtomicBoolean                               joined           = new AtomicBoolean();
    private final ConcurrentMap<Digest, Digest>               joiningMembers   = new ConcurrentSkipListMap<>();
    private final ConcurrentMap<Digest, NoteWrapper>          joins            = new ConcurrentSkipListMap<>();
    private final FireflyMetrics                              metrics;
    private final Node                                        node;
    private final Map<Digest, SignedViewChange>               observations     = new ConcurrentSkipListMap<>();
    private final Map<Digest, Consumer<List<Digeste>>>        pendingJoins     = new ConcurrentSkipListMap<>();
    private final ConcurrentMap<Digest, RoundScheduler.Timer> pendingRebuttals = new ConcurrentSkipListMap<>();
    private final RoundScheduler                              roundTimers;
    private final Service                                     service          = new Service();
    private final AtomicBoolean                               started          = new AtomicBoolean();
    private final Map<String, RoundScheduler.Timer>           timers           = new HashMap<>();
    private final EventValidation                             validation;
    private final ReadWriteLock                               viewChange       = new ReentrantReadWriteLock(true);
    private final AtomicReference<ViewChange>                 vote             = new AtomicReference<>();

    public View(Context<Participant> context, ControlledIdentifierMember member, InetSocketAddress endpoint,
                EventValidation validation, Router communications, double fpr, DigestAlgorithm digestAlgo,
                FireflyMetrics metrics, Executor exec) {
        this.metrics = metrics;
        this.validation = validation;
        this.fpr = fpr;
        this.digestAlgo = digestAlgo;
        this.context = context;
        this.roundTimers = new RoundScheduler(String.format("Timers for: %s", context.getId()), context.timeToLive());
        resetBootstrapView();
        this.node = new Node(member, endpoint);
        this.comm = communications.create(node, context.getId(), service,
                                          r -> new FfServer(service, communications.getClientIdentityProvider(), r,
                                                            exec, metrics),
                                          getCreate(metrics), Fireflies.getLocalLoopback(node));
        gossiper = new RingCommunications<>(context, node, comm, exec);
        this.exec = exec;
        context.activate(node);
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

        log.info("Starting: {} cardinality: {} tolerance: {} seeds: {} on: {}", context.getId(), context.cardinality(),
                 context.toleranceLevel(),
                 seeds.stream()
                      .map(s -> s.coordinates.getIdentifier())
                      .map(i -> (SelfAddressingIdentifier) i)
                      .map(sai -> sai.getDigest())
                      .toList(),
                 node.getId());
        roundTimers.reset();
        comm.register(context.getId(), service);
        context.clear();
        context.activate(node);
        node.reset();

        new Binding(() -> {
            var initial = Entropy.nextBitsStreamLong(d.toNanos());
            futureGossip = scheduler.schedule(() -> gossip(d, scheduler), initial, TimeUnit.NANOSECONDS);
        }).seeding(seeds, d, scheduler);

        log.info("{} started on: {}", context.getId(), node.getId());
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
        context.active().forEach(m -> {
            context.offline(m);
        });
        final var current = futureGossip;
        futureGossip = null;
        if (current != null) {
            current.cancel(true);
        }
        observations.clear();
        joins.clear();
        timers.values().forEach(t -> t.cancel());
        timers.clear();
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
        pendingRebuttals.computeIfAbsent(member.getId(), d -> roundTimers.schedule(() -> gc(member), REBUTTAL_TIMEOUT));
        log.debug("Accuse {} on ring {} (timer started) on: {}", member.getId(), ring, node.getId());
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
            log.trace("Accusation discarded, accused or accuser do not exist in view on: {}", node);
            return false;
        }

        if (accusation.getRingNumber() > context.getRingCount()) {
            log.trace("Invalid ring in accusation: {} on: {}", accusation.getRingNumber(), node);
            return false;
        }

        if (accused.getEpoch() != accusation.getEpoch()) {
            log.trace("Accusation discarded in epoch: {}  for: {} epoch: {} on: {}", accusation.getEpoch(),
                      accused.getId(), accused.getEpoch(), node);
            return false;
        }

        if (!accused.isDisabled(accusation.getRingNumber())) {
            log.trace("Member {} accussed on disabled ring {} by {} on: {}", accused.getId(),
                      accusation.getRingNumber(), accuser.getId(), node);
            return false;
        }

        if (!accuser.verify(accusation.getSignature(), accusation.getWrapped().getAccusation().toByteString())) {
            log.trace("Accusation by: {} accused:{} signature invalid on: {}", accuser.getId(), accused.getId(),
                      node.getId());
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
        if (accusation.getRingNumber() >= context.getRingCount()) {
            return false;
        }
        Ring<Participant> ring = context.ring(accusation.getRingNumber());

        if (accused.isAccusedOn(ring.getIndex())) {
            Participant currentAccuser = context.getMember(accused.getAccusation(ring.getIndex()).getAccuser());
            if (!currentAccuser.equals(accuser) && ring.isBetween(currentAccuser, accuser, accused)) {
                accused.addAccusation(accusation);
                pendingRebuttals.computeIfAbsent(accused.getId(),
                                                 d -> roundTimers.schedule(() -> gc(accused), REBUTTAL_TIMEOUT));
                log.debug("{} accused by {} on ring {} (replacing {}) on: {}", accused.getId(), accuser.getId(),
                          ring.getIndex(), currentAccuser, node.getId());
                if (metrics != null) {
                    metrics.accusations().mark();
                }
                return true;
            } else {
                log.debug("{} accused by {} on ring {} discarded as not predecessor on: {}", accused.getId(),
                          accuser.getId(), accusation.getRingNumber(), node.getId());
                return false;
            }
        } else {
            if (context.isOffline(accused.getId())) {
                accused.addAccusation(accusation);
                if (metrics != null) {
                    metrics.accusations().mark();
                }
                return false;
            }
            Participant predecessor = ring.predecessor(accused, m -> (!m.isAccused()) || (m.equals(accuser)));
            if (accuser.equals(predecessor)) {
                accused.addAccusation(accusation);
                if (!accused.equals(node) && !pendingRebuttals.containsKey(accused.getId())) {
                    log.debug("{} accused by {} on ring {} (timer started) on: {}", accused.getId(), accuser.getId(),
                              accusation.getRingNumber(), node.getId());
                    pendingRebuttals.computeIfAbsent(accused.getId(),
                                                     d -> roundTimers.schedule(() -> gc(accused), REBUTTAL_TIMEOUT));
                }
                if (metrics != null) {
                    metrics.accusations().mark();
                }
                return true;
            } else {
                log.debug("{} accused by {} on ring {} discarded as not predecessor {} on: {}", accused.getId(),
                          accuser.getId(), accusation.getRingNumber(), predecessor.getId(), node.getId());
                return false;
            }
        }
    }

    private boolean add(NoteWrapper note) {
        if (!isValidMask(note.getMask(), context.toleranceLevel())) {
            log.trace("Note: {} mask invalid {} on: {}", note.getId(), note.getMask(), node.getId());
            if (metrics != null) {
                metrics.filteredNotes().mark();
            }
            return false;
        }

        Participant m = context.getMember(note.getId());
        if (m == null) {
            if (!validation.verify(note.getCoordinates(), note.getSignature(),
                                   note.getWrapped().getNote().toByteString())) {
                log.trace("invalid participant note from {} on: {}", note.getId(), node.getId());
                if (metrics != null) {
                    metrics.filteredNotes().mark();
                }
                return false;
            }
            m = new Participant(note);
        } else {
            if (m.getEpoch() >= note.getEpoch()) {
                if (metrics != null) {
                    metrics.filteredNotes().mark();
                }
                return false;
            }
        }

        if (!m.verify(note.getSignature(), note.getWrapped().getNote().toByteString())) {
            log.trace("Note signature invalid: {} on: {}", note.getId(), node.getId());
            if (metrics != null) {
                metrics.filteredNotes().mark();
            }
            return false;
        }
        NoteWrapper current = m.getNote();
        if (current != null) {
            long nextEpoch = note.getEpoch();
            long currentEpoch = current.getEpoch();
            if (nextEpoch < currentEpoch) {
                log.trace("discarding note for {} with wrong epoch {} current: {} on: {}", m.getId(), nextEpoch,
                          currentEpoch, node.getId());
                if (metrics != null) {
                    metrics.filteredNotes().mark();
                }
                return false;
            }
        }

        if (metrics != null) {
            metrics.notes().mark();
        }

        log.trace("Adding member via note {} on: {}", m, node.getId());

        stopRebuttalTimer(m);

        if (m.isAccused()) {
            checkInvalidations(m);
        }

        m.setNote(note);
        recover(m);
        return true;
    }

    /**
     * Add an observation if it is for the current view and has not been previously
     * observed by the observer
     *
     * @param observation
     */
    private boolean add(SignedViewChange observation) {
        final Digest observer = Digest.from(observation.getChange().getObserver());
        final var next = Digest.from(observation.getChange().getCurrent());
        if (!currentView.get().equals(next)) {
            log.trace("Invalid view change: {} current: {} from {} on: {}", next, currentView.get(), observer,
                      node.getId());
            return false;
        }
        final var member = context.getActiveMember(observer);
        if (member == null) {
            log.trace("Invalid view change: {} current: {} offline: {} on: {}", next, currentView.get(), observer,
                      node.getId());
            return false;
        }
        final var signature = JohnHancock.from(observation.getSignature());
        if (!member.verify(signature, observation.getChange().toByteString())) {
            return false;
        }
        return observations.put(signature.toDigest(digestAlgo), observation) == null;
    }

    private boolean addJoin(SignedNote sn) {
        final var note = new NoteWrapper(sn, digestAlgo);

        if (!currentView.get().equals(note.currentView())) {
//            log.warn("Invalid join note view: {} current: {} from: {} on: {}", note.currentView(), currentView.get(),
//                     note.getId(), node.getId());
            return false;
        }

        if (context.isMember(note.getId())) {
//            log.warn("Already a member, ignoring join from: {} on: {}", note.currentView(), currentView.get(),
//                     note.getId(), node.getId());
            return false;
        }

        if (!validation.verify(note.getCoordinates(), note.getSignature(),
                               note.getWrapped().getNote().toByteString())) {
            log.warn("Invalid join note from {} on: {}", note.getId(), node.getId());
            return false;
        }
        if (!isValidMask(note.getMask(), context.toleranceLevel())) {
            log.trace("Invalid join note from: {} mask invalid {} on: {}", note.getId(), note.getMask(), node.getId());
            return false;
        }

        var isNew = new AtomicBoolean();
        joiningMembers.compute(note.getId(), (m, h) -> {
            if (h != null) {
                joins.remove(h);
            } else {
                isNew.set(true);
            }
            joins.put(note.getHash(), note);
            return note.getHash();
        });
        return isNew.get();
    }

    /**
     * add an inbound note to the view
     *
     * @param note
     */
    private boolean addToCurrentView(NoteWrapper note) {
        if (!currentView.get().equals(note.currentView())) {
//            log.trace("Ignoring note in invalid view: {} current: {} from {} on: {}", note.currentView(),
//                      currentView.get(), note.getId(), node.getId());
            if (metrics != null) {
                metrics.filteredNotes().mark();
            }
            return false;
        }
        if (context.isOffline(note.getId())) {
            if (metrics != null) {
                metrics.filteredNotes().mark();
            }
            return false;
        }
        return add(note);
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

    private Digest bootstrapView() {
        return context.getId().prefix(digestAlgo.getOrigin());
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
                      .setAccusationBff(getAccusationsBff(Entropy.nextSecureLong(), fpr).toBff())
                      .setNoteBff(getNotesBff(Entropy.nextSecureLong(), fpr).toBff())
                      .setJoinBiff(getJoinsBff(Entropy.nextSecureLong(), fpr).toBff())
                      .setObservationBff(getObservationsBff(Entropy.nextSecureLong(), fpr).toBff())
                      .build();
    }

    /**
     * Finalize the view change
     */
    private void finalizeViewChange() {
        viewChange(() -> {
            final var cardinality = context.memberCount();
            final var superMajority = cardinality - ((cardinality - 1) / 4);
            if (observations.size() < superMajority) {
//                log.trace("Do not have supermajority: {} required: {} local joins: {} leaving: {} for: {} on: {}",
//                          observations.size(), superMajority, joiningMembers.size(), context.offlineCount(),
//                          currentView.get(), node.getId());
                scheduleFinalizeViewChange(2);
                return;
            }
            HashMultiset<Ballot> ballots = HashMultiset.create();
            observations.values().forEach(vc -> {
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
                ballots.add(new Ballot(Digest.from(vc.getChange().getCurrent()), leaving, joining, digestAlgo));
            });
            var max = ballots.entrySet()
                             .stream()
                             .max(Ordering.natural().onResultOf(Multiset.Entry::getCount))
                             .orElse(null);
            if (max != null && max.getCount() >= superMajority) {
                log.info("Fast path consensus successful: {} required: {} local joins: {} leaving: {} for: {} on: {}",
                         max, superMajority, joiningMembers.size(), context.offlineCount(), currentView.get(),
                         node.getId());
                install(max.getElement());
            } else {
                @SuppressWarnings("unchecked")
                final var reversed = Comparator.comparing(e -> ((Entry<Ballot>) e).getCount()).reversed();
                log.info("Fast path consensus failed, required: {} local joins: {} leaving: {} ballots: {} for: {} on: {}",
                         superMajority, joiningMembers.size(), context.offlineCount(),
                         ballots.entrySet().stream().sorted(reversed).toList(), currentView.get(), node.getId());
            }
            scheduleViewChange();
            observations.clear();
            timers.remove(FINALIZE_VIEW_CHANGE);
            vote.set(null);
        });
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
        log.debug("Garbage collecting: {} on: {}", member.getId(), node.getId());
        context.offline(member);
    }

    /**
     * @param seed
     * @param p
     * @return the bloom filter containing the digests of known accusations
     */
    private BloomFilter<Digest> getAccusationsBff(long seed, double p) {
        BloomFilter<Digest> bff = new BloomFilter.DigestBloomFilter(seed,
                                                                    Math.max(MINIMUM_BIFF_SIZE, context.cardinality()),
                                                                    p);
        context.allMembers().flatMap(m -> m.getAccusations()).filter(e -> e != null).forEach(m -> bff.add(m.getHash()));
        return bff;
    }

    /**
     * @param seed
     * @param p
     * @return the bloom filter containing the digests of known joins
     */
    private BloomFilter<Digest> getJoinsBff(long seed, double p) {
        BloomFilter<Digest> bff = new BloomFilter.DigestBloomFilter(seed, Math.max(MINIMUM_BIFF_SIZE, joins.size() * 2),
                                                                    p);
        joins.keySet().forEach(d -> bff.add(d));
        return bff;
    }

    /**
     * @param seed
     * @param p
     * @return the bloom filter containing the digests of known notes
     */
    private BloomFilter<Digest> getNotesBff(long seed, double p) {
        BloomFilter<Digest> bff = new BloomFilter.DigestBloomFilter(seed, Math.max(MINIMUM_BIFF_SIZE,
                                                                                   context.cardinality() * 2),
                                                                    p);
        final var current = currentView.get();
        context.active()
               .map(m -> m.getNote())
               .filter(e -> e != null)
               .filter(n -> current.equals(n.currentView()))
               .forEach(n -> bff.add(n.getHash()));
        return bff;
    }

    /**
     * @param seed
     * @param p
     * @return the bloom filter containing the digests of known observations
     */
    private BloomFilter<Digest> getObservationsBff(long seed, double p) {
        BloomFilter<Digest> bff = new BloomFilter.DigestBloomFilter(seed, Math.max(MINIMUM_BIFF_SIZE,
                                                                                   context.cardinality() * 2),
                                                                    p);
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
        if (!started.get() || !joined.get()) {
            return;
        }

        exec.execute(Utils.wrapped(() -> {
            roundTimers.tick();
            var timer = metrics == null ? null : metrics.gossipRoundDuration().time();
            gossiper.execute((link, ring) -> gossip(link, ring),
                             (futureSailor, destination) -> gossip(futureSailor, destination, duration, scheduler,
                                                                   timer));
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
        if (context.isOffline(link.getMember().getId())) {
            log.warn("Shunning gossip view: {} with: {} on: {}", currentView.get(), link.getMember().getId(),
                     node.getId());
            if (metrics != null) {
                metrics.shunnedGossip().mark();
            }
            return null;
        }
        final SayWhat gossip = stable(() -> SayWhat.newBuilder()
                                                   .setContext(context.getId().toDigeste())
                                                   .setView(currentView.get().toDigeste())
                                                   .setNote(node.getNote().getWrapped())
                                                   .setRing(ring)
                                                   .setGossip(commonDigests())
                                                   .build());
        try {
            return link.gossip(gossip);
        } catch (Throwable e) {
            final var p = (Participant) link.getMember();
            if (e instanceof StatusRuntimeException sre) {
                if (sre.getStatus().getCode() != Status.NOT_FOUND.getCode()) {
                    log.debug("Error gossiping: {} from: {} on: {}", sre.getStatus(), p.getId(), node.getId());
                }
                accuse(p, ring);
                return null;
            } else {
                log.debug("Exception gossiping with {} on: {}", p.getId(), node.getId(), e.getCause());
                accuse(p, ring);
                return null;
            }
        }

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
        final var member = destination.member();
        if (futureSailor.isEmpty()) {
            if (member != null) {
                accuse(member, destination.ring());
            }
            futureGossip = scheduler.schedule(() -> gossip(duration, scheduler), duration.toNanos(),
                                              TimeUnit.NANOSECONDS);
            return;
        }

        try {
            Gossip gossip = futureSailor.get().get();
            if (gossip.getRedirect()) {
                stable(() -> redirect(member, gossip, destination.ring()));
            } else {
                try {
                    Update update = stable(() -> response(gossip));
                    if (update != null && !update.equals(Update.getDefaultInstance())) {
//                        log.trace("Update for: {} notes: {} accusations: {} joins: {} observations: {} on: {}",
//                                  destination.link().getMember().getId(), update.getNotesCount(),
//                                  update.getAccusationsCount(), update.getJoinsCount(), update.getObservationsCount(),
//                                  node.getId());
                        destination.link()
                                   .update(State.newBuilder()
                                                .setContext(context.getId().toDigeste())
                                                .setView(currentView.get().toDigeste())
                                                .setRing(destination.ring())
                                                .setUpdate(update)
                                                .build());
                    }
                } catch (StatusRuntimeException e) {
                    log.debug("Error updating: {} from: {} on: {}", e.getStatus(), node, node.getId());
                    accuse(member, destination.ring());
                }
            }
        } catch (ExecutionException e) {
            if (e.getCause() instanceof StatusRuntimeException sre) {
                if (sre.getStatus().getCode() != Status.NOT_FOUND.getCode()) {
                    log.debug("Error gossiping: {} from: {} on: {}", sre.getStatus(), member, node.getId());
                }
            } else {
                log.debug("Exception gossiping: with {} on: {}", member, node.getId(), e.getCause());
            }
            accuse(member, destination.ring());
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
        if (vote.get() != null) {
            log.trace("Vote already cast for: {} on: {}", currentView.get(), node.getId());
            return;
        }
        // Use pending rebuttals as a proxy for stability
        if (!pendingRebuttals.isEmpty()) {
            log.debug("Pending rebuttals: {} view: {} on: {}", pendingRebuttals.size(), currentView.get(),
                      node.getId());
            scheduleViewChange(1); // 1 TTL round to check again
            return;
        }
        stable(() -> {
            scheduleFinalizeViewChange();
            final var builder = ViewChange.newBuilder()
                                          .setObserver(node.getId().toDigeste())
                                          .setCurrent(currentView.get().toDigeste())
                                          .addAllLeaves(context.getOffline()
                                                               .stream()
                                                               .map(p -> p.getId().toDigeste())
                                                               .collect(Collectors.toSet()))
                                          .addAllJoins(joiningMembers.keySet()
                                                                     .stream()
                                                                     .map(d -> d.toDigeste())
                                                                     .toList());
            ViewChange change = builder.build();
            vote.set(change);
            var signature = node.sign(change.toByteString());
            final var hash = signature.toDigest(digestAlgo);
            observations.put(hash,
                             SignedViewChange.newBuilder().setChange(change).setSignature(signature.toSig()).build());
            log.info("View change vote: {} joins: {} leaves: {} on: {}", currentView.get(), change.getJoinsCount(),
                     change.getLeavesCount(), node.getId());
        });
    }

    /**
     * Install the new view
     * 
     * @param view
     */
    private void install(Ballot view) {
        log.debug("View change, joining: {} leaving: {} local joins: {} leaving: {} on: {}", view.joining, view.leaving,
                  joiningMembers.keySet().stream().toList(), context.getOffline(), node.getId());
        final var evicted = new AtomicBoolean();
        view.leaving.stream().filter(d -> !node.getId().equals(d)).forEach(p -> remove(p));
        var pending = view.joining.stream()
                                  .map(d -> joiningMembers.get(d))
                                  .filter(d -> d != null)
                                  .map(d -> joins.get(d))
                                  .filter(nw -> nw != null)
                                  .filter(nw -> add(nw))
                                  .peek(nw -> {
                                      if (metrics != null) {
                                          metrics.joins().mark();
                                      }
                                  })
                                  .map(nw -> pendingJoins.remove(nw.getId()))
                                  .filter(p -> p != null)
                                  .toList();
        joiningMembers.clear();
        joins.clear();

        // Tune
        context.rebalance();
        gossiper.reset();
        roundTimers.setRoundDuration(context.timeToLive());

        // The circle of life
        var previousView = currentView.get();

        // View is the combined hash of members on ring 0
        currentView.set(context.allMembers()
                               .map(p -> p.getId())
                               .sorted()
                               .reduce((a, b) -> a.xor(b))
                               .orElse(digestAlgo.getOrigin()));

        // Regenerate for new epoch
        node.nextNote();

        var members = context.allMembers().map(p -> p.getId().toDigeste()).toList();

        // complete all pending joins
        pending.forEach(r -> exec.execute(Utils.wrapped(() -> r.accept(members), log)));

        log.info("Installed new view: {} from: {} for context: {} cardinality: {} count: {} pending: {} leaving: {} joining: {} evicted: {} on: {}",
                 currentView.get(), previousView, context.getId(), context.cardinality(), context.allMembers().count(),
                 pending.size(), view.leaving.size(), view.joining.size(), evicted.get(), node.getId());
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

    /**
     * start a view change if there's any offline members or joining members
     */
    private void maybeViewChange() {
        if (context.offlineCount() > 0 || joins.size() > 0) {
            initiateViewChange();
        } else {
            scheduleViewChange();
        }
    }

    /**
     * Process the inbound accusations from the gossip. Reconcile the differences
     * between the view's state and the digests of the gossip. Update the reply with
     * the list of digests the view requires, as well as proposed updates based on
     * the inbound digets that the view has more recent information. Do not forward
     * accusations from crashed members
     *
     * @param p
     * @param digests
     *
     * @return
     */
    private AccusationGossip processAccusations(BloomFilter<Digest> bff, double p) {
        AccusationGossip.Builder builder = AccusationGossip.newBuilder();
        // Add all updates that this view has that aren't reflected in the inbound
        // bff
        context.active()
               .flatMap(m -> m.getAccusations())
               .filter(a -> !bff.contains(a.getHash()))
               .forEach(a -> builder.addUpdates(a.getWrapped()));
        builder.setBff(getAccusationsBff(Entropy.nextSecureLong(), p).toBff());
        if (builder.getUpdatesCount() != 0) {
//            log.trace("process accusations produced updates: {} on: {}", builder.getUpdatesCount(), node.getId());
        }
        return builder.build();
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
    private JoinGossip processJoins(BloomFilter<Digest> bff, double p) {
        JoinGossip.Builder builder = JoinGossip.newBuilder();

        // Add all updates that this view has that aren't reflected in the inbound bff
        joins.entrySet()
             .stream()
             .filter(m -> !bff.contains(m.getKey()))
             .map(m -> m.getValue())
             .forEach(n -> builder.addUpdates(n.getWrapped()));
        builder.setBff(getJoinsBff(Entropy.nextSecureLong(), p).toBff());
        JoinGossip gossip = builder.build();
        if (builder.getUpdatesCount() != 0) {
//            log.trace("process joins produced updates: {} on: {}", builder.getUpdatesCount(), node.getId());
        }
        return gossip;
    }

    /**
     * Process the inbound notes from the gossip. Reconcile the differences between
     * the view's state and the digests of the gossip. Update the reply with the
     * list of digests the view requires, as well as proposed updates based on the
     * inbound digests that the view has more recent information
     *
     * @param from
     * @param p
     * @param digests
     */
    private NoteGossip processNotes(Digest from, BloomFilter<Digest> bff, double p) {
        NoteGossip.Builder builder = NoteGossip.newBuilder();

        // Add all updates that this view has that aren't reflected in the inbound
        // bff
        final var current = currentView.get();
        context.active()
               .filter(m -> m.getNote() != null)
               .filter(m -> current.equals(m.getNote().currentView()))
               .filter(m -> !bff.contains(m.getNote().getHash()))
               .map(m -> m.getNote())
               .forEach(n -> builder.addUpdates(n.getWrapped()));
        builder.setBff(getNotesBff(Entropy.nextSecureLong(), p).toBff());
        if (builder.getUpdatesCount() != 0) {
//            log.trace("process notes produced updates: {} on: {}", builder.getUpdatesCount(), node.getId());
        }
        return builder.build();
    }

    /**
     * Process the inbound observer from the gossip. Reconcile the differences
     * between the view's state and the digests of the gossip. Update the reply with
     * the list of digests the view requires, as well as proposed updates based on
     * the inbound digests that the view has more recent information
     *
     * @param p
     * @param from
     * @param digests
     */
    private ViewChangeGossip processObservations(BloomFilter<Digest> bff, double p) {
        ViewChangeGossip.Builder builder = ViewChangeGossip.newBuilder();

        // Add all updates that this view has that aren't reflected in the inbound bff
        observations.entrySet()
                    .stream()
                    .filter(m -> !bff.contains(m.getKey()))
                    .map(m -> m.getValue())
                    .forEach(n -> builder.addUpdates(n));
        builder.setBff(getObservationsBff(Entropy.nextSecureLong(), p).toBff());
        if (builder.getUpdatesCount() != 0) {
//            log.trace("process view change produced updates: {} on: {}", builder.getUpdatesCount(), node.getId());
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
        if (nCount != 0 || aCount != 0 || oCount != 0 || jCount != 0) {
            log.trace("Updating notes: {}:{} accusations: {}:{} observations: {}:{} joins: {}:{} on: {}", nCount,
                      notes.size(), aCount, accusations.size(), oCount, observe.size(), jCount, joins.size(),
                      node.getId());
        }
    }

    /**
     * recover a member from the failed state
     *
     * @param member
     */
    private void recover(Participant member) {
        if (context.isMember(member) && context.isOffline(member.getId())) {
            log.debug("Not recovering shunned: {} on: {}", member.getId(), node.getId());
            return;
        }
        if (context.activate(member)) {
            log.trace("Recovering: {} on: {}", member.getId(), node.getId());
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
            node.nextNote(max + 1, currentView.get());
        }
        if (gossip.getNotes().getUpdatesCount() == 1) {
            var note = new NoteWrapper(gossip.getNotes().getUpdatesList().get(0), digestAlgo);
            if (validation.validate(note.getCoordinates())) {
                addToCurrentView(note);
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
        log.debug("Redirecting: {} to {} on ring {} on: {}", member, successor, ring, node.getId());
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
        var pending = pendingRebuttals.remove(digest);
        if (pending != null) {
            pending.cancel();
        }
        log.info("Permanently removing {} member {} from context: {} view: {} on: {}",
                 context.isActive(digest) ? "active" : "failed", digest, context.getId(), currentView.get(),
                 node.getId());
        context.remove(digest);
        if (metrics != null) {
            metrics.leaves().mark();
        }
    }

    private void resetBootstrapView() {
        currentView.set(bootstrapView());
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

    private void scheduleFinalizeViewChange() {
        scheduleFinalizeViewChange(FINALIZE_VIEW_ROUNDS);
    }

    private void scheduleFinalizeViewChange(final int finalizeViewRounds) {
//        log.trace("View change finalization scheduled: {} rounds for: {} joining: {} leaving: {} on: {}",
//                  finalizeViewRounds, currentView.get(), joins.size(), context.getOffline().size(), node.getId());
        timers.put(FINALIZE_VIEW_CHANGE,
                   roundTimers.schedule(FINALIZE_VIEW_CHANGE, () -> finalizeViewChange(), finalizeViewRounds));
    }

    private void scheduleViewChange() {
        scheduleViewChange(VIEW_CHANGE_ROUNDS);
    }

    private void scheduleViewChange(final int viewChangeRounds) {
//        log.trace("Schedule view change: {} rounds for: {}   on: {}", viewChangeRounds, currentView.get(),
//                  node.getId());
        timers.put(SCHEDULED_VIEW_CHANGE,
                   roundTimers.schedule(SCHEDULED_VIEW_CHANGE, () -> maybeViewChange(), viewChangeRounds));
    }

    private <T> T stable(Callable<T> call) {
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
    private void stopRebuttalTimer(Participant m) {
        m.clearAccusations();
        var timer = pendingRebuttals.remove(m.getId());
        if (timer != null) {
            log.debug("Cancelling accusation of: {} on: {}", m.getId(), node.getId());
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

        var biff = gossip.getNotes().getBff();
        if (!biff.equals(Biff.getDefaultInstance())) {
            BloomFilter<Digest> notesBff = BloomFilter.from(biff);
            final var current = currentView.get();
            context.activeMembers()
                   .stream()
                   .filter(m -> m.getNote() != null)
                   .filter(m -> current.equals(m.getNote().currentView()))
                   .filter(m -> !notesBff.contains(m.getNote().getHash()))
                   .map(m -> m.getNote().getWrapped())
                   .forEach(n -> builder.addNotes(n));
        }

        biff = gossip.getAccusations().getBff();
        if (!biff.equals(Biff.getDefaultInstance())) {
            BloomFilter<Digest> accBff = BloomFilter.from(biff);
            context.active()
                   .flatMap(m -> m.getAccusations())
                   .filter(a -> !accBff.contains(a.getHash()))
                   .forEach(a -> builder.addAccusations(a.getWrapped()));
        }

        biff = gossip.getObservations().getBff();
        if (!biff.equals(Biff.getDefaultInstance())) {
            BloomFilter<Digest> obsvBff = BloomFilter.from(biff);
            observations.entrySet()
                        .stream()
                        .filter(e -> !obsvBff.contains(e.getKey()))
                        .forEach(e -> builder.addObservations(e.getValue()));
        }

        biff = gossip.getJoins().getBff();
        if (!biff.equals(Biff.getDefaultInstance())) {
            BloomFilter<Digest> joinBff = BloomFilter.from(biff);
            joins.entrySet()
                 .stream()
                 .filter(e -> !joinBff.contains(e.getKey()))
                 .forEach(e -> builder.addJoins(e.getValue().getWrapped()));
        }

        return builder.build();
    }

    private void validate(Digest from, SayWhat request) {
        var valid = false;
        var note = new NoteWrapper(request.getNote(), digestAlgo);
        var requestView = Digest.from(request.getView());
        try {
            if (!started.get() || !joined.get()) {
                final String available = started.get() ? "pending join" : "offline";
                log.trace("{}, shunned from: {}  on: {}", available, from, node.getId());
                throw new StatusRuntimeException(Status.UNAVAILABLE.withDescription("Member: " + node.getId() + " is "
                + available));
            }
            if (context.isOffline(from)) {
                log.trace("Shunned from: {} local count: {} on: {}", from, context.allMembers().count(), node.getId());
                throw new StatusRuntimeException(Status.PERMISSION_DENIED.withDescription("Member is offline: "
                + from));
            }
            if (!requestView.equals(currentView.get())) {
                log.debug("Invalid view: {} current: {} ring: {} from: {} on: {}", requestView, currentView.get(),
                          request.getRing(), from, node.getId());
                throw new StatusRuntimeException(Status.PERMISSION_DENIED.withDescription("Invalid view: "
                + requestView));
            }
            if (!from.equals(note.getId())) {
                throw new StatusRuntimeException(Status.PERMISSION_DENIED.withDescription("Member does not match: "
                + from));
            }
            valid = true;
        } finally {
            if (!valid && metrics != null) {
                metrics.shunnedGossip().mark();
            }
        }
    }

    private void validate(Digest from, State request) {
        var valid = true;
        try {
            if (!started.get() | !joined.get()) {
                throw new StatusRuntimeException(Status.UNAVAILABLE.withDescription("Member: " + node.getId() + " is "
                + (started.get() ? "pending join" : "offline")));
            }
            if (context.isOffline(from)) {
                log.warn("Shunned update: {} on: {}", from, node.getId());
                throw new StatusRuntimeException(Status.PERMISSION_DENIED.withDescription("Member is offline: "
                + from));
            }
            final var requestView = Digest.from(request.getView());
            if (!requestView.equals(currentView.get())) {
                log.debug("invalid view: {} current: {} from: {} on: {}", requestView, currentView.get(),
                          request.getRing(), from, node.getId());
                throw new StatusRuntimeException(Status.PERMISSION_DENIED.withDescription("Invalid view: "
                + requestView));
            }
            valid = true;
        } finally {
            if (!valid && metrics != null) {
                metrics.shunnedGossip().mark();
            }
        }
    }

    private void viewChange(Runnable r) {
        final var lock = viewChange.writeLock();
        lock.lock();
        try {
            r.run();
        } finally {
            lock.unlock();
        }
    }
}
