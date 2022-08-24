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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
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
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.crypto.spec.SecretKeySpec;

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
import com.salesfoce.apollo.fireflies.proto.Credentials;
import com.salesfoce.apollo.fireflies.proto.Digests;
import com.salesfoce.apollo.fireflies.proto.Gateway;
import com.salesfoce.apollo.fireflies.proto.Gossip;
import com.salesfoce.apollo.fireflies.proto.Join;
import com.salesfoce.apollo.fireflies.proto.JoinGossip;
import com.salesfoce.apollo.fireflies.proto.Note;
import com.salesfoce.apollo.fireflies.proto.NoteGossip;
import com.salesfoce.apollo.fireflies.proto.Redirect;
import com.salesfoce.apollo.fireflies.proto.SayWhat;
import com.salesfoce.apollo.fireflies.proto.Seed_;
import com.salesfoce.apollo.fireflies.proto.SignedAccusation;
import com.salesfoce.apollo.fireflies.proto.SignedNote;
import com.salesfoce.apollo.fireflies.proto.SignedViewChange;
import com.salesfoce.apollo.fireflies.proto.State;
import com.salesfoce.apollo.fireflies.proto.Update;
import com.salesfoce.apollo.fireflies.proto.ViewChange;
import com.salesfoce.apollo.fireflies.proto.ViewChangeGossip;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyState_;
import com.salesfoce.apollo.utils.proto.Biff;
import com.salesforce.apollo.comm.RingCommunications;
import com.salesforce.apollo.comm.RingCommunications.Destination;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.comm.Router.ServiceRouting;
import com.salesforce.apollo.comm.SliceIterator;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.SigningThreshold;
import com.salesforce.apollo.fireflies.communications.Approach;
import com.salesforce.apollo.fireflies.communications.EntranceClient;
import com.salesforce.apollo.fireflies.communications.EntranceServer;
import com.salesforce.apollo.fireflies.communications.FfServer;
import com.salesforce.apollo.fireflies.communications.Fireflies;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.ReservoirSampler;
import com.salesforce.apollo.membership.Ring;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.EventValidation;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.RoundScheduler;
import com.salesforce.apollo.utils.Utils;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;

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
                        .setCurrentView(currentView().toDigeste())
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

        public KERL_ kerl() {
            try {
                return wrapped.kerl().get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return KERL_.getDefaultInstance();
            } catch (ExecutionException e) {
                throw new IllegalStateException(e.getCause());
            }
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
                                       .setCurrentView(currentView().toDigeste())
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
            for (int i = 0; i < context.getRingCount() && i < accusations.length; i++) {
                if (accusations[i] != null) {
                    mask.set(i, false);
                    continue;
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

            while (mask.cardinality() != toleranceLevel + 1) {
                int index = Entropy.nextBitsStreamInt(context.getRingCount());
                if (index < accusations.length) {
                    if (accusations[index] != null) {
                        continue;
                    }
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
            final var current = note;
            if (current == null) {
                return null;
            }
            return new InetSocketAddress(current.getHost(), current.getPort());
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
            return validation.filtered(current.getCoordinates(), threshold, signature, message);
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
            return getAccusations().map(w -> w.getWrapped()).toList();
        }

        @Override
        public Digest getId() {
            return id;
        }

        public Seed_ getSeed() {
            final var keyState = validation.getKeyState(note.getCoordinates());
            return Seed_.newBuilder()
                        .setNote(note.getWrapped())
                        .setKeyState(keyState.isEmpty() ? KeyState_.getDefaultInstance() : keyState.get().toKeyState_())
                        .build();
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
            return validation.verify(current.getCoordinates(), signature, message);
        }

        @Override
        public boolean verify(SigningThreshold threshold, JohnHancock signature, InputStream message) {
            final var current = note;
            return validation.verify(current.getCoordinates(), threshold, signature, message);
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
            for (var acc : validAccusations) {
                if (acc != null) {
                    log.trace("Clearing accusations for: {} on: {}", getId(), node.getId());
                    break;
                }
            }
            Arrays.fill(validAccusations, null);
        }

        AccusationWrapper getAccusation(int ring) {
            return validAccusations[ring];
        }

        Stream<AccusationWrapper> getAccusations() {
            return Arrays.asList(validAccusations).stream().filter(a -> a != null);
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
            log.trace("Invalidating accusations of: {} ring: {} on: {}", getId(), index, node.getId());
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

    public record Seed(EventCoordinates coordinates, InetSocketAddress endpoint) {}

    public class Service implements ServiceRouting {

        /**
         * Asynchronously add a member to the next view
         *
         * @param credentials      - request to join
         * @param from             - validated identity of partner
         * @param responseObserver - async response
         * @param timer            - metrics timer
         */
        public void join(Credentials credentials, Digest from, StreamObserver<Gateway> responseObserver,
                         Timer.Context timer) {
            if (!started.get()) {
                throw new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription("Not started"));
            }
            viewManagement.join(credentials, from, responseObserver, timer);
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
            return stable(() -> {
                validate(from, request);
                final var ring = request.getRing();
                if (!context.validRing(ring)) {
                    log.debug("invalid ring: {} from: {} on: {}", ring, from, node.getId());
                    return Gossip.getDefaultInstance();
                }
                Participant member = context.getActiveMember(from);
                if (member == null) {
                    add(new NoteWrapper(request.getNote(), digestAlgo));
                    member = context.getActiveMember(from);
                    if (member == null) {
                        return Gossip.getDefaultInstance();
                    }
                }
                Participant successor = context.ring(ring).successor(member, m -> context.isActive(m.getId()));
                if (successor == null) {
                    log.debug("No active successor on ring: {} from: {} on: {}", ring, from, node.getId());
                    throw new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription("No successor of: "
                    + from));
                }
                Gossip g;
                final var digests = request.getGossip();
                if (!successor.equals(node)) {
                    g = redirectTo(member, ring, successor, digests);
                } else {
                    g = Gossip.newBuilder()
                              .setRedirect(false)
                              .setNotes(processNotes(from, BloomFilter.from(digests.getNoteBff()), params.fpr()))
                              .setAccusations(processAccusations(BloomFilter.from(digests.getAccusationBff()),
                                                                 params.fpr()))
                              .setObservations(processObservations(BloomFilter.from(digests.getObservationBff()),
                                                                   params.fpr()))
                              .setJoins(viewManagement.processJoins(BloomFilter.from(digests.getJoinBiff()),
                                                                    params.fpr()))
                              .build();
                }
                if (g.getNotes().getUpdatesCount() + g.getAccusations().getUpdatesCount()
                + g.getObservations().getUpdatesCount() + g.getJoins().getUpdatesCount() != 0) {
                    log.trace("Gossip for: {} notes: {} accusations: {} joins: {} observations: {} on: {}", from,
                              g.getNotes().getUpdatesCount(), g.getAccusations().getUpdatesCount(),
                              g.getJoins().getUpdatesCount(), g.getObservations().getUpdatesCount(), node.getId());
                }
                return g;
            });
        }

        public Redirect seed(Credentials credentials, Digest from) {
            if (!started.get()) {
                throw new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription("Not started"));
            }
            return viewManagement.seed(credentials, from);
        }

        /**
         * The third and final message in the anti-entropy protocol. Process the inbound
         * update from another member.
         *
         * @param state - update state
         * @param from
         */
        public void update(State request, Digest from) {
            stable(() -> {
                validate(from, request);
                final var ring = request.getRing();
                if (!context.validRing(ring)) {
                    log.debug("invalid ring: {} current: {} from: {} on: {}", ring, currentView(), ring, from,
                              node.getId());
                    throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("No successor of: "
                    + from));
                }
                Participant member = context.getActiveMember(from);
                Participant successor = context.ring(ring).successor(member, m -> context.isActive(m.getId()));
                if (successor == null) {
                    log.debug("No successor, invalid update from: {} on ring: {} on: {}", from, ring, node.getId());
                    throw new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription("No successor of: "
                    + from));
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

    @FunctionalInterface
    public interface ViewChangeListener {
        /**
         * Notification of a view change event
         *
         * @param context - the context for which the view change has occurred
         * @param viewId  - the Digest id of the new view
         * @param joins   - the list of joining members ids
         * @param leaves  - the list of leaving members ids
         */
        void viewChange(Context<Participant> context, Digest viewId, List<Digest> joins, List<Digest> leaves);
    }

    public record Authentication(String identifier, SecretKeySpec authentication) {}

    /**
     * Embodiment of the client side join protocol
     *
     */
    private class Binding {

        private final Duration                 duration;
        private final ScheduledExecutorService scheduler;
        private final List<Seed>               seeds;

        public Binding(List<Seed> seeds, Duration duration, ScheduledExecutorService scheduler) {
            this.duration = duration;
            this.seeds = new ArrayList<>(seeds);
            this.scheduler = scheduler;
        }

        private void bootstrap() {
            log.info("Bootstrapping seed node view: {} context: {} on: {}", currentView(), context.getId(),
                     node.getId());

            var nw = node.getNote();
            final var sched = scheduler;
            final var dur = duration;

            viewManagement.bootstrap(nw, sched, dur);
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
                                        Optional<ListenableFuture<Gateway>> futureSailor,
                                        HashMultiset<Biff> memberships, HashMultiset<Digest> views,
                                        HashMultiset<Integer> cards, Set<SignedNote> seeds, Digest view, int majority,
                                        Set<SignedNote> joined) {
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
                        log.trace("Resource exhausted in join: {} with: {} : {} on: {}", view, member.getId(),
                                  sre.getStatus(), node.getId());
                        break;
                    case OUT_OF_RANGE:
                        log.debug("View change in join: {} with: {} : {} on: {}", view, member.getId(), sre.getStatus(),
                                  node.getId());
                        viewManagement.resetBootstrapView();
                        node.reset();
                        exec.execute(() -> seeding());
                        return false;
                    case DEADLINE_EXCEEDED:
                        log.trace("Join timeout for view: {} with: {} : {} on: {}", view, member.getId(),
                                  sre.getStatus(), node.getId());
                        break;
                    default:
                        log.warn("Failure in join: {} with: {} : {} on: {}", view, member.getId(), sre.getStatus(),
                                 node.getId());
                    }
                } else {
                    log.error("Failure in join: {} with: {} on: {}", view, member.getId(), node.getId(), e.getCause());
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
                log.warn("No seeds in join returned from: {} on: {}", member.getId(), node.getId());
                return true;
            }
            if (g.getMembers().equals(Biff.getDefaultInstance())) {
                log.warn("No membership in join returned from: {} on: {}", member.getId(), node.getId());
                return true;
            }
            var gatewayView = Digest.from(g.getView());
            if (gatewayView.equals(Digest.NONE)) {
                log.trace("Empty view in join returned from: {} on: {}", member.getId(), node.getId());
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
            log.debug("Gateway received, view count: {} cardinality: {} memberships: {} majority: {} from: {} view: {} context: {} on: {}",
                      views.size(), cards.size(), memberships.size(), majority, member.getId(), gatewayView,
                      context.getId(), node.getId());
            return true;
        }

        private Credentials credentials(Digest view) {
            var join = Join.newBuilder()
                           .setView(view.toDigeste())
                           .setNote(node.getNote().getWrapped())
                           .setKerl(node.kerl())
                           .build();
            var credentials = Credentials.newBuilder().setContext(context.getId().toDigeste()).setJoin(join).build();
            return credentials;
        }

        private BiConsumer<? super Redirect, ? super Throwable> redirect(Duration duration,
                                                                         ScheduledExecutorService scheduler,
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
                context.rebalance(r.getCardinality());
                node.nextNote(view);

                log.debug("Completing redirect to view: {} context: {} successors: {} on: {}", view, context.getId(),
                          r.getSuccessorsCount(), node.getId());
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
                                     .map(sn -> new NoteWrapper(sn.getNote(), digestAlgo))
                                     .map(nw -> new Participant(nw))
                                     .collect(Collectors.toList());
            log.info("Redirecting to: {} context: {} successors: {} on: {}", view, context.getId(), succsesors.size(),
                     node.getId());
            var gateway = new CompletableFuture<Bound>();
            var timer = metrics == null ? null : metrics.joinDuration().time();
            gateway.whenComplete(viewManagement.join(scheduler, duration, timer));

            var regate = new AtomicReference<Runnable>();
            var retries = new AtomicInteger();

            HashMultiset<Biff> biffs = HashMultiset.create();
            HashSet<SignedNote> seeds = new HashSet<>();
            HashMultiset<Digest> views = HashMultiset.create();
            HashMultiset<Integer> cards = HashMultiset.create();
            Set<SignedNote> joined = new HashSet<>();

            context.rebalance(redirect.getCardinality());
            node.nextNote(view);

            final var redirecting = new SliceIterator<>("Gateways", node, succsesors, approaches, exec);
            regate.set(() -> {
                redirecting.iterate((link, m) -> {
                    log.debug("Joining: {} contacting: {} on: {}", view, link.getMember().getId(), node.getId());
                    return link.join(credentials(view), params.seedingTimeout());
                }, (futureSailor, link,
                    m) -> completeGateway((Participant) m, gateway, futureSailor, biffs, views, cards, seeds, view,
                                          Context.minimalQuorum(redirect.getRings(), context.getBias()), joined),
                                    () -> {
                                        if (retries.get() < params.joinRetries()) {
                                            log.debug("Failed to join view: {} retry: {} out of: {} on: {}", view,
                                                      retries.incrementAndGet(), params.joinRetries(), node.getId());
                                            biffs.clear();
                                            views.clear();
                                            cards.clear();
                                            seeds.clear();
                                            scheduler.schedule(exec(() -> regate.get().run()),
                                                               Entropy.nextBitsStreamLong(params.retryDelay()
                                                                                                .toNanos()),
                                                               TimeUnit.NANOSECONDS);
                                        } else {
                                            log.error("Failed to join view: {} cannot obtain majority on: {}", view,
                                                      node.getId());
                                            stop();
                                        }
                                    }, scheduler, params.retryDelay());
            });
            regate.get().run();
        }

        private void seeding() {
            if (seeds.isEmpty()) {// This node is the bootstrap seed
                bootstrap();
                return;
            }
            Entropy.secureShuffle(seeds);
            log.info("Seeding view: {} context: {} with seeds: {} started on: {}", currentView(), context.getId(),
                     seeds.size(), node.getId());

            var seeding = new CompletableFuture<Redirect>();
            var timer = metrics == null ? null : metrics.seedDuration().time();
            seeding.whenComplete(redirect(duration, scheduler, timer));

            var seedlings = new SliceIterator<>("Seedlings", node,
                                                seeds.stream()
                                                     .map(s -> seedFor(s))
                                                     .map(nw -> new Participant(nw))
                                                     .filter(p -> !node.getId().equals(p.getId()))
                                                     .collect(Collectors.toList()),
                                                approaches, exec);
            AtomicReference<Runnable> reseed = new AtomicReference<>();
            reseed.set(() -> {
                seedlings.iterate((link, m) -> {
                    log.debug("Requesting Seeding from: {} on: {}", link.getMember().getId(), node.getId());
                    return link.seed(credentials(bootstrapView()));
                }, (futureSailor, link, m) -> complete(seeding, futureSailor, m), () -> {
                    if (!seeding.isDone()) {
                        scheduler.schedule(exec(() -> reseed.get().run()), params.retryDelay().toNanos(),
                                           TimeUnit.NANOSECONDS);
                    }
                }, scheduler, params.retryDelay());
            });
            reseed.get().run();
        }

        private boolean validate(Gateway g, Digest view, CompletableFuture<Bound> gateway,
                                 HashMultiset<Biff> memberships, HashMultiset<Integer> cards, Set<SignedNote> seeds,
                                 int majority, Set<SignedNote> joined) {
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
                                                   seeds.stream().map(sn -> new NoteWrapper(sn, digestAlgo)).toList(),
                                                   cardinality, joined, BloomFilter.from(g.getMembers())))) {
                        log.info("Gateway acquired: {} context: {} on: {}", view, context.getId(), node.getId());
                    }
                    return true;
                }
            }
            log.info("Gateway: {} majority not achieved: {} required: {} count: {} context: {} on: {}", view,
                     cardinality, majority, cards.size(), context.getId(), node.getId());
            return false;
        }
    }

    private record Bound(Digest view, List<NoteWrapper> seeds, int cardinality, Set<SignedNote> joined,
                         BloomFilter<Digest> bff) {}

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

    private class ViewManagement {
        private final AtomicInteger                            attempt      = new AtomicInteger();
        private final AtomicReference<Digest>                  crown;
        private final AtomicReference<Digest>                  currentView  = new AtomicReference<>();
        private final AtomicBoolean                            joined       = new AtomicBoolean();
        private final ConcurrentMap<Digest, NoteWrapper>       joins        = new ConcurrentSkipListMap<>();
        private CompletableFuture<Void>                        onJoined;
        private final Map<Digest, Consumer<List<NoteWrapper>>> pendingJoins = new ConcurrentSkipListMap<>();
        private final AtomicReference<ViewChange>              vote         = new AtomicReference<>();

        private ViewManagement() {
            crown = new AtomicReference<>(digestAlgo.getOrigin());
        }

        private boolean addToView(NoteWrapper note) {
            var newMember = false;
            Participant m = context.getMember(note.getId());
            if (m == null) {
                newMember = true;
                if (!validation.verify(note.getCoordinates(), note.getSignature(),
                                       note.getWrapped().getNote().toByteString())) {
                    log.trace("invalid participant note from: {} on: {}", note.getId(), node.getId());
                    if (metrics != null) {
                        metrics.filteredNotes().mark();
                    }
                    return false;
                }
                m = new Participant(note);
                context.add(m);
            } else {
                NoteWrapper current = m.getNote();
                if (!newMember && current != null) {
                    long nextEpoch = note.getEpoch();
                    long currentEpoch = current.getEpoch();
                    if (nextEpoch <= currentEpoch) {
//                        log.trace("Note: {} epoch out of date: {} current: {} on: {}", note.getId(), nextEpoch,
//                                  currentEpoch, node.getId());
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
            }

            if (metrics != null) {
                metrics.notes().mark();
            }

            var member = m;
            stable(() -> {
                var accused = member.isAccused();
                stopRebuttalTimer(member);
                member.setNote(note);
                recover(member);
                if (accused) {
                    checkInvalidations(member);
                }
                if (!onJoined.isDone() && context.totalCount() == context.cardinality()) {
                    assert context.totalCount() == context.cardinality();
                    join();
                } else {
                    assert context.totalCount() <= context.cardinality() : "total: " + context.totalCount() + " card: "
                    + context.cardinality();
                }
            });
            return true;
        }

        private void bootstrap(NoteWrapper nw, final ScheduledExecutorService sched, final Duration dur) {
            joins.put(nw.getId(), nw);
            context.activate(node);

            crown.set(digestAlgo.getOrigin());
            viewChange(() -> install(new Ballot(currentView(), Collections.emptyList(),
                                                Collections.singletonList(node.getId()), digestAlgo)));

            scheduleViewChange();

            futureGossip = sched.schedule(Utils.wrapped(() -> gossip(dur, sched), log),
                                          Entropy.nextBitsStreamLong(dur.toNanos()), TimeUnit.NANOSECONDS);

            log.info("Bootstrapped view: {} cardinality: {} count: {} context: {} on: {}", currentView(),
                     context.cardinality(), context.activeCount(), context.getId(), node.getId());
            onJoined.complete(null);
            joined.set(true);
            if (metrics != null) {
                metrics.viewChanges().mark();
            }
        }

        private void clear() {
            joins.clear();
            resetBootstrapView();
        }

        /**
         * Finalize the view change
         */
        private void finalizeViewChange() {
            viewChange(() -> {
                final var cardinality = context.memberCount();
                final var superMajority = cardinality - ((cardinality - 1) / 4);
                if (observations.size() < superMajority) {
                    log.trace("Do not have supermajority: {} required: {} local joins: {} leaving: {} for: {} on: {}",
                              observations.size(), superMajority, joins.size(), context.offlineCount(), currentView(),
                              node.getId());
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
                    log.info("Fast path consensus successful: {} required: {} cardinality: {} for: {} on: {}", max,
                             superMajority, context.cardinality(), currentView(), node.getId());
                    install(max.getElement());
                } else {
                    @SuppressWarnings("unchecked")
                    final var reversed = Comparator.comparing(e -> ((Entry<Ballot>) e).getCount()).reversed();
                    log.info("Fast path consensus failed: {}, required: {} cardinality: {} ballots: {} for: {} on: {}",
                             observations.size(), superMajority, context.cardinality(),
                             ballots.entrySet().stream().sorted(reversed).limit(1).toList(), currentView(),
                             node.getId());
                }

                scheduleViewChange();
                timers.remove(FINALIZE_VIEW_CHANGE);
                vote.set(null);
                observations.clear();
            });
        }

        /**
         * Initiate the view change
         */
        private void initiateViewChange() {
            stable(() -> {
                if (vote.get() != null) {
                    log.trace("Vote already cast for: {} on: {}", currentView(), node.getId());
                    return;
                }
                // Use pending rebuttals as a proxy for stability
                if (!pendingRebuttals.isEmpty()) {
                    log.debug("Pending rebuttals: {} view: {} on: {}", pendingRebuttals.size(), currentView(),
                              node.getId());
                    scheduleViewChange(1); // 1 TTL round to check again
                    return;
                }
                scheduleFinalizeViewChange();
                final var builder = ViewChange.newBuilder()
                                              .setObserver(node.getId().toDigeste())
                                              .setCurrent(currentView().toDigeste())
                                              .setAttempt(attempt.getAndIncrement())
                                              .addAllLeaves(shunned.stream()
                                                                   .map(id -> id.toDigeste())
                                                                   .collect(Collectors.toSet()))
                                              .addAllJoins(joins.keySet().stream().map(id -> id.toDigeste()).toList());
                ViewChange change = builder.build();
                vote.set(change);
                var signature = node.sign(change.toByteString());
                observations.put(node.getId(),
                                 SignedViewChange.newBuilder()
                                                 .setChange(change)
                                                 .setSignature(signature.toSig())
                                                 .build());
                log.info("View change vote: {} joins: {} leaves: {} on: {}", currentView(), change.getJoinsCount(),
                         change.getLeavesCount(), node.getId());
            });
        }

        /**
         * Install the new view
         * 
         * @param view
         */
        private void install(Ballot view) {
            log.debug("View change: {}, pending: {} joining: {} leaving: {} local joins: {} leaving: {} on: {}",
                      currentView(), pendingJoins.size(), view.joining.size(), view.leaving.size(), joins.size(),
                      context.offlineCount(), node.getId());
            attempt.set(0);
            view.leaving.stream().filter(d -> !node.getId().equals(d)).forEach(p -> remove(p));
            context.rebalance(context.totalCount() + view.joining.size());

            // Compute the new membership
            membership = new DigestBloomFilter(currentView().fold(),
                                               Math.max(params.minimumBiffCardinality(),
                                                        (context.memberCount() + view.joining.size())),
                                               MEMBERSHIP_FPR);
            context.allMembers().forEach(p -> membership.add(p.getId()));
            view.joining().forEach(id -> membership.add(id));

            // Gather joining notes and members joining on this node
            var joiningNotes = new ArrayList<NoteWrapper>();
            var pending = view.joining.stream()
                                      .map(d -> joins.remove(d))
                                      .filter(sn -> sn != null)
                                      .peek(sn -> joiningNotes.add(sn))
                                      .peek(nw -> addToView(nw))
                                      .peek(nw -> {
                                          if (metrics != null) {
                                              metrics.joins().mark();
                                          }
                                      })
                                      .map(nw -> pendingJoins.remove(nw.getId()))
                                      .filter(p -> p != null)
                                      .toList();

            // The circle of life
            var previousView = currentView();

            // HEX-BLOOM authentication maintenance
            view.joining.forEach(d -> crown.accumulateAndGet(d, (a, b) -> a.xor(b)));
            view.leaving.forEach(d -> crown.accumulateAndGet(d, (a, b) -> a.xor(b)));
            currentView.set(crown.get().rehash());

            // Tune
            gossiper.reset();
            roundTimers.setRoundDuration(context.timeToLive());

            // Regenerate for new epoch
            node.nextNote();

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
                     context.allMembers().count(), pending.size(), view.leaving.size(), view.joining.size(),
                     node.getId());

            notifyListeners(view.joining, view.leaving);
        }

        /**
         * Formally join the view. Calculate the HEX-BLOOM crown and view, fail and stop
         * if does not match currentView
         */
        private synchronized void join() {
            assert context.totalCount() == context.cardinality();
            if (onJoined.isDone()) {
                return;
            }
            crown.set(digestAlgo.getOrigin());
            context.allMembers().forEach(p -> crown.accumulateAndGet(p.id, (a, b) -> a.xor(b)));
            if (!currentView().equals(crown.get().rehash())) {
                log.error("Crown: {} does not produce view: {} cardinality: {} count: {} on: {}", crown.get(),
                          currentView(), context.cardinality(), context.totalCount(), node.getId());
                stop();
                throw new IllegalStateException("Invalid crown");
            }
            notifyListeners(context.allMembers().map(p -> p.getId()).toList(), Collections.emptyList());
            onJoined.complete(null);
            joined.set(true);

            scheduleViewChange();

            if (metrics != null) {
                metrics.viewChanges().mark();
            }
            log.info("Joined view: {} cardinality: {} count: {} on: {}", currentView(), context.cardinality(),
                     context.totalCount(), node.getId());
        }

        private void join(Credentials credentials, Digest from, StreamObserver<Gateway> responseObserver,
                          Timer.Context timer) {
            final var join = credentials.getJoin();
            if (!joined.get()) {
                log.trace("Not joined, ignored join of view: {} from: {} on: {}", Digest.from(join.getView()), from,
                          node.getId());
                responseObserver.onNext(Gateway.getDefaultInstance());
                responseObserver.onCompleted();
                return;
            }
            stable(() -> {
                var note = new NoteWrapper(join.getNote(), digestAlgo);
                if (!from.equals(note.getId())) {
                    responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Member not match note")));
                    return;
                }
                var requestView = Digest.from(join.getView());
                final var current = membership;
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

        private BiConsumer<? super Bound, ? super Throwable> join(ScheduledExecutorService scheduler, Duration duration,
                                                                  Timer.Context timer) {
            return (bound, t) -> {
                viewChange(() -> {
                    if (t != null) {
                        log.error("Failed to join view: {}on: {}", bound.view, node.getId(), t);
                        stop();
                        return;
                    }

                    context.rebalance(bound.cardinality);
                    context.activate(node);

                    membership = bound.bff;
                    currentView.set(bound.view);

                    bound.seeds.forEach(nw -> addToView(nw));
                    bound.joined.forEach(nw -> addToView(new NoteWrapper(nw, digestAlgo)));

                    gossiper.reset();
                    roundTimers.setRoundDuration(context.timeToLive());
                    node.nextNote();

                    context.allMembers().forEach(p -> p.clearAccusations());

                    futureGossip = scheduler.schedule(() -> gossip(duration, scheduler),
                                                      Entropy.nextBitsStreamLong(params.retryDelay().toNanos()),
                                                      TimeUnit.NANOSECONDS);

                    if (timer != null) {
                        timer.stop();
                    }
                    joined.set(true);

                    log.info("Currently joining view: {} seeds: {} cardinality: {} count: {} on: {}", bound.view,
                             bound.seeds.size(), context.cardinality(), context.totalCount(), node.getId());
                    if (context.totalCount() == context.cardinality()) {
                        join();
                    }
                });
            };
        }

        private void joined(List<SignedNote> joined, Digest from, StreamObserver<Gateway> responseObserver,
                            Timer.Context timer) {
            var seedSet = new TreeSet<Participant>();
            context.successors(from, m -> context.isActive(m)).forEach(p -> seedSet.add(p));
            final var current = View.this.membership;
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

        /**
         * start a view change if there's any offline members or joining members
         */
        private void maybeViewChange() {
            if (context.offlineCount() > 0 || viewManagement.joins.size() > 0) {
                initiateViewChange();
            } else {
                scheduleViewChange();
            }
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
            JoinGossip.Builder builder = processJoins(bff);
            builder.setBff(getJoinsBff(Entropy.nextSecureLong(), p).toBff());
            JoinGossip gossip = builder.build();
            if (builder.getUpdatesCount() != 0) {
                log.trace("process joins produced updates: {} on: {}", builder.getUpdatesCount(), node.getId());
            }
            return gossip;
        }

        private JoinGossip.Builder processJoins(BloomFilter<Digest> bff) {
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

        private void resetBootstrapView() {
            crown.set(digestAlgo.getOrigin());
            currentView.set(bootstrapView());
        }

        private void scheduleFinalizeViewChange() {
            scheduleFinalizeViewChange(params.finalizeViewRounds());
        }

        private void scheduleFinalizeViewChange(final int finalizeViewRounds) {
//            log.trace("View change finalization scheduled: {} rounds for: {} joining: {} leaving: {} on: {}",
//                      finalizeViewRounds, currentView(), joins.size(), context.getOffline().size(), node.getId());
            timers.put(FINALIZE_VIEW_CHANGE,
                       roundTimers.schedule(FINALIZE_VIEW_CHANGE, () -> finalizeViewChange(), finalizeViewRounds));
        }

        private void scheduleViewChange() {
            scheduleViewChange(params.viewChangeRounds());
        }

        private void scheduleViewChange(final int viewChangeRounds) {
//            log.trace("Schedule view change: {} rounds for: {}   on: {}", viewChangeRounds, currentView(),
//                      node.getId());
            timers.put(SCHEDULED_VIEW_CHANGE,
                       roundTimers.schedule(SCHEDULED_VIEW_CHANGE, () -> maybeViewChange(), viewChangeRounds));
        }

        private Redirect seed(Credentials credentials, Digest from) {
            final var join = credentials.getJoin();
            final var requestView = Digest.from(join.getView());

            if (!joined.get()) {
                log.warn("Not joined, ignored seed view: {} from: {} on: {}", requestView, from, node.getId());
                return Redirect.getDefaultInstance();
            }
            if (!bootstrapView().equals(requestView)) {
                log.warn("Invalid bootstrap view: {} from: {} on: {}", requestView, from, node.getId());
                return Redirect.getDefaultInstance();
            }
            var note = new NoteWrapper(join.getNote(), digestAlgo);
            if (!from.equals(note.getId())) {
                log.warn("Invalid bootstrap note: {} from: {} claiming: {} on: {}", requestView, from, note.getId(),
                         node.getId());
                return Redirect.getDefaultInstance();
            }
            return stable(() -> {
                var newMember = new Participant(note.getId());
                final var successors = new TreeSet<Participant>(context.successors(newMember,
                                                                                   m -> context.isActive(m)));

                log.debug("Member seeding: {} view: {} context: {} successors: {} on: {}", newMember.getId(),
                          currentView(), context.getId(), successors.size(), node.getId());
                return Redirect.newBuilder()
                               .setView(currentView().toDigeste())
                               .addAllSuccessors(successors.stream()
                                                           .filter(p -> p != null)
                                                           .map(p -> p.getSeed())
                                                           .toList())
                               .setCardinality(context.cardinality())
                               .setRings(context.getRingCount())
                               .build();
            });
        }
    }

    private static final String FINALIZE_VIEW_CHANGE  = "FINALIZE VIEW CHANGE";
    private static final Logger log                   = LoggerFactory.getLogger(View.class);
    private static final double MEMBERSHIP_FPR        = 0.0000125;
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
        return mask.cardinality() == toleranceLevel + 1 && mask.length() <= 2 * toleranceLevel + 1;
    }

    private final CommonCommunications<Approach, Service>     approaches;
    private final CommonCommunications<Fireflies, Service>    comm;
    private final Context<Participant>                        context;
    private final DigestAlgorithm                             digestAlgo;
    private final Executor                                    exec;
    private volatile ScheduledFuture<?>                       futureGossip;
    private final RingCommunications<Participant, Fireflies>  gossiper;
    private volatile BloomFilter<Digest>                      membership;
    private final FireflyMetrics                              metrics;
    private final Node                                        node;
    private final Map<Digest, SignedViewChange>               observations        = new ConcurrentSkipListMap<>();
    private final Parameters                                  params;
    private final ConcurrentMap<Digest, RoundScheduler.Timer> pendingRebuttals    = new ConcurrentSkipListMap<>();
    private final RoundScheduler                              roundTimers;
    private final Set<Digest>                                 shunned             = new ConcurrentSkipListSet<>();
    private final AtomicBoolean                               started             = new AtomicBoolean();
    private final Map<String, RoundScheduler.Timer>           timers              = new HashMap<>();
    private final EventValidation                             validation;
    private final ReadWriteLock                               viewChange          = new ReentrantReadWriteLock(true);
    private final Map<UUID, ViewChangeListener>               viewChangeListeners = new HashMap<>();
    private final ViewManagement                              viewManagement;

    public View(Context<Participant> context, ControlledIdentifierMember member, InetSocketAddress endpoint,
                EventValidation validation, Router communications, Parameters params, DigestAlgorithm digestAlgo,
                FireflyMetrics metrics, Executor exec) {
        this(context, member, endpoint, validation, communications, params, communications, digestAlgo, metrics, exec);
    }

    public View(Context<Participant> context, ControlledIdentifierMember member, InetSocketAddress endpoint,
                EventValidation validation, Router communications, Parameters params, Router gateway,
                DigestAlgorithm digestAlgo, FireflyMetrics metrics, Executor exec) {
        this.metrics = metrics;
        this.validation = validation;
        this.params = params;
        this.digestAlgo = digestAlgo;
        this.context = context;
        this.roundTimers = new RoundScheduler(String.format("Timers for: %s", context.getId()), context.timeToLive());
        viewManagement = new ViewManagement();
        viewManagement.resetBootstrapView();
        this.node = new Node(member, endpoint);
        var service = new Service();
        this.comm = communications.create(node, context.getId(), service,
                                          r -> new FfServer(service, communications.getClientIdentityProvider(), r,
                                                            exec, metrics),
                                          getCreate(metrics), Fireflies.getLocalLoopback(node));
        this.approaches = gateway.create(node, context.getId(), service, service.getClass().getCanonicalName()
        + ":approach", r -> new EntranceServer(service, gateway.getClientIdentityProvider(), r, exec, metrics),
                                         EntranceClient.getCreate(metrics), Approach.getLocalLoopback(node));
        gossiper = new RingCommunications<>(context, node, comm, exec);
        this.exec = exec;
        this.membership = new DigestBloomFilter(0x666, params.minimumBiffCardinality(), MEMBERSHIP_FPR);
    }

    /**
     * Deregister the listener with the supplied id
     *
     * @param listenerId
     */
    public void deregister(UUID listenerId) {
        viewChangeListeners.remove(listenerId);
    }

    /**
     * 
     * @return the context of the view
     */
    public Context<Participant> getContext() {
        return context;
    }

    /**
     * Register a listener to receive view change events
     *
     * @param listener - the ViewChangeListener to receive events
     * @return the UUID identifying this listener
     */
    public UUID register(ViewChangeListener listener) {
        final var id = UUID.randomUUID();
        viewChangeListeners.put(id, listener);
        return id;
    }

    /**
     * Start the View
     */
    public void start(CompletableFuture<Void> onJoin, Duration d, List<Seed> seedpods,
                      ScheduledExecutorService scheduler) {
        java.util.Objects.requireNonNull(onJoin, "Join completion must not be null");
        if (!started.compareAndSet(false, true)) {
            return;
        }
        viewManagement.onJoined = onJoin;
        var seeds = new ArrayList<>(seedpods);
        Entropy.secureShuffle(seeds);

        log.info("Starting: {} cardinality: {} tolerance: {} seeds: {} on: {}", context.getId(), context.cardinality(),
                 context.toleranceLevel(), seeds.size(), node.getId());
        viewManagement.clear();
        roundTimers.reset();
        context.clear();
        node.reset();

        var initial = Entropy.nextBitsStreamLong(d.toNanos());
        scheduler.schedule(exec(() -> new Binding(seeds, d, scheduler).seeding()), initial, TimeUnit.NANOSECONDS);

        log.info("{} started on: {}", context.getId(), node.getId());
    }

    /**
     * Start the View
     */
    public void start(Runnable onJoin, Duration d, List<Seed> seedpods, ScheduledExecutorService scheduler) {
        final var futureSailor = new CompletableFuture<Void>();
        futureSailor.whenComplete((v, t) -> {
            onJoin.run();
        });
        start(futureSailor, d, seedpods, scheduler);
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
        timers.values().forEach(t -> t.cancel());
        timers.clear();
        viewManagement.clear();
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
    private void accuse(Participant member, int ring, Throwable e) {
        if (member.isAccusedOn(ring) || member.isDisabled(ring)) {
            return; // Don't issue multiple accusations
        }
        member.addAccusation(node.accuse(member, ring));
        pendingRebuttals.computeIfAbsent(member.getId(),
                                         d -> roundTimers.schedule(() -> gc(member), params.rebuttalTimeout()));
        log.debug("Accuse {} on ring {} view: {} (timer started): {} on: {}", member.getId(), ring, currentView(),
                  e.toString(), node.getId());
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

        if (!accuser.verify(accusation.getSignature(), accusation.getWrapped().getAccusation().toByteString())) {
            log.trace("Accusation discarded, accusation by: {} accused:{} signature invalid on: {}", accuser.getId(),
                      accused.getId(), node.getId());
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
        Ring<Participant> ring = context.ring(accusation.getRingNumber());

        if (accused.isAccusedOn(ring.getIndex())) {
            Participant currentAccuser = context.getMember(accused.getAccusation(ring.getIndex()).getAccuser());
            if (!currentAccuser.equals(accuser)) {
                if (ring.isBetween(currentAccuser, accuser, accused)) {
                    accused.addAccusation(accusation);
                    pendingRebuttals.computeIfAbsent(accused.getId(),
                                                     d -> roundTimers.schedule(() -> gc(accused),
                                                                               params.rebuttalTimeout()));
                    log.debug("{} accused by: {} on ring: {} (replacing: {}) on: {}", accused.getId(), accuser.getId(),
                              ring.getIndex(), currentAccuser.getId(), node.getId());
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
            Participant predecessor = ring.predecessor(accused, m -> (!m.isAccused()) || (m.equals(accuser)));
            if (accuser.equals(predecessor)) {
                accused.addAccusation(accusation);
                if (!accused.equals(node) && !pendingRebuttals.containsKey(accused.getId())) {
                    log.debug("{} accused by: {} on ring: {} (timer started) on: {}", accused.getId(), accuser.getId(),
                              accusation.getRingNumber(), node.getId());
                    pendingRebuttals.computeIfAbsent(accused.getId(),
                                                     d -> roundTimers.schedule(() -> gc(accused),
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
        final var current = membership;
        if (!current.contains(note.getId())) {
            log.debug("Note: {} is not a member  on: {}", note.getId(), node.getId());
            if (metrics != null) {
                metrics.filteredNotes().mark();
            }
            return false;
        }

        if (!isValidMask(note.getMask(), context.toleranceLevel())) {
            log.trace("Note: {} mask invalid: {} on: {}", note.getId(), note.getMask(), node.getId());
            if (metrics != null) {
                metrics.filteredNotes().mark();
            }
            return false;
        }

        return viewManagement.addToView(note);
    }

    /**
     * Add an observation if it is for the current view and has not been previously
     * observed by the observer
     *
     * @param observation
     */
    private boolean add(SignedViewChange observation) {
        final Digest observer = Digest.from(observation.getChange().getObserver());
        final var inView = Digest.from(observation.getChange().getCurrent());
        if (!currentView().equals(inView)) {
            log.trace("Invalid view change: {} current: {} from {} on: {}", inView, currentView(), observer,
                      node.getId());
            return false;
        }
        var currentObservation = observations.get(observer);
        if (currentObservation != null) {
            if (observation.getChange().getAttempt() <= currentObservation.getChange().getAttempt()) {
                log.trace("Stale observation: {} current: {} view change: {} current: {} offline: {} on: {}",
                          observation.getChange().getAttempt(), currentObservation.getChange().getAttempt(), inView,
                          currentView(), observer, node.getId());
                return false;
            }
        }
        final var member = context.getActiveMember(observer);
        if (member == null) {
            log.trace("Cannot validate view change: {} current: {} offline: {} on: {}", inView, currentView(), observer,
                      node.getId());
            return false;
        }
        final var signature = JohnHancock.from(observation.getSignature());
        if (!member.verify(signature, observation.getChange().toByteString())) {
            return false;
        }
        return observations.put(observer.prefix(observation.getChange().getAttempt()), observation) == null;
    }

    private boolean addJoin(SignedNote sn) {
        final var note = new NoteWrapper(sn, digestAlgo);

        if (!currentView().equals(note.currentView())) {
            log.trace("Invalid join note view: {} current: {} from: {} on: {}", note.currentView(), currentView(),
                      note.getId(), node.getId());
            return false;
        }

        final var current = membership;
        if (current.contains(note.getId())) {
            log.trace("Already a member, ignoring join note from: {} on: {}", note.currentView(), currentView(),
                      note.getId(), node.getId());
            return false;
        }

        if (!isValidMask(note.getMask(), context.toleranceLevel())) {
            log.trace("Invalid join note from: {} mask invalid {} on: {}", note.getId(), note.getMask(), node.getId());
            return false;
        }

        if (!validation.verify(note.getCoordinates(), note.getSignature(),
                               note.getWrapped().getNote().toByteString())) {
            log.trace("Invalid join note from {} on: {}", note.getId(), node.getId());
            return false;
        }

        return viewManagement.joins.put(note.getId(), note) == null;
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
     * @param sa
     */
    private void amplify(Participant target) {
        context.rings()
               .filter(ring -> !target.isDisabled(ring.getIndex()) &&
                               target.equals(ring.successor(node, m -> context.isActive(m))))
               .forEach(ring -> {
                   log.trace("amplifying: {} ring: {} on: {}", target.getId(), ring.getIndex(), node.getId());
                   accuse(target, ring.getIndex(), new IllegalStateException("Amplifying accusation"));
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
                      .setAccusationBff(getAccusationsBff(Entropy.nextSecureLong(), params.fpr()).toBff())
                      .setNoteBff(getNotesBff(Entropy.nextSecureLong(), params.fpr()).toBff())
                      .setJoinBiff(getJoinsBff(Entropy.nextSecureLong(), params.fpr()).toBff())
                      .setObservationBff(getObservationsBff(Entropy.nextSecureLong(), params.fpr()).toBff())
                      .build();
    }

    private Digest currentView() {
        return viewManagement.currentView.get();
    }

    private Runnable exec(Runnable action) {
        return () -> exec.execute(Utils.wrapped(action, log));
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
        shunned.add(member.getId());
    }

    /**
     * @param seed
     * @param p
     * @return the bloom filter containing the digests of known accusations
     */
    private BloomFilter<Digest> getAccusationsBff(long seed, double p) {
        BloomFilter<Digest> bff = new BloomFilter.DigestBloomFilter(seed, Math.max(params.minimumBiffCardinality(),
                                                                                   context.cardinality()),
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
        BloomFilter<Digest> bff = new BloomFilter.DigestBloomFilter(seed, Math.max(params.minimumBiffCardinality(),
                                                                                   viewManagement.joins.size() * 2),
                                                                    p);
        viewManagement.joins.keySet().forEach(d -> bff.add(d));
        return bff;
    }

    /**
     * @param seed
     * @param p
     * @return the bloom filter containing the digests of known notes
     */
    private BloomFilter<Digest> getNotesBff(long seed, double p) {
        BloomFilter<Digest> bff = new BloomFilter.DigestBloomFilter(seed, Math.max(params.minimumBiffCardinality(),
                                                                                   context.cardinality() * 2),
                                                                    p);
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
        if (!started.get()) {
            return;
        }

        exec.execute(Utils.wrapped(() -> {
            if (context.activeCount() == 1) {
                roundTimers.tick();
            }
            gossiper.execute((link, ring) -> gossip(link, ring),
                             (futureSailor, destination) -> gossip(futureSailor, destination, duration, scheduler));
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
        roundTimers.tick();
        if (shunned.contains(link.getMember().getId())) {
            log.trace("Shunning gossip view: {} with: {} on: {}", currentView(), link.getMember().getId(),
                      node.getId());
            if (metrics != null) {
                metrics.shunnedGossip().mark();
            }
            return null;
        }

        final SayWhat gossip = stable(() -> SayWhat.newBuilder()
                                                   .setContext(context.getId().toDigeste())
                                                   .setView(currentView().toDigeste())
                                                   .setNote(node.getNote().getWrapped())
                                                   .setRing(ring)
                                                   .setGossip(commonDigests())
                                                   .build());
        try {
            return link.gossip(gossip);
        } catch (Throwable e) {
            final var p = (Participant) link.getMember();
            if (e instanceof StatusRuntimeException sre) {
                switch (sre.getStatus().getCode()) {
                case PERMISSION_DENIED:
                    log.trace("Rejected gossip: {} view: {} from: {} on: {}", sre.getStatus(), currentView(), p.getId(),
                              node.getId());
                    break;
                case RESOURCE_EXHAUSTED:
                    log.trace("Unavailable for gossip: {} view: {} from: {} on: {}", sre.getStatus(), currentView(),
                              p.getId(), node.getId());
                    break;
                default:
                    log.debug("Error gossiping: {} view: {} from: {} on: {}", sre.getStatus(), p.getId(), currentView(),
                              node.getId());
                    accuse(p, ring, sre);
                    break;

                }
                return null;
            } else {
                log.debug("Exception gossiping with {} view: {} on: {}", p.getId(), currentView(), node.getId(),
                          e.getCause());
                accuse(p, ring, e);
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
     */
    private void gossip(Optional<ListenableFuture<Gossip>> futureSailor,
                        Destination<Participant, Fireflies> destination, Duration duration,
                        ScheduledExecutorService scheduler) {
        final var member = destination.member();
        try {
            if (futureSailor.isEmpty()) {
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
                            log.trace("Update for: {} notes: {} accusations: {} joins: {} observations: {} on: {}",
                                      destination.link().getMember().getId(), update.getNotesCount(),
                                      update.getAccusationsCount(), update.getJoinsCount(),
                                      update.getObservationsCount(), node.getId());
                            destination.link()
                                       .update(State.newBuilder()
                                                    .setContext(context.getId().toDigeste())
                                                    .setView(currentView().toDigeste())
                                                    .setRing(destination.ring())
                                                    .setUpdate(update)
                                                    .build());
                        }
                    } catch (StatusRuntimeException e) {
                        handleSRE("update", destination, member, e);
                    }
                }
            } catch (ExecutionException e) {
                if (e.getCause() instanceof StatusRuntimeException sre) {
                    handleSRE("gossip", destination, member, sre);
                } else {
                    accuse(member, destination.ring(), e);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        } finally {
            futureGossip = scheduler.schedule(() -> gossip(duration, scheduler), duration.toNanos(),
                                              TimeUnit.NANOSECONDS);
        }
    }

    private void handleSRE(String type, Destination<Participant, Fireflies> destination, final Participant member,
                           StatusRuntimeException sre) {
        switch (sre.getStatus().getCode()) {
        case PERMISSION_DENIED:
            log.trace("Rejected {}: {} view: {} from: {} on: {}", type, sre.getStatus(), currentView(), member.getId(),
                      node.getId());
            break;
        case RESOURCE_EXHAUSTED:
            log.trace("Unavailable for {}: {} view: {} from: {} on: {}", type, sre.getStatus(), currentView(),
                      member.getId(), node.getId());
            break;
        default:
            log.debug("Error {}: {} from: {} on: {}", type, sre.getStatus(), member.getId(), node.getId());
            accuse(member, destination.ring(), sre);
            break;
        }
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

    private void notifyListeners(List<Digest> joining, List<Digest> leaving) {
        final var current = currentView();
        viewChangeListeners.forEach((id, listener) -> {
            try {
                log.trace("Notifying view change: {} listener: {} cardinality: {} joins: {} leaves: {} on: {} ",
                          currentView(), id, context.totalCount(), joining.size(), leaving.size(), node.getId());
                listener.viewChange(context, current, joining, leaving);
            } catch (Throwable e) {
                log.error("error in view change listener: {} on: {} ", id, node.getId(), e);
            }
        });
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
               .collect(new ReservoirSampler<>(params.maximumTxfr(), Entropy.bitsStream()))
               .forEach(a -> builder.addUpdates(a.getWrapped()));
        return builder;
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
               .collect(new ReservoirSampler<>(params.maximumTxfr(), Entropy.bitsStream()))
               .forEach(n -> builder.addUpdates(n.getWrapped()));
        return builder;
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
                    .collect(new ReservoirSampler<>(params.maximumTxfr(), Entropy.bitsStream()))
                    .forEach(n -> builder.addUpdates(n));
        return builder;
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
        if (notes.size() + accusations.size() + observe.size() + joins.size() != 0) {
            log.trace("Updating, members: {} notes: {}:{} accusations: {}:{} observations: {}:{} joins: {}:{} on: {}",
                      context.totalCount(), nCount, notes.size(), aCount, accusations.size(), oCount, observe.size(),
                      jCount, joins.size(), node.getId());
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
//            log.debug("Recovering: {} cardinality: {} count: {} on: {}", member.getId(), context.cardinality(),
//                      context.totalCount(), node.getId());
        } else {
//            log.trace("Already active: {} cardinality: {} count: {} on: {}", member.getId(), context.cardinality(),
//                      context.totalCount(), node.getId());
        }
    }

    /**
     * Redirect the receiver to the correct ring, processing any new accusations
     * 
     * @param member
     * @param gossip
     * @param ring
     */
    private boolean redirect(Participant member, Gossip gossip, int ring) {
        if (gossip.getNotes().getUpdatesCount() != 1) {
            log.warn("Redirect from: {} on ring: {} did not contain redirect member note on: {}", member.getId(), ring,
                     node.getId());
            return false;
        }
        if (gossip.getNotes().getUpdatesCount() == 1) {
            var note = new NoteWrapper(gossip.getNotes().getUpdatesList().get(0), digestAlgo);
            addToCurrentView(note);
            final Participant redirect = context.getActiveMember(note.getId());
            if (redirect == null) {
                log.trace("Ignored redirect from: {} to: {} ring: {} not currently a member on: {}", member.getId(),
                          note.getId(), ring, node.getId());
                return false;
            }
            if (gossip.getAccusations().getUpdatesCount() > 0) {
                gossip.getAccusations().getUpdatesList().forEach(s -> add(new AccusationWrapper(s, digestAlgo)));
                // Reset our epoch to whatever the group has recorded for this node
                long max = Math.max(node.getEpoch(),
                                    gossip.getAccusations()
                                          .getUpdatesList()
                                          .stream()
                                          .map(signed -> new AccusationWrapper(signed, digestAlgo))
                                          .mapToLong(a -> a.getEpoch())
                                          .max()
                                          .orElse(-1));
                node.nextNote(max + 1, currentView());
                node.clearAccusations();
            }
            log.debug("Redirected from {} to {} on ring {} on: {}", member.getId(), note.getId(), ring, node.getId());
            return true;
        } else {
            log.warn("Redirect identity from {} on ring {} is invalid on: {}", member.getId(), ring, node.getId());
            return false;
        }
    }

    /**
     * Redirect the member to the successor from this view's perspective
     *
     * @param member
     * @param ring
     * @param successor
     * @param digests
     * @return the Gossip containing the successor's Identity and Note from this
     *         view
     */
    private Gossip redirectTo(Participant member, int ring, Participant successor, Digests digests) {
        assert member != null;
        assert successor != null;
        if (successor.getNote() == null) {
            log.debug("Cannot redirect from: {} to: {} on ring: {} as note is null on: {}", node, successor, ring,
                      node.getId());
            return Gossip.getDefaultInstance();
        }

        var identity = successor.getNote();
        if (identity == null) {
            log.debug("Cannot redirect from: {} to: {} on ring: {} as note is null on: {}", node, successor, ring,
                      node.getId());
            return Gossip.getDefaultInstance();
        }
        return Gossip.newBuilder()
                     .setRedirect(false)
                     .setNotes(processNotes(BloomFilter.from(digests.getNoteBff())))
                     .setAccusations(processAccusations(BloomFilter.from(digests.getAccusationBff())))
                     .setObservations(processObservations(BloomFilter.from(digests.getObservationBff())))
                     .setJoins(viewManagement.processJoins(BloomFilter.from(digests.getJoinBiff())))
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
                 context.isActive(digest) ? "active" : "failed", digest, context.getId(), currentView(), node.getId());
        context.remove(digest);
        shunned.remove(digest);
        if (metrics != null) {
            metrics.leaves().mark();
        }
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

    private NoteWrapper seedFor(Seed seed) {
        SignedNote seedNote = SignedNote.newBuilder()
                                        .setNote(Note.newBuilder()
                                                     .setCurrentView(currentView().toDigeste())
                                                     .setHost(seed.endpoint.getHostName())
                                                     .setPort(seed.endpoint.getPort())
                                                     .setCoordinates(seed.coordinates.toEventCoords())
                                                     .setEpoch(-1)
                                                     .setMask(ByteString.copyFrom(Node.createInitialMask(context.toleranceLevel())
                                                                                      .toByteArray())))
                                        .setSignature(SignatureAlgorithm.NULL_SIGNATURE.sign(null, new byte[0]).toSig())
                                        .build();
        return new NoteWrapper(seedNote, digestAlgo);
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
     * Cancel the timer to track the accused member
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
            viewManagement.joins.entrySet()
                                .stream()
                                .filter(e -> !joinBff.contains(e.getKey()))
                                .collect(new ReservoirSampler<>(params.maximumTxfr(), Entropy.bitsStream()))
                                .forEach(e -> builder.addJoins(e.getValue().getWrapped()));
        }

        return builder.build();
    }

    private void validate(Digest from, final int ring, Digest requestView) {
        if (shunned.contains(from)) {
            log.trace("Member is shunned: {} on: {}", from, node.getId());
            throw new StatusRuntimeException(Status.UNKNOWN.withDescription("Member is shunned: " + from));
        }
        if (!started.get()) {
            log.trace("Currently offline, send unknown to: {}  on: {}", from, node.getId());
            throw new StatusRuntimeException(Status.UNKNOWN.withDescription("Member: " + node.getId() + " is offline"));
        }
        if (!requestView.equals(currentView())) {
            log.debug("Invalid view: {} current: {} ring: {} from: {} on: {}", requestView, currentView(), ring, from,
                      node.getId());
            throw new StatusRuntimeException(Status.PERMISSION_DENIED.withDescription("Invalid view: " + requestView
            + " current: " + currentView()));
        }
    }

    private void validate(Digest from, NoteWrapper note, Digest requestView, final int ring) {
        if (!from.equals(note.getId())) {
            throw new StatusRuntimeException(Status.UNAUTHENTICATED.withDescription("Member does not match: " + from));
        }
        validate(from, ring, requestView);
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
        var valid = true;
        try {
            validate(from, request.getRing(), Digest.from(request.getView()));
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
