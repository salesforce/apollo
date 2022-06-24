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
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.fireflies.proto.Accusation;
import com.salesfoce.apollo.fireflies.proto.AccusationGossip;
import com.salesfoce.apollo.fireflies.proto.Digests;
import com.salesfoce.apollo.fireflies.proto.Gossip;
import com.salesfoce.apollo.fireflies.proto.Identity;
import com.salesfoce.apollo.fireflies.proto.IdentityGossip;
import com.salesfoce.apollo.fireflies.proto.Note;
import com.salesfoce.apollo.fireflies.proto.NoteGossip;
import com.salesfoce.apollo.fireflies.proto.SignedAccusation;
import com.salesfoce.apollo.fireflies.proto.SignedNote;
import com.salesfoce.apollo.fireflies.proto.Update;
import com.salesforce.apollo.comm.RingCommunications;
import com.salesforce.apollo.comm.RingCommunications.Destination;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.SigningThreshold;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.fireflies.communications.FfServer;
import com.salesforce.apollo.fireflies.communications.Fireflies;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.Ring;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.EventValidation;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.InceptionEventImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.event.protobuf.RotationEventImpl;
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
    /**
     * Used in set reconcillation of Accusation Digests
     */
    public record AccTag(Digest id, int ring) {}

    public interface CertToMember {
        Member from(X509Certificate cert);

        Digest idOf(X509Certificate cert);
    }

    public record CertWithHash(X509Certificate certificate, Digest certificateHash, byte[] derEncoded) {}

    public record IdentityWrapper(Digest hash, Identity identity) implements Verifier {
        public InetSocketAddress endpoint() {
            return new InetSocketAddress(identity.getHost(), identity.getPort());
        }

        public long epoch() {
            return identity.getEpoch();
        }

        public Digest identifier() {
            return ((SelfAddressingIdentifier) event().getIdentifier()).getDigest();
        }

        @Override
        public Filtered filtered(SigningThreshold threshold, JohnHancock signature, InputStream message) {
            return getVerifier().filtered(threshold, signature, message);
        }

        @Override
        public boolean verify(JohnHancock signature, InputStream message) {
            return getVerifier().verify(signature, message);
        }

        @Override
        public boolean verify(SigningThreshold threshold, JohnHancock signature, InputStream message) {
            return getVerifier().verify(threshold, signature, message);
        }

        private Verifier getVerifier() {
            return new DefaultVerifier(event().getKeys());
        }

        public EstablishmentEvent event() {
            return switch (identity.getEventCase()) {
            case EVENT_NOT_SET -> null;
            case INCEPTION -> ProtobufEventFactory.toKeyEvent(identity.getInception());
            case ROTATION -> ProtobufEventFactory.toKeyEvent(identity.getRotation());
            default -> throw new IllegalArgumentException("Unexpected value: " + identity.getEventCase());
            };
        }

        public IdentityWrapper inEpoch(long epoch, DigestAlgorithm digestAlgo) {
            var next = Identity.newBuilder(identity).setEpoch(epoch).build();
            return new IdentityWrapper(digestAlgo.digest(next.toByteString()), next);
        }
    }

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

        public Node(ControlledIdentifierMember wrapped, IdentityWrapper identity) {
            super(identity);
            this.wrapped = wrapped;
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
            for (int i : validAccusations.keySet()) {
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
            var n = Note.newBuilder()
                        .setId(getId().toDigeste())
                        .setEpoch(newEpoch)
                        .setMask(ByteString.copyFrom(nextMask().toByteArray()))
                        .build();
            var signedNote = SignedNote.newBuilder()
                                       .setNote(n)
                                       .setSignature(wrapped.sign(n.toByteString()).toSig())
                                       .build();
            note = new NoteWrapper(signedNote, digestAlgo);

            identity = identity.inEpoch(newEpoch, digestAlgo);
        }
    }

    public class Participant implements Member {

        private static final Logger log = LoggerFactory.getLogger(Participant.class);

        protected volatile IdentityWrapper              identity;
        protected volatile NoteWrapper                  note;
        protected final Map<Integer, AccusationWrapper> validAccusations = new ConcurrentHashMap<>();

        public Participant(IdentityWrapper id) {
            assert id != null;
            this.identity = id;
        }

        @Override
        public int compareTo(Member o) {
            return identity.identifier().compareTo(o.getId());
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
            return identity.filtered(threshold, signature, message);
        }

        @Override
        public Digest getId() {
            return identity.identifier();
        }

        public IdentityWrapper getIdentity() {
            return identity;
        }

        @Override
        public int hashCode() {
            return identity.identifier().hashCode();
        }

        @Override
        public String toString() {
            return "Member[" + getId() + "]";
        }

        @Override
        public boolean verify(JohnHancock signature, InputStream message) {
            return identity.verify(signature, message);
        }

        @Override
        public boolean verify(SigningThreshold threshold, JohnHancock signature, InputStream message) {
            return identity.verify(threshold, signature, message);
        }

        /**
         * Add an accusation to the member
         *
         * @param accusation
         */
        void addAccusation(AccusationWrapper accusation) {
            NoteWrapper n = getNote();
            if (n == null) {
                return;
            }
            Integer ringNumber = accusation.getRingNumber();
            if (n.getEpoch() != accusation.getEpoch()) {
                log.trace("Invalid epoch discarding accusation from {} on {} ring {}", accusation.getAccuser(), getId(),
                          ringNumber);
            }
            if (n.getMask().get(ringNumber)) {
                validAccusations.put(ringNumber, accusation);
                if (log.isDebugEnabled()) {
                    log.debug("Member {} is accusing {} on {}", accusation.getAccuser(), getId(), ringNumber);
                }
            }
        }

        /**
         * clear all accusations for the member
         */
        void clearAccusations() {
            validAccusations.clear();
            log.trace("Clearing accusations for {}", getId());
        }

        AccusationWrapper getAccusation(int ring) {
            return validAccusations.get(ring);
        }

        Stream<AccusationWrapper> getAccusations() {
            return validAccusations.values().stream();
        }

        List<AccTag> getAccusationTags() {
            return validAccusations.keySet()
                                   .stream()
                                   .map(ring -> new AccTag(getId(), ring))
                                   .collect(Collectors.toList());
        }

        AccusationWrapper getEncodedAccusation(Integer ring) {
            return validAccusations.get(ring);
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
            validAccusations.remove(index);
            log.trace("Invalidating accusations of {} on {}", getId(), index);
        }

        boolean isAccused() {
            return !validAccusations.isEmpty();
        }

        boolean isAccusedOn(int index) {
            return validAccusations.containsKey(index);
        }

        void reset() {
            note = null;
            validAccusations.clear();
            log.trace("Reset {}", getId());
        }

        void setNote(NoteWrapper next) {
            NoteWrapper current = note;
            if (current != null) {
                long nextEpoch = next.getEpoch();
                long currentEpoch = current.getEpoch();
                if (currentEpoch > 0 && currentEpoch < nextEpoch - 1) {
                    log.info("discarding note for {} with wrong previous epoch {} : {}", getId(), nextEpoch,
                             currentEpoch);
                    return;
                }
            }
            note = next;
            identity = identity.inEpoch(note.getEpoch(), digestAlgo);
            clearAccusations();
        }
    }

    public class Service {

        /**
         * The first message in the anti-entropy protocol. Process any digests from the
         * inbound gossip digest. Respond with the Gossip that represents the digests
         * newer or not known in this view, as well as updates from this node based on
         * out of date information in the supplied digests.
         *
         * @param ring     - the index of the gossip ring the inbound member is
         *                 gossiping on
         * @param digests  - the inbound gossip
         * @param from     - verified identity of the sending member
         * @param identity - the identity of the sending member
         * @param note     - the signed note for the sending member
         * @return Teh response for Moar gossip - updates this node has which the sender
         *         is out of touch with, and digests from the sender that this node
         *         would like updated.
         */
        public Gossip rumors(int ring, Digests digests, Digest from, Identity identity, SignedNote note) {
            if (ring >= context.getRingCount() || ring < 0) {
                log.warn("invalid ring {} from {}", ring, from);
                return Gossip.getDefaultInstance();
            }
            var wrapper = new IdentityWrapper(digestAlgo.digest(identity.toByteString()), identity);
            if (!from.equals(wrapper.identifier())) {
                log.warn("invalid identity on ring {} from {}", ring, from);
                return Gossip.getDefaultInstance();
            }

            Participant member = context.getMember(from);
            if (member == null) {
                add(wrapper);
                member = context.getMember(from);
                if (member == null) {
                    log.warn("No member on ring {} from {}", ring, from);
                    return Gossip.getDefaultInstance();
                }
            }

            add(new NoteWrapper(note, digestAlgo));

            Participant successor = context.ring(ring).successor(member, m -> context.isActive(m.getId()));
            if (successor == null) {
                log.warn("invalid gossip from: {} on ring: {} on: {}", from, ring, member.getId());
                return Gossip.getDefaultInstance();
            }
            if (!successor.equals(node)) {
                return redirectTo(member, ring, successor);
            }
            long seed = Entropy.nextSecureLong();
            return Gossip.newBuilder()
                         .setRedirect(false)
                         .setIdentities(processIdentitys(from, BloomFilter.from(digests.getIdentityBff()), seed, fpr))
                         .setNotes(processNotes(from, BloomFilter.from(digests.getNoteBff()), seed, fpr))
                         .setAccusations(processAccusations(BloomFilter.from(digests.getAccusationBff()), seed, fpr))
                         .build();
        }

        /**
         * The third and final message in the anti-entropy protocol. Process the inbound
         * update from another member.
         *
         * @param ring
         * @param update
         * @param from
         */
        public void update(int ring, Update update, Digest from) {
            Participant member = context.getActiveMember(from);
            if (member == null) {
                log.info("No member, invalid update from: {} on ring: {} on: {}", from, ring, from);
                return;
            }
            Participant successor = context.ring(ring).successor(member, m -> context.isActive(m.getId()));
            if (successor == null) {
                log.info("No predecessor, invalid update from: {} on ring: {} on: {}", from, ring, member.getId());
                return;
            }
            if (!successor.equals(node)) {
                log.info("Incorrect predecessor, invalid update from: {} on ring: {} on: {}", from, ring,
                         member.getId());
                return;
            }
            processUpdates(update.getIdentitiesList(), update.getNotesList(), update.getAccusationsList());
        }
    }

    private static Logger log = LoggerFactory.getLogger(View.class);

    private static final int REBUTAL_TIMEOUT = 2;

    public static Identity identityFor(int epoch, InetSocketAddress endpoint, EstablishmentEvent event) {
        assert endpoint != null;
        assert event != null;
        var builder = Identity.newBuilder().setEpoch(epoch).setHost(endpoint.getHostName()).setPort(endpoint.getPort());
        if (event instanceof InceptionEventImpl incept) {
            builder.setInception(incept.toInceptionEvent_());
        } else if (event instanceof RotationEventImpl rot) {
            builder.setRotation(rot.toRotationEvent_());
        } else {
            throw new IllegalStateException("Event is not a valid type: " + event.getClass());
        }
        return builder.build();
    }

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
    private final DigestAlgorithm                             digestAlgo;
    private final Executor                                    exec;
    private final double                                      fpr;
    private volatile ScheduledFuture<?>                       futureGossip;
    private final RingCommunications<Participant, Fireflies>  gossiper;
    private final FireflyMetrics                              metrics;
    private final Node                                        node;
    private final ConcurrentMap<Digest, RoundScheduler.Timer> pendingRebutals = new ConcurrentSkipListMap<>();
    private final RoundScheduler                              roundTimers;
    private final Service                                     service         = new Service();
    private final AtomicBoolean                               started         = new AtomicBoolean();
    private final EventValidation                             validation;

    public View(Context<Participant> context, ControlledIdentifierMember member, InetSocketAddress endpoint,
                EventValidation validation, Router communications, double fpr, DigestAlgorithm digestAlgo,
                FireflyMetrics metrics, Executor exec) {
        this.metrics = metrics;
        this.validation = validation;
        this.fpr = fpr;
        this.digestAlgo = digestAlgo;
        this.context = context;
        this.roundTimers = new RoundScheduler(String.format("Timers for: %s", context.getId()), context.timeToLive());
        var identity = identityFor(0, endpoint, member.getEvent());
        this.node = new Node(member, new IdentityWrapper(digestAlgo.digest(identity.toByteString()), identity));
        this.comm = communications.create(node, context.getId(), service,
                                          r -> new FfServer(service, communications.getClientIdentityProvider(), r,
                                                            exec, metrics),
                                          getCreate(metrics), Fireflies.getLocalLoopback(node));
        gossiper = new RingCommunications<>(context, node, comm, exec);
        this.exec = exec;
        add(node);
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
    public void start(Duration d, List<Identity> seeds, ScheduledExecutorService scheduler) {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        roundTimers.reset();
        comm.register(context.getId(), service);
        context.allMembers().forEach(e -> e.clearAccusations());
        context.activate(node);
        node.nextNote();
        context.activate(node);
        List<Digest> seedList = new ArrayList<>();
        seeds.stream()
             .map(identity -> new Participant(new IdentityWrapper(digestAlgo.digest(identity.toByteString()),
                                                                  identity)))
             .peek(m -> seedList.add(m.getId()))
             .forEach(m -> addSeed(m));
        var initial = Entropy.nextBitsStreamLong(d.toNanos());
        futureGossip = scheduler.schedule(() -> {
            exec.execute(Utils.wrapped(() -> gossip(d, scheduler), log));
        }, initial, TimeUnit.NANOSECONDS);
        log.info("{} started, seeds: {}", node.getId(), seedList);
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
        NoteWrapper n = member.getNote();
        if (n == null) {
            return;
        }
        member.addAccusation(node.accuse(member, ring));
        pendingRebutals.computeIfAbsent(member.getId(), d -> roundTimers.schedule(() -> gc(member), REBUTAL_TIMEOUT));
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

            if (!currentAccuser.equals(accuser) && ring.isBetween(currentAccuser, accuser, accused)) {
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
     * Add a new inbound identity
     *
     * @param identity
     * @return the added member or the real member associated with this identity
     */
    private Participant add(IdentityWrapper identity) {
        Participant member = context.getMember(identity.identifier());
        if (member != null) {
            update(member, identity);
            return member;
        }
        member = new Participant(identity);
        log.debug("Adding member via identity: {} on: {}", member.getId(), node.getId());
        return add(member);
    }

    /**
     * add an inbound note to the view
     *
     * @param note
     */
    private boolean add(NoteWrapper note) {
        log.trace("Adding note {} : {} on: {}", note.getId(), note.getEpoch(), node.getId());
        Participant m = context.getMember(note.getId());
        if (m == null) {
            log.trace("No member for note: {} on: {}", note.getId(), node.getId());
            return false;
        }

        if (m.getEpoch() >= note.getEpoch()) {
            return false;
        }

        if (!isValidMask(note.getMask(), context.toleranceLevel())) {
            log.trace("Note: {} mask invalid {} on: {}", note.getId(), note.getMask(), node.getId());
            return false;
        }

        // verify the note after all other tests pass, as it's reasonably expensive and
        // we want to filter out all
        // the noise first, before going to all the trouble to validate

        if (!m.verify(note.getSignature(), note.getWrapped().getNote().toByteString())) {
            log.trace("Note signature invalid: {} on: {}", note.getId(), node.getId());
            return false;
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
            recover(member);
            return member;
        }
        return previous;
    }

//    private void add(SignedAlert sa) {
//        Digest issuedBy = Digest.from(sa.getAlert().getIssuedBy());
//        Digest targetDigest = Digest.from(sa.getAlert().getTarget());
//
//        // Run the gauntlet
//        Participant alerter = context.getActiveMember(issuedBy);
//        if (alerter == null) {
//            log.info("Alert discarded, isuedBy: {} target: {}, alerter does not exist in view on: {}", issuedBy,
//                     targetDigest, node);
//            return;
//        }
//
//        Participant target = context.getMember(targetDigest);
//        if (target == null) {
//            log.info("Alert discarded, isuedBy: {} target: {} does not exist in view on: {}", issuedBy, targetDigest,
//                     node);
//            return;
//        }
//
//        if (alerter.getEpoch() != sa.getAlert().getEpoch()) {
//            log.debug("Alert discarded, issued by: {} alerted on:{} invalid epoch: {} expected: {} on: {}",
//                      alerter.getId(), target, alerter.getEpoch(), sa.getAlert().getEpoch(), node.getId());
//            return;
//        }
//
//        int ring = sa.getAlert().getRing();
//        if (!target.getNote().getMask().get(ring)) {
//            log.debug("Alert discarded, issued by: {} target: {}, ring masked by target on: {}", alerter.getId(),
//                      target, ring, node.getId());
//            return;
//        }
//
//        Participant successor = context.ring(ring).successor(alerter, m -> context.isActive(m.getId()));
//        if (successor == null) {
//            log.info("Alert discarded, alerter: {} cannot issue alert on target: {} in view on: {}", issuedBy,
//                     targetDigest, node);
//            return;
//        }
//
//        JohnHancock signature = JohnHancock.from(sa.getSignature());
//        if (!alerter.verify(signature, sa.getAlert().toByteString())) {
//            log.debug("Alert discarded, issued by: {} allerted on:{} signature invalid on: {}", alerter.getId(), target,
//                      node.getId());
//            return;
//        }
//
    // It's dead, Jim
//        target.alert(issuedBy);
//        context.offline(target);
//        var hash = signature.toDigest(digestAlgo);
//        alerts.put(hash, sa);
//        alerted.add(target);
//        amplify(target);
//    }

    /**
     * For bootstrap, add the seed as a fake, non crashed member. The note is signed
     * by this node, and so will be rejected by any other node as invalid. The
     * gossip protocol of Fireflies provides redirects to what actual members
     * believe the view should be gossiping with, and so new members quickly
     * converge on a common, valid view of the membership
     *
     * @param seed
     */
    private void addSeed(Participant seed) {
        SignedNote seedNote = SignedNote.newBuilder()
                                        .setNote(Note.newBuilder()
                                                     .setId(seed.getId().toDigeste())
                                                     .setEpoch(-1)
                                                     .setMask(ByteString.copyFrom(Node.createInitialMask(context.toleranceLevel())
                                                                                      .toByteArray())))
                                        .setSignature(SignatureAlgorithm.NULL_SIGNATURE.sign(null, new byte[0]).toSig())
                                        .build();
        seed.setNote(new NoteWrapper(seedNote, digestAlgo));
        context.activate(seed);
    }

    /**
     * If we monitor the target and haven't issued an alert, do so
     * 
     * @param sa
     */
    private void amplify(Participant target) {
        context.rings()
               .filter(ring -> target.equals(ring.successor(target, m -> context.isActive(m))))
               .forEach(ring -> {
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
                      .setIdentityBff(getIdentityBff(seed, fpr).toBff())
                      .build();
    }

    private void gc(Participant member) {
        if (context.isActive(member)) {
            amplify(member);
            context.offline(member);
            log.debug("Offlining: {} on: {}", member.getId(), node.getId());

        }
    }

    private BloomFilter<Digest> getAccusationsBff(long seed, double p) {
        BloomFilter<Digest> bff = new BloomFilter.DigestBloomFilter(seed,
                                                                    context.cardinality() * context.getRingCount() * 2,
                                                                    p);
        context.allMembers().flatMap(m -> m.getAccusations()).filter(e -> e != null).forEach(m -> bff.add(m.getHash()));
        return bff;
    }

    private BloomFilter<Digest> getIdentityBff(long seed, double p) {
        BloomFilter<Digest> bff = new BloomFilter.DigestBloomFilter(seed, context.cardinality() * 2, p);
        context.active().map(m -> m.getIdentity()).filter(e -> e != null).forEach(n -> bff.add(n.hash));
        return bff;
    }

    private BloomFilter<Digest> getNotesBff(long seed, double p) {
        BloomFilter<Digest> bff = new BloomFilter.DigestBloomFilter(seed, context.cardinality() * 2, p);
        context.active().map(m -> m.getNote()).filter(e -> e != null).forEach(n -> bff.add(n.getHash()));
        return bff;
    }

    private void gossip(Duration duration, ScheduledExecutorService scheduler) {
        if (!started.get()) {
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
        NoteWrapper n = node.getNote();
        if (n == null) {
            return null;
        }
        SignedNote signedNote = n.getWrapped();
        Digests outbound = commonDigests();
        return link.gossip(context.getId(), signedNote, ring, outbound, node);
    }

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
                if (!isEmpty(update)) {
                    destination.link().update(context.getId(), destination.ring(), update);
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

    private boolean isEmpty(Update update) {
        return update.getAccusationsCount() == 0 && update.getIdentitiesCount() == 0 && update.getNotesCount() == 0;
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
        AccusationGossip gossip = builder.build();
        if (!gossip.getUpdatesList().isEmpty()) {
            log.trace("process accusations produced updates: {} on: {}", gossip.getUpdatesCount(), node.getId());
        }
        return gossip;
    }

    private IdentityGossip processIdentitys(Digest from, BloomFilter<Digest> bff, long seed, double p) {
        IdentityGossip.Builder builder = IdentityGossip.newBuilder();
        // Add all updates that this view has that aren't reflected in the inbound
        // bff
        context.allMembers()
               .filter(m -> m.getId().equals(from))
               .filter(m -> !bff.contains(m.getIdentity().hash))
               .map(m -> m.getIdentity())
               .filter(id -> id != null)
               .forEach(id -> builder.addUpdates(id.identity));
        builder.setBff(getIdentityBff(seed, p).toBff());
        IdentityGossip gossip = builder.build();
        if (!gossip.getUpdatesList().isEmpty()) {
            log.trace("process identity produced updates: {} on: {}", gossip.getUpdatesCount(), node.getId());
        }
        return gossip;
    }

    /**
     * Process the inbound notes from the gossip. Reconcile the differences between
     * the view's state and the digests of the gossip. Update the reply with the
     * list of digests the view requires, as well as proposed updates based on the
     * inbound digets that the view has more recent information
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
        NoteGossip gossip = builder.build();
        if (!gossip.getUpdatesList().isEmpty()) {
            log.trace("process notes produced updates: {} on: {}", gossip.getUpdatesCount(), node.getId());
        }
        return gossip;
    }

    /**
     * Process the updates of the supplied juicy gossip.
     *
     * @param gossip
     */
    private void processUpdates(Gossip gossip) {
        processUpdates(gossip.getIdentities().getUpdatesList(), gossip.getNotes().getUpdatesList(),
                       gossip.getAccusations().getUpdatesList());
    }

    /**
     * Process the updates of the supplied juicy gossip.
     *
     * @param identities
     * @param notes
     * @param accusations
     */
    private void processUpdates(List<Identity> identities, List<SignedNote> notes, List<SignedAccusation> accusations) {
        identities.stream()
                  .map(id -> new IdentityWrapper(digestAlgo.digest(id.toString()), id))
                  .filter(id -> validation.validate(id.event()))
                  .forEach(identity -> add(identity));
        notes.stream().map(s -> new NoteWrapper(s, digestAlgo)).forEach(note -> add(note));
        accusations.stream().map(s -> new AccusationWrapper(s, digestAlgo)).forEach(accusation -> add(accusation));
    }

    /**
     * recover a member from the failed state
     *
     * @param member
     */
    private void recover(Participant member) {
        if (context.activate(member)) {
            member.clearAccusations();
            log.debug("Recovering: {} on: {}", member.getId(), node.getId());
        } else {
            log.trace("Already active: {} on: {}", member.getId(), node.getId());
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
        if (gossip.getIdentities().getUpdatesCount() != 1 && gossip.getNotes().getUpdatesCount() != 1) {
            log.warn("Redirect response from {} on ring {} did not contain redirect member certificate and note on: {}",
                     member.getId(), ring, node.getId());
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
        if (gossip.getIdentities().getUpdatesCount() == 1) {
            var id = gossip.getIdentities().getUpdates(0);
            IdentityWrapper identity = new IdentityWrapper(digestAlgo.digest(id.toByteString()), id);
            if (validation.validate(identity.event())) {
                add(identity);
                SignedNote signed = gossip.getNotes().getUpdates(0);
                NoteWrapper note = new NoteWrapper(signed, digestAlgo);
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

        var identity = successor.getIdentity();
        if (identity == null) {
            log.debug("Cannot redirect from {} to {} on ring: {} as identity is null on: {}", node, successor, ring,
                      node.getId());
            return Gossip.getDefaultInstance();
        }

        log.debug("Redirecting from {} to {} on ring {} on: {}", member, successor, ring, node.getId());
        return Gossip.newBuilder()
                     .setRedirect(true)
                     .setIdentities(IdentityGossip.newBuilder().addUpdates(identity.identity).build())
                     .setNotes(NoteGossip.newBuilder().addUpdates(successor.getNote().getWrapped()).build())
                     .setAccusations(AccusationGossip.newBuilder()
                                                     .addAllUpdates(member.getEncodedAccusations(context.getRingCount())))
                     .build();
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
     * Update the member with a new identity
     *
     * @param member
     * @param identity
     */
    private void update(Participant member, IdentityWrapper identity) {
        // TODO Auto-generated method stub
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

        // certificates
        BloomFilter<Digest> identityBff = BloomFilter.from(gossip.getIdentities().getBff());
        context.allMembers()
               .filter(m -> !identityBff.contains((m.getIdentity().hash)))
               .map(m -> m.getIdentity())
               .filter(ec -> ec != null)
               .forEach(id -> builder.addIdentities(id.identity));

        // notes
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

        return builder.build();
    }
}
