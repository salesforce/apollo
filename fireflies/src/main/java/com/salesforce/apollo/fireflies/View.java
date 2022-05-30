/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import static com.salesforce.apollo.fireflies.communications.FfClient.getCreate;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.Provider;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.comm.grpc.MtlsServer;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.SigningThreshold;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.crypto.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.crypto.ssl.CertificateValidator;
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
import com.salesforce.apollo.utils.Utils;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;

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

    public record FutureRebutal(Participant member, long targetRound) implements Comparable<FutureRebutal> {
        @Override
        public int compareTo(FutureRebutal o) {
            int comparison = Long.compare(targetRound, o.targetRound);
            if (comparison != 0) {
                return comparison;
            }
            return member.getId().compareTo(o.member.getId());
        }

        @Override
        public String toString() {
            return "FutureRebutal[member=" + member + ", targetRound=" + targetRound + "]";
        }
    }

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

        private final SigningMember wrapped;

        public Node(SigningMember wrapped, IdentityWrapper identity) {
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

        SslContext forClient(ClientAuth clientAuth, String alias, CertificateValidator validator, Provider provider,
                             String tlsVersion) {
            var certWithKey = getCertificateWithPrivateKey();
            return MtlsServer.forClient(clientAuth, alias, certWithKey.getX509Certificate(),
                                        certWithKey.getPrivateKey(), validator);
        }

        SslContext forServer(ClientAuth clientAuth, String alias, CertificateValidator validator, Provider provider,
                             String tlsVersion) {
            var certWithKey = getCertificateWithPrivateKey();
            return MtlsServer.forServer(clientAuth, alias, certWithKey.getX509Certificate(),
                                        certWithKey.getPrivateKey(), validator);
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

        private CertificateWithPrivateKey getCertificateWithPrivateKey() {
            // TODO Auto-generated method stub
            return null;
        }
    }

    public class Participant implements Member {

        private static final Logger log = LoggerFactory.getLogger(Participant.class);

        /**
         * Identity
         */
        protected volatile IdentityWrapper identity;

        /**
         * The member's latest note
         */
        protected volatile NoteWrapper                  note;
        /**
         * The valid accusatons for this member
         */
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

        AccusationWrapper getAccusation(int index) {
            return validAccusations.get(index);
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
         * The scheduled gossip round
         */
        private volatile ScheduledFuture<?> futureGossip;

        /**
         * Last ring gossiped with
         */
        private volatile int lastRing = -1;

        /**
         * Service lifecycle
         */
        private final AtomicBoolean started = new AtomicBoolean();

        /**
         * Perform ye one ring round of gossip. Gossip is performed per ring, requiring
         * 2 * tolerance + 1 gossip rounds across all rings.
         *
         * @param completion
         */
        public void gossip(Runnable completion) {
            if (!started.get()) {
                return;
            }
            Fireflies link = nextRing();

            if (link == null) {
                log.debug("No members to gossip with on ring: {}", lastRing);
                return;
            }
            log.trace("gossiping with {} on {}", link.getMember(), lastRing);
            View.this.gossip(lastRing, link, completion);
        }

        public boolean isStarted() {
            return started.get();
        }

        /**
         * The first message in the anti-entropy protocol. Process any digests from the
         * inbound gossip digest. Respond with the Gossip that represents the digests
         * newer or not known in this view, as well as updates from this node based on
         * out of date information in the supplied digests.
         *
         * @param ring     - the index of the gossip ring the inbound member is
         *                 gossiping from
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
                log.debug("invalid ring {} from {}", ring, from);
                return Gossip.getDefaultInstance();
            }
            var wrapper = new IdentityWrapper(digestAlgo.digest(identity.toByteString()), identity);
            if (!from.equals(wrapper.identifier())) {
                log.debug("invalid identity on ring {} from {}", ring, from);
                return Gossip.getDefaultInstance();
            }

            Participant member = context.getMember(from);
            if (member == null) {
                add(wrapper);
                member = context.getMember(from);
                if (member == null) {
                    log.debug("No member on ring {} from {}", ring, from);
                    return Gossip.getDefaultInstance();
                }
            }

            add(new NoteWrapper(note, digestAlgo));

            Participant successor = context.ring(ring).successor(member, m -> context.isActive(m.getId()));
            if (successor == null) {
                log.debug("invalid gossip from: {} on ring: {} on: {}", from, ring, member.getId());
                return Gossip.getDefaultInstance();
            }
            if (!successor.equals(node)) {
                redirectTo(member, ring, successor);
            }
            long seed = Entropy.nextSecureLong();
            return Gossip.newBuilder()
                         .setRedirect(false)
                         .setIdentities(processIdentityDigests(from, BloomFilter.from(digests.getIdentityBff()), seed,
                                                               fpr))
                         .setNotes(processNoteDigests(from, BloomFilter.from(digests.getNoteBff()), seed, fpr))
                         .setAccusations(processAccusationDigests(BloomFilter.from(digests.getAccusationBff()), seed,
                                                                  fpr))
                         .build();
        }

        public void start(Duration d, List<Identity> seeds, ScheduledExecutorService scheduler) {
            if (!started.compareAndSet(false, true)) {
                return;
            }
            comm.register(context.getId(), service);
            context.activate(node);
            node.nextNote();
            recover(node);
            List<Digest> seedList = new ArrayList<>();
            seeds.stream()
                 .map(identity -> new Participant(new IdentityWrapper(digestAlgo.digest(identity.toByteString()),
                                                                      identity)))
                 .peek(m -> seedList.add(m.getId()))
                 .forEach(m -> addSeed(m));

            long interval = d.toMillis();
            int initialDelay = Entropy.nextSecureInt((int) interval * 2);
            futureGossip = scheduler.schedule(() -> {
                exec.execute(Utils.wrapped(() -> {
                    try {
                        oneRound(exec, d, scheduler);
                    } catch (Throwable e) {
                        log.error("unexpected error during gossip round", e);
                    }
                }, log));
            }, initialDelay, TimeUnit.MILLISECONDS);
            log.info("{} started, initial delay: {} ms seeds: {}", node.getId(), initialDelay, seedList);
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
                log.debug("invalid update from: {} on ring: {} on: {}", from, ring, from);
                return;
            }
            Participant successor = context.ring(ring).successor(member, m -> context.isActive(m.getId()));
            if (successor == null) {
                log.debug("invalid update from: {} on ring: {} on: {}", from, ring, member.getId());
                return;
            }
            if (!successor.equals(node)) {
                log.debug("invalid update from: {} on ring: {} on: {}", from, ring, member.getId());
                return;
            }
            processUpdates(update.getIdentitiesList(), update.getNotesList(), update.getAccusationsList());
        }

        /**
         * Perform one round of monitoring the members assigned to this view. Monitor
         * the unique set of members who are the live successors of this member on the
         * rings of this view.
         */
        private void monitor() {
            if (!started.get()) {
                return;
            }
            int ring = lastRing;
            if (ring < 0) {
                return;
            }
            Participant successor = context.ring(ring)
                                           .successor(node, m -> context.isActive(m.getId()) && !m.isAccused());
            if (successor == null) {
                log.info("No successor to node on ring: {}", ring);
                return;
            }

            Fireflies link = linkFor(successor);
            if (link == null) {
                log.info("Accusing: {} on: {}", successor.getId(), ring);
                accuseOn(successor, ring);
            } else {
                View.this.monitor(link, ring);
            }
        }

        /**
         * @return the next ClientCommunications in the next ring
         */
        private Fireflies nextRing() {
            Fireflies link = null;
            int last = lastRing;
            int current = (last + 1) % context.getRingCount();
            for (int i = 0; i < context.getRingCount(); i++) {
                link = linkFor(current);
                if (link != null) {
                    break;
                }
                current = (current + 1) % context.getRingCount();
            }
            lastRing = current;
            return link;
        }

        /**
         * stop the view from performing gossip and monitoring rounds
         */
        private void stop() {
            if (!started.compareAndSet(true, false)) {
                return;
            }
            comm.deregister(context.getId());
            ScheduledFuture<?> currentGossip = futureGossip;
            futureGossip = null;
            if (currentGossip != null) {
                currentGossip.cancel(false);
            }
            scheduledRebutals.clear();
            pendingRebutals.clear();
            context.getActive().forEach(m -> {
                context.offline(m);
            });
        }
    }

    private static Logger log = LoggerFactory.getLogger(View.class);

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
    private final CommonCommunications<Fireflies, Service> comm; 
    private final Context<Participant> context;
    private final DigestAlgorithm digestAlgo; 
    private final double fpr; 
    private final FireflyMetrics metrics; 
    private final Node node; 
    private final ConcurrentMap<Digest, FutureRebutal> pendingRebutals = new ConcurrentHashMap<>(); 
    private final AtomicLong round = new AtomicLong(0); 
    private final ConcurrentSkipListSet<FutureRebutal> scheduledRebutals = new ConcurrentSkipListSet<>();
    private final Service service = new Service();
    private final EventValidation validation;
    private final Executor exec;

    public View(Context<Participant> context, ControlledIdentifierMember member, InetSocketAddress endpoint,
                EventValidation validation, Router communications, double fpr, DigestAlgorithm digestAlgo,
                FireflyMetrics metrics, Executor exec) {
        this.metrics = metrics;
        this.validation = validation;
        this.fpr = fpr;
        this.digestAlgo = digestAlgo;
        this.context = context;
        var identity = identityFor(0, endpoint, member.getEvent());
        this.node = new Node(member, new IdentityWrapper(digestAlgo.digest(identity.toByteString()), identity));
        this.comm = communications.create(node, context.getId(), service,
                                          r -> new FfServer(service, communications.getClientIdentityProvider(), r,
                                                            exec, metrics),
                                          getCreate(metrics), Fireflies.getLocalLoopback(node));
        this.exec = exec;
        add(node);
        log.info("View [{}]", node.getId());
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
        service.start(d, seeds, scheduler);
    }

    /**
     * stop the view from performing gossip and monitoring rounds
     */
    public void stop() {
        service.stop();
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
     * Accuse the member on the list of rings. If the member has disabled the ring
     * in its mask, do not issue the accusation
     *
     * @param member
     * @param ring
     */
    private void accuseOn(Participant member, int ring) {
        NoteWrapper n = member.getNote();
        if (n == null) {
            return;
        }
        BitSet mask = n.getMask();
        if (mask.get(ring)) {
            log.info("{} accusing: {} ring: {}", node.getId(), member.getId(), ring);
            add(node.accuse(member, ring));
        }
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
            log.info("Accusation discarded, accused or accuser do not exist in view on: {}", node);
            return;
        }

        if (accusation.getRingNumber() > context.getRingCount()) {
            log.debug("Invalid ring in accusation: {} on: {}", accusation.getRingNumber(), node);
            return;
        }

        if (accused.getEpoch() != accusation.getEpoch()) {
            log.debug("Accusation discarded in epoch: {}  for: {} epoch: {} on: {}" + accusation.getEpoch(),
                      accused.getId(), accused.getEpoch(), node);
            return;
        }

        if (!accused.getNote().getMask().get(accusation.getRingNumber())) {
            log.debug("Member {} accussed on disabled ring {} by {} on: {}", accused.getId(),
                      accusation.getRingNumber(), accuser.getId(), node);
            return;
        }

        // verify the accusation after all other tests pass, as it's reasonably
        // expensive and we want to filter out all the noise first, before going to all
        // the trouble (and cost) to validate the sig
        if (!accuser.verify(accusation.getSignature(), accusation.getWrapped().getAccusation().toByteString())) {
            log.debug("Accusation by: {} accused:{} signature invalid on: {}", accuser.getId(), accused.getId(),
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
        Ring<Participant> ring = context.ring(accusation.getRingNumber());

        if (accused.isAccusedOn(ring.getIndex())) {
            AccusationWrapper currentAccusation = accused.getAccusation(ring.getIndex());
            Participant currentAccuser = context.getMember(currentAccusation.getAccuser());

            if (!currentAccuser.equals(accuser) && ring.isBetween(currentAccuser, accuser, accused)) {
                accused.addAccusation(accusation);
                log.info("{} accused by {} on ring {} (replacing {}) on: {}", accused.getId(), accuser.getId(),
                         ring.getIndex(), currentAccuser, node.getId());
            }
        } else {
            Participant predecessor = ring.predecessor(accused, m -> (!m.isAccused()) || (m.equals(accuser)));
            if (accuser.equals(predecessor)) {
                accused.addAccusation(accusation);
                if (!accused.equals(node) && !pendingRebutals.containsKey(accused.getId()) &&
                    context.isActive(accused.getId())) {
                    log.info("{} accused by {} on ring {} (timer started) on: {}", accused.getId(), accuser.getId(),
                             accusation.getRingNumber(), node.getId());
                    startRebutalTimer(accused);
                }
            } else {
                log.info("{} accused by {} on ring {} discarded as not predecessor {} on: {}", accused.getId(),
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
        log.trace("Adding member via identity: {} on: {}", member.getId(), node.getId());
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
            log.debug("No member for note: {} on: {}", note.getId(), node.getId());
            return false;
        }

        if (m.getEpoch() >= note.getEpoch()) {
            log.trace("Note redundant in epoch: {} (<= {} ) for: {}, failed: {} on: {}", note.getEpoch(), m.getEpoch(),
                      note.getId(), context.isOffline(m.getId()), node.getId());
            return false;
        }

        if (!isValidMask(note.getMask(), context.toleranceLevel())) {
            log.debug("Note: {} mask invalid {} on: {}", note.getId(), note.getMask(), node.getId());
            return false;
        }

        // verify the note after all other tests pass, as it's reasonably expensive and
        // we want to filter out all
        // the noise first, before going to all the trouble to validate

        if (!m.verify(note.getSignature(), note.getWrapped().getNote().toByteString())) {
            log.debug("Note signature invalid: {} on: {}", note.getId(), node.getId());
            return false;
        }

        log.trace("Adding member via note {} on: {}", m, node.getId());

        if (m.isAccused()) {
            stopRebutalTimer(m);
            checkInvalidations(m);
        }

        recover(m);

        m.setNote(note);

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
        context.offline(member);
    }

    private BloomFilter<Digest> getAccusationsBff(long seed, double p) {
        BloomFilter<Digest> bff = new BloomFilter.DigestBloomFilter(seed,
                                                                    context.cardinality() * context.getRingCount(), p);
        context.getActive()
               .stream()
               .flatMap(m -> m.getAccusations())
               .filter(e -> e != null)
               .forEach(m -> bff.add(m.getHash()));
        return bff;
    }

    private BloomFilter<Digest> getIdentityBff(long seed, double p) {
        BloomFilter<Digest> bff = new BloomFilter.DigestBloomFilter(seed, context.cardinality(), p);
        context.allMembers().map(m -> m.getIdentity()).filter(e -> e != null).forEach(n -> bff.add(n.hash));
        return bff;
    }

    private BloomFilter<Digest> getNotesBff(long seed, double p) {
        BloomFilter<Digest> bff = new BloomFilter.DigestBloomFilter(seed, context.cardinality(), p);
        context.allMembers().map(m -> m.getNote()).filter(e -> e != null).forEach(n -> bff.add(n.getHash()));
        return bff;
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
    private void gossip(int ring, Fireflies link, Runnable completion) {
        Participant member = (Participant) link.getMember();
        NoteWrapper n = node.getNote();
        if (n == null) {
            try {
                link.close();
            } catch (IOException e) {
            }
            completion.run();
            return;
        }
        SignedNote signedNote = n.getWrapped();
        Digests outbound = commonDigests();
        ListenableFuture<Gossip> futureSailor = link.gossip(context.getId(), signedNote, ring, outbound, node);
        futureSailor.addListener(() -> {
            Gossip gossip;
            try {
                gossip = futureSailor.get();
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode() == Status.NOT_FOUND.getCode() ||
                    e.getStatus().getCode() == Status.UNAVAILABLE.getCode() ||
                    e.getStatus().getCode() == Status.UNKNOWN.getCode()) {
                    log.trace("Cannot find/unknown: {} on: {}", link.getMember(), node.getId());
                } else {
                    log.warn("Exception gossiping with {} on: {}", link.getMember(), node.getId(), e);
                }
                try {
                    link.close();
                } catch (IOException e1) {
                }
                if (completion != null) {
                    completion.run();
                }
                return;
            } catch (ExecutionException e) {
                var cause = e.getCause();
                if (cause instanceof StatusRuntimeException sre) {
                    if (sre.getStatus().getCode() == Status.NOT_FOUND.getCode() ||
                        sre.getStatus().getCode() == Status.UNAVAILABLE.getCode() ||
                        sre.getStatus().getCode() == Status.UNKNOWN.getCode()) {
                        log.trace("Cannot find/unknown: {} on: {}", link.getMember(), node.getId());
                    } else {
                        log.warn("Exception gossiping with {} on: {}", link.getMember(), node.getId(), e);
                    }
                } else {
                    log.warn("Exception gossiping with {} on: {}", link.getMember(), node.getId(), cause);
                }
                try {
                    link.close();
                } catch (IOException e1) {
                }
                if (completion != null) {
                    completion.run();
                }
                return;
            } catch (Throwable e) {
                log.warn("Exception gossiping with {} on: {}", link.getMember(), node.getId(), e);
                try {
                    link.close();
                } catch (IOException e1) {
                }
                if (completion != null) {
                    completion.run();
                }
                return;
            }

            if (log.isTraceEnabled()) {
                log.trace("inbound: redirect: {} updates: identities: {}, notes: {}, accusations: {} on: {}",
                          gossip.getRedirect(), gossip.getIdentities().getUpdatesCount(),
                          gossip.getNotes().getUpdatesCount(), gossip.getAccusations().getUpdatesCount(), node.getId());
            }
            if (gossip.getRedirect()) {
                try {
                    link.close();
                } catch (IOException e) {
                }
                redirect(member, gossip, ring);
                if (completion != null) {
                    completion.run();
                }
            } else {
                Update update = response(gossip);
                if (!isEmpty(update)) {
                    link.update(context.getId(), ring, update);
                }
                try {
                    link.close();
                } catch (IOException e) {
                }
                if (completion != null) {
                    completion.run();
                }
            }
        }, r -> r.run());
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
            assert q.isAccusedOn(ring.getIndex());
            q.invalidateAccusationOnRing(ring.getIndex());
            log.debug("Invalidating accusation on ring: {} for member: {} on: {}", ring.getIndex(), q.getId(),
                      node.getId());
            if (!q.isAccused()) {
                if (context.isOffline(q)) {
                    recover(q);
                    log.debug("Member: {} recovered to accusation invalidated ring: {} on: {}", q.getId(),
                              ring.getIndex(), node.getId());
                } else {
                    stopRebutalTimer(q);
                    log.debug("Member: {} rebuts (accusation invalidated) ring: {} on: {}", q.getId(), ring.getIndex(),
                              node.getId());
                    check.add(q);
                }
            }
        }
    }

    private boolean isEmpty(Update update) {
        return update.getAccusationsCount() == 0 && update.getIdentitiesCount() == 0 && update.getNotesCount() == 0;
    }

    /**
     * @param ring - the ring to gossip on
     * @return the communication link for this ring, based on current membership
     *         state
     */
    private Fireflies linkFor(Integer ring) {
        Participant successor = context.ring(ring).successor(node, m -> context.isActive(m));
        if (successor == null) {
            log.debug("No successor to node on ring: {} members: {} on: {}", ring, context.ring(ring).size(),
                      node.getId());
            return null;
        }
        return linkFor(successor);
    }

    private Fireflies linkFor(Participant m) {
        try {
            return comm.apply(m, node);
        } catch (Throwable e) {
            log.debug("error opening connection to {}: {} on: {}", m.getId(),
                      (e.getCause() != null ? e.getCause() : e).getMessage(), node.getId());
        }
        return null;
    }

    /**
     * Collect and fail all expired pending future rebutal timers that have come due
     */
    private void maintainTimers() {
        List<FutureRebutal> expired = new ArrayList<>();
        for (Iterator<FutureRebutal> iterator = scheduledRebutals.iterator(); iterator.hasNext();) {
            FutureRebutal future = iterator.next();
            long currentRound = round.get();
            if (future.targetRound <= currentRound) {
                expired.add(future);
                iterator.remove();
            } else {
                break; // remaining in the future
            }
        }
        if (expired.isEmpty()) {
            return;
        }
        log.info("{} failing members: {}", node.getId(),
                 expired.stream().map(f -> f.member.getId()).collect(Collectors.toList()));
        expired.forEach(f -> gc(f.member));
    }

    /**
     * Check the link. If it stands accused, accuse this link on the supplied rings.
     * In the fortunate, lucky case, the list of rings is a singleton list - this is
     * fundamental to a good quality mesh. But on the off chance there's an overlap
     * in the rings, we need to only monitor each link only once, or it skews the
     * failure detection with the short interval.
     *
     * @param link     - the ClientCommunications link to check
     * @param lastRing - the ring for this link
     */
    private void monitor(Fireflies link, int lastRing) {
        try {
            link.ping(context.getId(), 200);
            log.trace("Successful ping from {} to {}", node.getId(), link.getMember().getId());
        } catch (Exception e) {
            log.debug("Exception pinging {} : {} : {} on: {}", link.getMember().getId(), e.toString(),
                      e.getCause().getMessage(), node.getId());
            accuseOn((Participant) link.getMember(), lastRing);
        } finally {
            try {
                link.close();
            } catch (IOException e) {
            }
        }
    }

    /**
     * Drive one round of the View. This involves a round of gossip() and a round of
     * monitor().
     *
     * @param scheduler
     * @param d
     */
    private void oneRound(Executor exec, Duration d, ScheduledExecutorService scheduler) {
        Timer.Context timer = metrics != null ? metrics.gossipRoundDuration().time() : null;
        round.incrementAndGet();
        try {
            service.gossip(() -> {
                try {
                    try {
                        service.monitor();
                    } catch (Throwable e) {
                        log.error("unexpected error during monitor round on: {}", node.getId(), e);
                    }
                    maintainTimers();
                } finally {
                    if (timer != null) {
                        timer.stop();
                    }
                }
                scheduler.schedule(() -> {
                    exec.execute(Utils.wrapped(() -> {
                        try {
                            oneRound(exec, d, scheduler);
                        } catch (Throwable e) {
                            log.error("unexpected error during gossip round on: {}", node.getId(), e);
                        }
                    }, log));
                }, d.toMillis(), TimeUnit.MILLISECONDS);
            });
        } catch (Throwable e) {
            log.error("unexpected error during gossip round on: {}", node.getId(), e);
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
    private AccusationGossip processAccusationDigests(BloomFilter<Digest> bff, long seed, double p) {
        AccusationGossip.Builder builder = AccusationGossip.newBuilder();
        // Add all updates that this view has that aren't reflected in the inbound
        // bff
        context.getActive()
               .stream()
               .flatMap(m -> m.getAccusations())
               .filter(a -> !bff.contains(a.getHash()))
               .forEach(a -> builder.addUpdates(a.getWrapped()));
        builder.setBff(getAccusationsBff(seed, p).toBff());
        AccusationGossip gossip = builder.build();
        log.trace("process accusations produded updates: {} on: {}", gossip.getUpdatesCount(), node.getId());
        return gossip;
    }

    private IdentityGossip processIdentityDigests(Digest from, BloomFilter<Digest> bff, long seed, double p) {
        log.trace("process identity digests on:{}", node.getId());
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
        log.trace("process identity produced updates: {} on: {}", gossip.getUpdatesCount(), node.getId());
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
    private NoteGossip processNoteDigests(Digest from, BloomFilter<Digest> bff, long seed, double p) {
        NoteGossip.Builder builder = NoteGossip.newBuilder();

        // Add all updates that this view has that aren't reflected in the inbound
        // bff
        context.getActive()
               .stream()
               .filter(m -> m.getNote() != null)
               .filter(m -> !bff.contains(m.getNote().getHash()))
               .map(m -> m.getNote())
               .forEach(n -> builder.addUpdates(n.getWrapped()));
        builder.setBff(getNotesBff(seed, p).toBff());
        NoteGossip gossip = builder.build();
        log.trace("process notes produded updates: {} on: {}", gossip.getUpdatesCount(), node.getId());
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

        if (node.isAccused()) {
            // Rebut the accusations by creating a new note and clearing the accusations
            node.nextNote();
            node.clearAccusations();
        }
    }

    /**
     * recover a member from the failed state
     *
     * @param member
     */
    private void recover(Participant member) {
        if (context.activate(member)) {
            log.info("Recovering: {} on: {}", member.getId(), node.getId());
        } else {
            log.trace("Already active: {} on: {}", member.getId(), node.getId());
        }
    }

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
     * Initiate a timer to track the accussed member
     *
     * @param m
     */
    private void startRebutalTimer(Participant m) {
        pendingRebutals.computeIfAbsent(m.getId(), id -> {
            FutureRebutal future = new FutureRebutal(m, round.get() + context.timeToLive());
            scheduledRebutals.add(future);
            return future;
        });
    }

    private void stopRebutalTimer(Participant m) {
        m.clearAccusations();
        log.info("New note, epoch {}, clearing accusations of {} on: {}", m.getEpoch(), m.getId(), node.getId());
        FutureRebutal pending = pendingRebutals.remove(m.getId());
        if (pending != null) {
            scheduledRebutals.remove(pending);
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
        context.getActive()
               .stream()
               .flatMap(m -> m.getAccusations())
               .filter(a -> !accBff.contains(a.getHash()))
               .forEach(a -> builder.addAccusations(a.getWrapped()));

        return builder.build();
    }
}
