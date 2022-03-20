/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import static com.salesforce.apollo.crypto.QualifiedBase64.digest;
import static com.salesforce.apollo.fireflies.communications.FfClient.getCreate;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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
import com.salesfoce.apollo.fireflies.proto.CertificateGossip;
import com.salesfoce.apollo.fireflies.proto.Digests;
import com.salesfoce.apollo.fireflies.proto.EncodedCertificate;
import com.salesfoce.apollo.fireflies.proto.Gossip;
import com.salesfoce.apollo.fireflies.proto.Note;
import com.salesfoce.apollo.fireflies.proto.NoteGossip;
import com.salesfoce.apollo.fireflies.proto.SignedAccusation;
import com.salesfoce.apollo.fireflies.proto.SignedNote;
import com.salesfoce.apollo.fireflies.proto.Update;
import com.salesforce.apollo.comm.EndpointProvider;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.comm.StandardEpProvider;
import com.salesforce.apollo.comm.grpc.MtlsServer;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.SigningThreshold;
import com.salesforce.apollo.crypto.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.crypto.ssl.CertificateValidator;
import com.salesforce.apollo.fireflies.communications.FfServer;
import com.salesforce.apollo.fireflies.communications.Fireflies;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.Ring;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.impl.MemberImpl;
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

    public class Node extends Participant implements SigningMember {

        /**
         * Create a mask of length 2t+1 with t randomly disabled rings
         *
         * @param toleranceLevel - t
         * @return the mask
         */
        public static BitSet createInitialMask(int toleranceLevel, SecureRandom entropy) {
            int nbits = 2 * toleranceLevel + 1;
            BitSet mask = new BitSet(nbits);
            List<Boolean> random = new ArrayList<>();
            for (int i = 0; i < toleranceLevel + 1; i++) {
                random.add(true);
            }
            for (int i = 0; i < toleranceLevel; i++) {
                random.add(false);
            }
            Collections.shuffle(random, entropy);
            for (int i = 0; i < nbits; i++) {
                if (random.get(i)) {
                    mask.set(i);
                }
            }
            return mask;
        }

        private volatile PrivateKey privateKey;
        private final SigningMember wrapped;

        public Node(SigningMember wrapped, CertificateWithPrivateKey cert) {
            super(wrapped, cert.getX509Certificate());
            this.wrapped = wrapped;
            this.privateKey = cert.getPrivateKey();
        }

        @Override
        public SignatureAlgorithm algorithm() {
            return wrapped.algorithm();
        }

        @Override
        public SslContext forClient(ClientAuth clientAuth, String alias, CertificateValidator validator,
                                    Provider provider, String tlsVersion) {
            return MtlsServer.forClient(clientAuth, alias, certificate, privateKey, validator);
        }

        @Override
        public SslContext forServer(ClientAuth clientAuth, String alias, CertificateValidator validator,
                                    Provider provider, String tlsVersion) {
            return MtlsServer.forServer(clientAuth, alias, certificate, privateKey, validator);
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
                BitSet mask = createInitialMask(context.toleranceLevel(), Utils.secureEntropy());
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
                    int index = Utils.secureEntropy().nextInt(context.getRingCount());
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

            encoded.set(EncodedCertificate.newBuilder()
                                          .setId(getId().toDigeste())
                                          .setEpoch(note.getEpoch())
                                          .setHash(certificateHash.toDigeste())
                                          .setContent(ByteString.copyFrom(derEncodedCertificate))
                                          .build());
        }
    }

    public class Participant implements Member {

        private static final Logger                         log     = LoggerFactory.getLogger(Participant.class);
        /**
         * Certificate
         */
        protected volatile X509Certificate                  certificate;
        /**
         * The hash of the member's certificate
         */
        protected final Digest                              certificateHash;
        /**
         * The DER serialized certificate
         */
        protected final byte[]                              derEncodedCertificate;
        protected final AtomicReference<EncodedCertificate> encoded = new AtomicReference<>();

        /**
         * The member's latest note
         */
        protected volatile NoteWrapper                  note;
        /**
         * The valid accusatons for this member
         */
        protected final Map<Integer, AccusationWrapper> validAccusations = new ConcurrentHashMap<>();

        private final Member wrapped;

        public Participant(Member wrapped) {
            this(wrapped, wrapped.getCertificate());
        }

        public Participant(Member wrapped, X509Certificate certificate) {
            assert wrapped != null;
            this.wrapped = wrapped;
            this.certificate = certificate;
            try {
                this.derEncodedCertificate = getCertificate().getEncoded();
            } catch (CertificateEncodingException e) {
                throw new IllegalArgumentException("Cannot encode certifiate for member: " + getId(), e);
            }
            this.certificateHash = digestAlgo.digest(derEncodedCertificate);
        }

        @Override
        public int compareTo(Member o) {
            return wrapped.compareTo(o);
        }

        @Override
        public boolean equals(Object obj) {
            return wrapped.equals(obj);
        }

        @Override
        public Filtered filtered(SigningThreshold threshold, JohnHancock signature, InputStream message) {
            return wrapped.filtered(threshold, signature, message);
        }

        @Override
        public X509Certificate getCertificate() {
            return certificate;
        }

        @Override
        public Digest getId() {
            return wrapped.getId();
        }

        @Override
        public int hashCode() {
            return wrapped.hashCode();
        }

        @Override
        public String toString() {
            return "Member[" + getId() + "]";
        }

        @Override
        public boolean verify(JohnHancock signature, InputStream message) {
            return wrapped.verify(signature, message);
        }

        @Override
        public boolean verify(SigningThreshold threshold, JohnHancock signature, InputStream message) {
            return wrapped.verify(threshold, signature, message);
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

        Digest getCertificateHash() {
            return certificateHash;
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

        EncodedCertificate getEncodedCertificate() {
            return encoded.get();
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
            encoded.set(null);
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
            encoded.set(EncodedCertificate.newBuilder()
                                          .setId(getId().toDigeste())
                                          .setEpoch(note.getEpoch())
                                          .setHash(certificateHash.toDigeste())
                                          .setContent(ByteString.copyFrom(derEncodedCertificate))
                                          .build());
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
         * @param completion TODO
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
         * Perform one round of monitoring the members assigned to this view. Monitor
         * the unique set of members who are the live successors of this member on the
         * rings of this view.
         */
        public void monitor() {
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
         * The first message in the anti-entropy protocol. Process any digests from the
         * inbound gossip digest. Respond with the Gossip that represents the digests
         * newer or not known in this view, as well as updates from this node based on
         * out of date information in the supplied digests.
         *
         * @param ring        - the index of the gossip ring the inbound member is
         *                    gossiping from
         * @param digests     - the inbound gossip
         * @param from        - verified id of the sending member
         * @param certificate - the certificate for the sending member
         * @param note        - the signed note for the sending member
         * @return Teh response for Moar gossip - updates this node has which the sender
         *         is out of touch with, and digests from the sender that this node
         *         would like updated.
         */
        public Gossip rumors(int ring, Digests digests, Digest from, X509Certificate certificate, SignedNote note) {
            if (ring >= context.getRingCount() || ring < 0) {
                log.info("invalid ring {} from {}", ring, from);
                return emptyGossip();
            }

            Participant member = context.getMember(from);
            if (member == null) {
                add(certificate);
                member = context.getMember(from);
                if (member == null) {
                    log.info("invalid credentials on ring {} from {}", ring, from);
                    // invalid creds
                    return emptyGossip();
                }
            }

            add(new NoteWrapper(note, digestAlgo));

            Participant successor = context.ring(ring).successor(member, m -> context.isActive(m.getId()));
            if (successor == null) {
                return emptyGossip();
            }
            if (!successor.equals(node)) {
                redirectTo(member, ring, successor);
            }
            long seed = Utils.secureEntropy().nextLong();
            return Gossip.newBuilder()
                         .setRedirect(false)
                         .setCertificates(processCertificateDigests(from, BloomFilter.from(digests.getCertificateBff()),
                                                                    seed, fpr))
                         .setNotes(processNoteDigests(from, BloomFilter.from(digests.getNoteBff()), seed, fpr))
                         .setAccusations(processAccusationDigests(BloomFilter.from(digests.getAccusationBff()), seed,
                                                                  fpr))
                         .build();
        }

        public void start(Duration d, List<X509Certificate> seeds, ScheduledExecutorService scheduler) {
            if (!started.compareAndSet(false, true)) {
                return;
            }
            comm.register(context.getId(), service);
            context.activate(node);
            node.nextNote();
            recover(node);
            List<Digest> seedList = new ArrayList<>();
            seeds.stream()
                 .map(cert -> new Participant(certToMember.from(cert)))
                 .peek(m -> seedList.add(m.getId()))
                 .forEach(m -> addSeed(m));

            long interval = d.toMillis();
            int initialDelay = Utils.secureEntropy().nextInt((int) interval * 2);
            futureGossip = scheduler.schedule(() -> {
                try {
                    oneRound(d, scheduler);
                } catch (Throwable e) {
                    log.error("unexpected error during gossip round", e);
                }
            }, initialDelay, TimeUnit.MILLISECONDS);
            log.info("{} started, initial delay: {} ms seeds: {}", node.getId(), initialDelay, seedList);
        }

        /**
         * stop the view from performing gossip and monitoring rounds
         */
        public void stop() {
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

        /**
         * The third and final message in the anti-entropy protocol. Process the inbound
         * update from another member.
         *
         * @param ring
         * @param update
         * @param from
         */
        public void update(int ring, Update update, Digest from) {
            processUpdates(update.getCertificatesList(), update.getNotesList(), update.getAccusationsList());
        }

        /**
         * @return the next ClientCommunications in the next ring
         */
        Fireflies nextRing() {
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
    }

    private static final CertificateFactory cf;

    private static Logger log = LoggerFactory.getLogger(View.class);
    static {
        try {
            cf = CertificateFactory.getInstance("X.509");
        } catch (CertificateException e) {
            throw new IllegalStateException("Cannot get X.509 factory", e);
        }
    }

    public static Gossip emptyGossip() {
        return Gossip.getDefaultInstance();
    }

    public static EndpointProvider getStandardEpProvider(Member node) {
        return new StandardEpProvider(Member.portsFrom(node.getCertificate()), ClientAuth.REQUIRE,
                                      CertificateValidator.NONE);
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

    /**
     * Constructor for Member implementations
     */
    private final CertToMember certToMember;

    /**
     * Communications with other members
     */
    private final CommonCommunications<Fireflies, Service> comm;

    /**
     * View context
     */
    private final Context<Participant> context;

    private final DigestAlgorithm digestAlgo;

    /**
     * The false positive rate for the bloomfilters used for the antientropy
     * protocol
     */
    private final double fpr;

    private final FireflyMetrics metrics;

    /**
     * This member
     */
    private final Node node;

    /**
     * Pending rebutal timers by member id
     */
    private final ConcurrentMap<Digest, FutureRebutal> pendingRebutals = new ConcurrentHashMap<>();

    /**
     * Current gossip round
     */
    private final AtomicLong round = new AtomicLong(0);

    /**
     * Rebutals sorted by target round
     */
    private final ConcurrentSkipListSet<FutureRebutal> scheduledRebutals = new ConcurrentSkipListSet<>();

    /**
     * The gossip service
     */
    private final Service service = new Service();

    private final CertificateValidator validator;

    public View(Context<Participant> context, SigningMember member, CertificateWithPrivateKey cert,
                CertificateValidator validator, Router communications, double fpr, DigestAlgorithm digestAlgo,
                FireflyMetrics metrics) {
        this(context, member, cert, new CertToMember() {

            @Override
            public Member from(X509Certificate cert) {
                return new MemberImpl(cert);
            }

            @Override
            public Digest idOf(X509Certificate cert) {
                return Member.getMemberIdentifier(cert);
            }
        }, validator, communications, fpr, digestAlgo, metrics);
    }

    public View(Context<Participant> context, SigningMember member, CertificateWithPrivateKey cert,
                CertToMember certToMember, CertificateValidator validator, Router communications, double fpr,
                DigestAlgorithm digestAlgo, FireflyMetrics metrics) {
        this.metrics = metrics;
        this.certToMember = certToMember;
        this.fpr = fpr;
        this.digestAlgo = digestAlgo;
        this.validator = validator;
        this.context = context;
        this.node = new Node(member, cert);
        this.comm = communications.create(node, context.getId(), service,
                                          r -> new FfServer(service, communications.getClientIdentityProvider(),
                                                            metrics, r),
                                          getCreate(metrics), Fireflies.getLocalLoopback(node));
        add(node);
        log.info("View [{}]", node.getId());
    }

    public Context<Participant> getContext() {
        return context;
    }

    /**
     * @return the collection of all failed members
     */
    public Collection<Participant> getFailed() {
        return context.getOffline();
    }

    /**
     * @return the collection of all live members
     */
    public Collection<Participant> getLive() {
        return context.getActive();
    }

    /**
     * @return The member that represents this View
     */
    public Node getNode() {
        return node;
    }

    /**
     * Start the View
     */
    public void start(Duration d, List<X509Certificate> seeds, ScheduledExecutorService scheduler) {
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
     * Accuse the member on the list of rings. If the member has disabled the ring
     * in its mask, do not issue the accusation
     *
     * @param member
     * @param ring
     */
    void accuseOn(Participant member, int ring) {
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
    void add(AccusationWrapper accusation) {
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
    void add(AccusationWrapper accusation, Participant accuser, Participant accused) {
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
     * Add a new inbound certificate
     *
     * @param cert
     * @return the added member or the real member associated with this certificate
     */
    Participant add(CertWithHash cert) {
        Digest id = certToMember.idOf(cert.certificate);
        Participant member = context.getMember(id);
        if (member != null) {
            update(member, cert);
            return member;
        }
        member = new Participant(certToMember.from(cert.certificate), cert.certificate);
        log.trace("Adding member via cert: {} on: {}", member.getId(), node.getId());
        return add(member);
    }

    /**
     * add an inbound note to the view
     *
     * @param note
     */
    boolean add(NoteWrapper note) {
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
    Participant add(Participant member) {
        Participant previous = context.getMember(member.getId());
        if (previous == null) {
            context.add(member);
            recover(member);
            return member;
        }
        return previous;
    }

    /**
     * add an inbound member certificate to the view
     *
     * @param cert
     */
    void add(X509Certificate cert) {
        add(new CertWithHash(cert, null, null));
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
    void addSeed(Participant seed) {
        SignedNote seedNote = SignedNote.newBuilder()
                                        .setNote(Note.newBuilder()
                                                     .setId(seed.getId().toDigeste())
                                                     .setEpoch(-1)
                                                     .setMask(ByteString.copyFrom(Node.createInitialMask(context.toleranceLevel(),
                                                                                                         Utils.secureEntropy())
                                                                                      .toByteArray())))
                                        .setSignature(SignatureAlgorithm.NULL_SIGNATURE.sign(null, new byte[0]).toSig())
                                        .build();
        seed.setNote(new NoteWrapper(seedNote, digestAlgo));
        context.activate(seed);
    }

    /**
     * deserialize the DER encoded certificate. Validate it is signed by the pinned
     * CA certificate trusted.
     *
     * @param encoded
     * @return the deserialized certificate, or null if invalid or not correctly
     *         signed.
     */
    CertWithHash certificateFrom(EncodedCertificate encoded) {
        X509Certificate certificate;
        try {
            certificate = (X509Certificate) cf.generateCertificate(new ByteArrayInputStream(encoded.getContent()
                                                                                                   .toByteArray()));
        } catch (CertificateException e) {
            log.warn("Invalid DER encoded certificate on: {}", node.getId(), e);
            return null;
        }
        try {
            validator.validateClient(new X509Certificate[] { certificate });
        } catch (CertificateException e) {
            log.warn("Invalid cert: {} on: {}", certificate.getSubjectX500Principal(), node.getId(), e);
            return null;
        }

        return new CertWithHash(certificate, digest(encoded.getHash()), encoded.getContent().toByteArray());
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
    void checkInvalidations(Participant m) {
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
    Digests commonDigests() {
        long seed = Utils.secureEntropy().nextLong();
        return Digests.newBuilder()
                      .setAccusationBff(getAccusationsBff(seed, fpr).toBff())
                      .setNoteBff(getNotesBff(seed, fpr).toBff())
                      .setCertificateBff(getCertificatesBff(seed, fpr).toBff())
                      .build();
    }

    void gc(Participant member) {
        context.offline(member);
    }

    BloomFilter<Digest> getAccusationsBff(long seed, double p) {
        BloomFilter<Digest> bff = new BloomFilter.DigestBloomFilter(seed,
                                                                    context.cardinality() * context.getRingCount(), p);
        context.getActive()
               .stream()
               .flatMap(m -> m.getAccusations())
               .filter(e -> e != null)
               .forEach(m -> bff.add(m.getHash()));
        return bff;
    }

    BloomFilter<Digest> getCertificatesBff(long seed, double p) {
        BloomFilter<Digest> bff = new BloomFilter.DigestBloomFilter(seed, context.cardinality(), p);
        context.allMembers().map(m -> m.getCertificateHash()).filter(e -> e != null).forEach(n -> bff.add(n));
        return bff;
    }

    BloomFilter<Digest> getNotesBff(long seed, double p) {
        BloomFilter<Digest> bff = new BloomFilter.DigestBloomFilter(seed, context.cardinality(), p);
        context.allMembers().map(m -> m.getNote()).filter(e -> e != null).forEach(n -> bff.add(n.getHash()));
        return bff;
    }

    /**
     * for testing
     *
     * @return
     */
    Map<Digest, FutureRebutal> getPendingRebutals() {
        return pendingRebutals;
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
    void gossip(int ring, Fireflies link, Runnable completion) {
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
        ListenableFuture<Gossip> futureSailor = link.gossip(context.getId(), signedNote, ring, outbound);
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
                if (e.getCause() instanceof StatusRuntimeException sre) {
                    if (sre.getStatus().getCode() == Status.NOT_FOUND.getCode() ||
                        sre.getStatus().getCode() == Status.UNAVAILABLE.getCode() ||
                        sre.getStatus().getCode() == Status.UNKNOWN.getCode()) {
                        log.trace("Cannot find/unknown: {} on: {}", link.getMember(), node.getId());
                    } else {
                        log.warn("Exception gossiping with {} on: {}", link.getMember(), node.getId(), e);
                    }
                } else {
                    log.warn("Exception gossiping with {} on: {}", link.getMember(), node.getId(), e.getCause());
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
                log.trace("inbound: redirect: {} updates: certs: {}, notes: {}, accusations: {} on: {}",
                          gossip.getRedirect(), gossip.getCertificates().getUpdatesCount(),
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
    void invalidate(Participant q, Ring<Participant> ring, Deque<Participant> check) {
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

    boolean isEmpty(Update update) {
        return update.getAccusationsCount() == 0 && update.getCertificatesCount() == 0 && update.getNotesCount() == 0;
    }

    /**
     * @param ring - the ring to gossip on
     * @return the communication link for this ring, based on current membership
     *         state
     */
    Fireflies linkFor(Integer ring) {
        Participant successor = context.ring(ring).successor(node, m -> context.isActive(m));
        if (successor == null) {
            log.debug("No successor to node on ring: {} members: {} on: {}", ring, context.ring(ring).size(),
                      node.getId());
            return null;
        }
        return linkFor(successor);
    }

    /**
     * Collect and fail all expired pending future rebutal timers that have come due
     */
    void maintainTimers() {
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
    void monitor(Fireflies link, int lastRing) {
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
    void oneRound(Duration d, ScheduledExecutorService scheduler) {
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
                    try {
                        oneRound(d, scheduler);
                    } catch (Throwable e) {
                        log.error("unexpected error during gossip round on: {}", node.getId(), e);
                    }
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
    AccusationGossip processAccusationDigests(BloomFilter<Digest> bff, long seed, double p) {
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

    CertificateGossip processCertificateDigests(Digest from, BloomFilter<Digest> bff, long seed, double p) {
        log.trace("process cert digests on:{}", node.getId());
        CertificateGossip.Builder builder = CertificateGossip.newBuilder();
        // Add all updates that this view has that aren't reflected in the inbound
        // bff
        context.allMembers()
               .filter(m -> m.getId().equals(from))
               .filter(m -> !bff.contains(m.getCertificateHash()))
               .map(m -> m.getEncodedCertificate())
               .filter(cert -> cert != null)
               .forEach(cert -> builder.addUpdates(cert));
        builder.setBff(getCertificatesBff(seed, p).toBff());
        CertificateGossip gossip = builder.build();
        log.trace("process certificates produced updates: {} on: {}", gossip.getUpdatesCount(), node.getId());
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
    NoteGossip processNoteDigests(Digest from, BloomFilter<Digest> bff, long seed, double p) {
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
     * Process the updates of the supplied juicy gossip. This is the Jesus Nut of
     * state change driving the view.
     *
     * @param gossip
     */
    void processUpdates(Gossip gossip) {
        processUpdates(gossip.getCertificates().getUpdatesList(), gossip.getNotes().getUpdatesList(),
                       gossip.getAccusations().getUpdatesList());
    }

    /**
     * Process the updates of the supplied juicy gossip. This is the Jesus Nut of
     * state change driving the view.
     *
     * @param certificatUpdates
     * @param list
     * @param list2
     */
    void processUpdates(List<EncodedCertificate> certificatUpdates, List<SignedNote> list,
                        List<SignedAccusation> list2) {
        certificatUpdates.stream()
                         .map(cert -> certificateFrom(cert))
                         .filter(cert -> cert != null)
                         .forEach(cert -> add(cert));
        list.stream().map(s -> new NoteWrapper(s, digestAlgo)).forEach(note -> add(note));
        list2.stream().map(s -> new AccusationWrapper(s, digestAlgo)).forEach(accusation -> add(accusation));

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
    void recover(Participant member) {
        if (context.activate(member)) {
            log.info("Recovering: {} on: {}", member.getId(), node.getId());
        } else {
            log.trace("Already active: {} on: {}", member.getId(), node.getId());
        }
    }

    /**
     * Redirect the member to the successor from this view's perspective
     *
     * @param member
     * @param ring
     * @param successor
     * @return the Gossip containing the successor's Certificate and Note from this
     *         view
     */
    Gossip redirectTo(Participant member, int ring, Participant successor) {
        assert member != null;
        assert successor != null;
        if (successor.getNote() == null) {
            log.debug("Cannot redirect from {} to {} on ring: {} as note is null on: {}", node, successor, ring,
                      node.getId());
            return Gossip.getDefaultInstance();
        }

        var encodedCertificate = successor.getEncodedCertificate();
        if (encodedCertificate == null) {
            log.debug("Cannot redirect from {} to {} on ring: {} as note is null on: {}", node, successor, ring,
                      node.getId());
            return Gossip.getDefaultInstance();
        }

        log.debug("Redirecting from {} to {} on ring {} on: {}", member, successor, ring, node.getId());
        return Gossip.newBuilder()
                     .setRedirect(true)
                     .setCertificates(CertificateGossip.newBuilder().addUpdates(encodedCertificate).build())
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
    Update response(Gossip gossip) {
        processUpdates(gossip);
        return updatesForDigests(gossip);
    }

    /**
     * Initiate a timer to track the accussed member
     *
     * @param m
     */
    void startRebutalTimer(Participant m) {
        pendingRebutals.computeIfAbsent(m.getId(), id -> {
            FutureRebutal future = new FutureRebutal(m, round.get() + context.timeToLive());
            scheduledRebutals.add(future);
            return future;
        });
    }

    void stopRebutalTimer(Participant m) {
        m.clearAccusations();
        log.info("New note, epoch {}, clearing accusations of {} on: {}", m.getEpoch(), m.getId(), node.getId());
        FutureRebutal pending = pendingRebutals.remove(m.getId());
        if (pending != null) {
            scheduledRebutals.remove(pending);
        }
    }

    /**
     * Update the member with a new certificate
     *
     * @param member
     * @param cert
     */
    void update(Participant member, CertWithHash cert) {
        // TODO Auto-generated method stub

    }

    /**
     * Process the gossip reply. Return the gossip with the updates determined from
     * the inbound digests.
     *
     * @param gossip
     * @return
     */
    Update updatesForDigests(Gossip gossip) {
        Update.Builder builder = Update.newBuilder();

        // certificates
        BloomFilter<Digest> certBff = BloomFilter.from(gossip.getCertificates().getBff());
        context.allMembers()
               .filter(m -> !certBff.contains((m.getCertificateHash())))
               .map(m -> m.getEncodedCertificate())
               .filter(ec -> ec != null)
               .forEach(cert -> builder.addCertificates(cert));

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

    private Fireflies linkFor(Participant m) {
        try {
            return comm.apply(m, node);
        } catch (Throwable e) {
            log.debug("error opening connection to {}: {} on: {}", m.getId(),
                      (e.getCause() != null ? e.getCause() : e).getMessage(), node.getId());
        }
        return null;
    }

    private void redirect(Participant member, Gossip gossip, int ring) {
        if (gossip.getCertificates().getUpdatesCount() != 1 && gossip.getNotes().getUpdatesCount() != 1) {
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
        CertWithHash certificate = certificateFrom(gossip.getCertificates().getUpdates(0));
        if (certificate != null) {
            add(certificate);
            SignedNote signed = gossip.getNotes().getUpdates(0);
            NoteWrapper note = new NoteWrapper(signed, digestAlgo);
            add(note);
            gossip.getAccusations().getUpdatesList().forEach(s -> add(new AccusationWrapper(s, digestAlgo)));
            log.debug("Redirected from {} to {} on ring {} on: {}", member.getId(), note.getId(), ring, node.getId());
        } else {
            log.warn("Redirect certificate from {} on ring {} is null on: {}", member.getId(), ring, node.getId());
        }
    }
}
