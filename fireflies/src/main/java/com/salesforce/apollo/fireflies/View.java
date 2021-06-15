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
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

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
import com.salesfoce.apollo.fireflies.proto.Update;
import com.salesforce.apollo.comm.EndpointProvider;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.comm.StandardEpProvider;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.cert.CaValidator;
import com.salesforce.apollo.fireflies.communications.FfClient;
import com.salesforce.apollo.fireflies.communications.FfServer;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.Ring;
import com.salesforce.apollo.membership.impl.MemberImpl;
import com.salesforce.apollo.utils.BloomFilter;
import com.salesforce.apollo.utils.Utils;

import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;

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
    public static class AccTag {
        public final Digest id;
        public final int    ring;

        public AccTag(Digest id, int ring) {
            this.id = id;
            this.ring = ring;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            AccTag other = (AccTag) obj;
            if (id == null) {
                if (other.id != null)
                    return false;
            } else if (!id.equals(other.id))
                return false;
            if (ring != other.ring)
                return false;
            return true;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((id == null) ? 0 : id.hashCode());
            result = prime * result + ring;
            return result;
        }
    }

    public static class CertWithHash {
        public final X509Certificate certificate;
        public final Digest          certificateHash;
        public final byte[]          derEncoded;

        public CertWithHash(Digest certificateHash, X509Certificate certificate, byte[] derEncoded) {
            this.certificate = certificate;
            this.derEncoded = derEncoded;
            this.certificateHash = certificateHash;
        }
    }

    public class FutureRebutal implements Comparable<FutureRebutal> {
        public final Participant member;
        public final long        targetRound;

        public FutureRebutal(long targetRound, Participant member) {
            this.targetRound = targetRound;
            this.member = member;
        }

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
            return "FutureRebutal(" + node.getId() + " [member=" + member + ", targetRound=" + targetRound + "]";
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
            FfClient link = nextRing();

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
            Participant successor = context.ring(ring).successor(node, m -> !m.isFailed() && !m.isAccused());
            if (successor == null) {
                log.info("No successor to node on ring: {}", ring);
                return;
            }

            FfClient link = linkFor(successor);
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
        public Gossip rumors(int ring, Digests digests, Digest from, X509Certificate certificate, Note note) {
            if (ring >= context.getRingCount() || ring < 0) {
                log.info("invalid ring {} from {}", ring, from);
                return emptyGossip();
            }

            Participant member = view.get(from);
            if (member == null) {
                add(certificate);
                member = view.get(from);
                if (member == null) {
                    log.info("invalid credentials on ring {} from {}", ring, from);
                    // invalid creds
                    return emptyGossip();
                }
            }

            add(new NoteWrapper(getDigestAlgorithm().digest(note.toByteString()), note));

            Participant successor = getRing(ring).successor(member, m -> !m.isFailed());
            if (successor == null) {
                return emptyGossip();
            }
            if (!successor.equals(node)) {
                redirectTo(member, ring, successor);
            }
            int seed = Utils.secureEntropy().nextInt();
            return Gossip.newBuilder()
                         .setRedirect(false)
                         .setCertificates(processCertificateDigests(from, BloomFilter.from(digests.getCertificateBff()),
                                                                    seed, getParameters().falsePositiveRate))
                         .setNotes(processNoteDigests(from, BloomFilter.from(digests.getNoteBff()), seed,
                                                      getParameters().falsePositiveRate))
                         .setAccusations(processAccusationDigests(BloomFilter.from(digests.getAccusationBff()), seed,
                                                                  getParameters().falsePositiveRate))
                         .build();
        }

        public void start(Duration d, List<X509Certificate> seeds, ScheduledExecutorService scheduler) {
            if (!started.compareAndSet(false, true)) {
                return;
            }
            comm.register(context.getId(), service);
            node.setFailed(false);
            node.nextNote();
            recover(node);
            List<Digest> seedList = new ArrayList<>();
            seeds.stream()
                 .map(cert -> new Participant(new MemberImpl(cert), node.getParameters()))
                 .peek(m -> seedList.add(m.getId()))
                 .forEach(m -> addSeed(m));

            long interval = d.toMillis();
            int initialDelay = Utils.secureEntropy().nextInt((int) interval * 2);
            futureGossip = scheduler.schedule(() -> fjPool.execute(() -> {
                try {
                    oneRound(d, scheduler);
                } catch (Throwable e) {
                    log.error("unexpected error during gossip round", e);
                }
            }), initialDelay, TimeUnit.MILLISECONDS);
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
                m.setFailed(true);
                context.offline(m);
            });
            context.clear();
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
        FfClient nextRing() {
            FfClient link = null;
            int last = lastRing;
            int current = (last + 1) % getParameters().rings;
            for (int i = 0; i < getParameters().rings; i++) {
                link = linkFor(current);
                if (link != null) {
                    break;
                }
                current = (current + 1) % getParameters().rings;
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

    public static EndpointProvider getStandardEpProvider(Node node) {
        return new StandardEpProvider(Member.portsFrom(node.getCertificate()), ClientAuth.REQUIRE, CaValidator.NONE);
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
    public static boolean isValidMask(BitSet mask, FirefliesParameters parameters) {
        return mask.cardinality() == parameters.toleranceLevel + 1;
    }

    /**
     * Communications with other members
     */
    private final CommonCommunications<FfClient, Service> comm;

    /**
     * View context
     */
    private final Context<Participant> context;

    /**
     * The analytical diameter of the graph of members
     */
    private final int diameter;

    private final ForkJoinPool fjPool;

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
     * Core round listeners
     */
    private final List<Runnable> roundListeners = new CopyOnWriteArrayList<>();

    /**
     * Rebutals sorted by target round
     */
    private final ConcurrentSkipListSet<FutureRebutal> scheduledRebutals = new ConcurrentSkipListSet<>();

    /**
     * The gossip service
     */
    private final Service service = new Service();

    /**
     * The view of all known members
     */
    private final ConcurrentMap<Digest, Participant> view = new ConcurrentHashMap<>();

    public View(Digest id, Node node, Router communications, FireflyMetrics metrics) {
        this(id, node, communications, metrics, ForkJoinPool.commonPool());
    }

    public View(Digest id, Node node, Router communications, FireflyMetrics metrics, ForkJoinPool fjPool) {
        this.metrics = metrics;
        this.node = node;
        this.fjPool = fjPool;
        this.comm = communications.create(node, id, service, r -> new FfServer(service,
                communications.getClientIdentityProvider(), metrics, r), getCreate(metrics));
        context = new Context<>(id, getParameters().rings);
        diameter = context.diameter(getParameters().cardinality);
        assert diameter > 0 : "Diameter must be greater than zero: " + diameter;
        add(node);
        log.info("View [{}]\n  Parameters: {}", node.getId(), getParameters());
    }

    public Context<? extends Member> getContext() {
        return context;
    }

    /**
     * @return the analytical diameter of the graph of members
     */
    public int getDiameter() {
        return diameter;
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
     * @return the maximum number of members allowed for the view
     */
    public int getMaximumCardinality() {
        return getParameters().cardinality;
    }

    /**
     * @return The member that represents this View
     */
    public Node getNode() {
        return node;
    }

    /**
     * @return the parameters
     */
    public FirefliesParameters getParameters() {
        return node.getParameters();
    }

    /**
     * @param ring
     * @return the Ring corresponding to the index
     */
    public Ring<Participant> getRing(int ring) {
        return context.ring(ring);
    }

    /**
     * @return the List of Rings that this view maintains
     */
    public List<Ring<Participant>> getRings() {
        return context.rings().collect(Collectors.toList());
    }

    /**
     * current round of gossip for this view
     */
    public long getRound() {
        return round.get();
    }

    /**
     * @return the scheduledRebutals
     */
    public ConcurrentSkipListSet<FutureRebutal> getScheduledRebutals() {
        return scheduledRebutals;
    }

    /**
     * @return the gossip service of the View
     */
    public Service getService() {
        return service;
    }

    /**
     * @return the entire view - members both failed and live
     */
    public ConcurrentMap<Digest, Participant> getView() {
        return view;
    }

    public void registerRoundListener(Runnable callback) {
        roundListeners.add(callback);
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
            log.info("{} accusing {} on {}", node.getId(), member.getId(), ring);
            add(node.accuse(member, ring));
        }
    }

    /**
     * Add an inbound accusation to the view.
     * 
     * @param accusation
     */
    void add(AccusationWrapper accusation) {
        Participant accuser = view.get(accusation.getAccuser());
        Participant accused = view.get(accusation.getAccused());
        if (accuser == null || accused == null) {
            log.info("Accusation discarded, accused or accuser do not exist in view");
            return;
        }

        if (accusation.getRingNumber() > getParameters().rings) {
            log.debug("Invalid ring in accusation: {}", accusation.getRingNumber());
            return;
        }

        if (accused.getEpoch() != accusation.getEpoch()) {
            log.debug("Accusation discarded in epoch: {}  for: {} epoch: {}" + accusation.getEpoch(), accused.getId(),
                      accused.getEpoch());
            return;
        }

        if (!accused.getNote().getMask().get(accusation.getRingNumber())) {
            log.debug("Member {} accussed on disabled ring {} by {}", accused.getId(), accusation.getRingNumber(),
                      accuser.getId());
            return;
        }

        // verify the accusation after all other tests pass, as it's reasonably
        // expensive and we want to filter out all the noise first, before going to all
        // the trouble (and cost) to validate the sig
        if (!accuser.verify(accusation.getSignature(), AccusationWrapper.forSigning(accusation.getWrapped()))) {
            log.debug("Accusation signature invalid ");
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
            Participant currentAccuser = view.get(currentAccusation.getAccuser());

            if (!currentAccuser.equals(accuser) && ring.isBetween(currentAccuser, accuser, accused)) {
                accused.addAccusation(accusation);
                log.info("{} accused by {} on ring {} (replacing {})", accused, accuser, ring.getIndex(),
                         currentAccuser);
            }
        } else {
            Participant predecessor = ring.predecessor(accused, m -> (!m.isAccused()) || (m.equals(accuser)));
            if (accuser.equals(predecessor)) {
                accused.addAccusation(accusation);
                if (!accused.equals(node) && !pendingRebutals.containsKey(accused.getId()) && accused.isLive()) {
                    log.info("{} accused by {} on ring {} (timer started)", accused, accuser,
                             accusation.getRingNumber());
                    startRebutalTimer(accused);
                }
            } else {
                log.info("{} accused by {} on ring {} discarded as not predecessor {}", accused, accuser,
                         accusation.getRingNumber(), predecessor);
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
        Digest id = Member.getMemberIdentifier(cert.certificate);
        Participant member = view.get(id);
        if (member != null) {
            update(member, cert);
            return member;
        }
        member = new Participant(new MemberImpl(cert.certificate), cert.derEncoded, cert.certificateHash,
                getParameters());
        log.trace("Adding member via cert: {}", member.getId());
        return add(member);
    }

    /**
     * add an inbound note to the view
     * 
     * @param note
     */
    boolean add(NoteWrapper note) {
        log.trace("Adding note {} : {} ", note.getId(), note.getEpoch());
        Participant m = view.get(note.getId());
        if (m == null) {
            log.debug("No member for note: " + note.getId());
            return false;
        }

        if (m.getEpoch() >= note.getEpoch()) {
            log.trace("Note redundant in epoch: {} (<= {} ) for: {}, failed: {}", note.getEpoch(), m.getEpoch(),
                      note.getId(), m.isFailed());
            return false;
        }

        if (!isValidMask(note.getMask(), getParameters())) {
            log.debug("Note: {} mask invalid {}", note.getId(), note.getMask());
            return false;
        }

        // verify the note after all other tests pass, as it's reasonably expensive and
        // we want to filter out all
        // the noise first, before going to all the trouble to validate

        if (!m.verify(note.getSignature(), NoteWrapper.forSigning(note.getWrapped()))) {
            log.debug("Note signature invalid: {}", note.getId());
            return false;
        }

        log.trace("Adding member via note {} ", m);

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
        Participant previous = view.putIfAbsent(member.getId(), member);
        if (previous == null) {
            context.add(member);
            if (!member.isFailed()) {
                recover(member);
            }
            log.trace("Adding member: {}, recovering: {}", member.getId(), !member.isFailed());
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
        add(new CertWithHash(null, cert, null));
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
        Note seedNote = Note.newBuilder()
                            .setId(seed.getId().toByteString())
                            .setEpoch(-1)
                            .setMask(ByteString.copyFrom(Node.createInitialMask(getParameters().toleranceLevel,
                                                                                Utils.secureEntropy())
                                                             .toByteArray()))
                            .build();
        seed.setNote(new NoteWrapper(getDigestAlgorithm().digest(seedNote.toByteString()), seedNote));
        context.add(seed);
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
            certificate = (X509Certificate) cf.generateCertificate(new ByteArrayInputStream(
                    encoded.getContent().toByteArray()));
        } catch (CertificateException e) {
            log.warn("Invalid DER encoded certificate", e);
            return null;
        }
        try {
            getParameters().certificateValidator.validateClient(new X509Certificate[] { certificate });
        } catch (CertificateException e) {
            log.warn("Invalid cert: {}", certificate.getSubjectDN(), e);
            return null;
        }

        return new CertWithHash(digest(encoded.getHash()), certificate, encoded.getContent().toByteArray());
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
        int seed = Utils.secureEntropy().nextInt();
        return Digests.newBuilder()
                      .setAccusationBff(getAccusationsBff(seed, getParameters().falsePositiveRate).toBff()
                                                                                                  .toByteString())
                      .setNoteBff(getNotesBff(seed, getParameters().falsePositiveRate).toBff().toByteString())
                      .setCertificateBff(getCertificatesBff(seed, getParameters().falsePositiveRate).toBff()
                                                                                                    .toByteString())
                      .build();
    }

    void gc(Participant member) {
        context.offline(member);
        member.setFailed(true);
    }

    BloomFilter<Digest> getAccusationsBff(int seed, double p) {
        BloomFilter<Digest> bff = new BloomFilter.DigestBloomFilter(seed,
                getParameters().cardinality * getParameters().rings, p);
        context.getActive()
               .stream()
               .flatMap(m -> m.getAccusations())
               .filter(e -> e != null)
               .forEach(m -> bff.add(m.getHash()));
        return bff;
    }

    BloomFilter<Digest> getCertificatesBff(int seed, double p) {
        BloomFilter<Digest> bff = new BloomFilter.DigestBloomFilter(seed, getParameters().cardinality, p);
        view.values().stream().map(m -> m.getCertificateHash()).filter(e -> e != null).forEach(n -> bff.add(n));
        return bff;
    }

    DigestAlgorithm getDigestAlgorithm() {
        return getParameters().hashAlgorithm;
    }

    BloomFilter<Digest> getNotesBff(int seed, double p) {
        BloomFilter<Digest> bff = new BloomFilter.DigestBloomFilter(seed, getParameters().cardinality, p);
        view.values().stream().map(m -> m.getNote()).filter(e -> e != null).forEach(n -> bff.add(n.getHash()));
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
    void gossip(int ring, FfClient link, Runnable completion) {
        Participant member = link.getMember();
        NoteWrapper n = node.getNote();
        if (n == null) {
            link.release();
            completion.run();
            return;
        }
        Note signedNote = n.getWrapped();
        Digests outbound = commonDigests();
        ListenableFuture<Gossip> futureSailor = link.gossip(context.getId(), signedNote, ring, outbound);
        futureSailor.addListener(() -> {
            Gossip gossip;
            try {
                gossip = futureSailor.get();
            } catch (Throwable e) {
                log.debug("Exception gossiping with {}", link.getMember(), e);
                link.release();
                if (completion != null) {
                    completion.run();
                }
                return;
            }

            if (log.isTraceEnabled()) {
                log.trace("inbound: redirect: {} updates: certs: {}, notes: {}, accusations: {} ", gossip.getRedirect(),
                          gossip.getCertificates().getUpdatesCount(), gossip.getNotes().getUpdatesCount(),
                          gossip.getAccusations().getUpdatesCount());
            }
            if (gossip.getRedirect()) {
                link.release();
                redirect(member, gossip, ring);
                if (completion != null) {
                    completion.run();
                }
            } else {
                Update update = response(gossip);
                if (!isEmpty(update)) {
                    link.update(context.getId(), ring, update);
                }
                link.release();
                if (completion != null) {
                    completion.run();
                }
            }
        }, fjPool);
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
        Participant accuser = view.get(qa.getAccuser());
        Participant accused = view.get(qa.getAccused());
        if (ring.isBetween(accuser, q, accused)) {
            assert q.isAccused();
            assert q.isAccusedOn(ring.getIndex());
            q.invalidateAccusationOnRing(ring.getIndex());
            log.debug("Invalidating accusation on ring: {} for member: {}", ring.getIndex(), q.getId());
            if (!q.isAccused()) {
                if (q.isFailed()) {
                    recover(q);
                    log.debug("Member: {} recovered to accusation invalidated ring: {}", q.getId(), ring.getIndex());
                } else {
                    stopRebutalTimer(q);
                    log.debug("Member: {} rebuts (accusation invalidated) ring: {}", q.getId(), ring.getIndex());
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
    FfClient linkFor(Integer ring) {
        Participant successor = context.ring(ring).successor(node, m -> !m.isFailed());
        if (successor == null) {
            log.debug("No successor to node on ring: {} members: {}", ring, context.ring(ring).size());
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
        log.info("{} failing members {}", node.getId(),
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
    void monitor(FfClient link, int lastRing) {
        try {
            link.ping(context.getId(), 200);
            log.trace("Successful ping from {} to {}", node.getId(), link.getMember().getId());
        } catch (Exception e) {
            log.debug("Exception pinging {} : {} : {}", link.getMember().getId(), e.toString(),
                      e.getCause().getMessage());
            accuseOn(link.getMember(), lastRing);
        } finally {
            link.release();
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
                        log.error("unexpected error during monitor round", e);
                    }
                    maintainTimers();
                } finally {
                    if (timer != null) {
                        timer.stop();
                    }
                }

                roundListeners.forEach(l -> {
                    fjPool.execute(() -> {
                        try {
                            l.run();
                        } catch (Throwable e) {
                            log.error("error sending round() to listener: " + l, e);
                        }
                    });
                });
                scheduler.schedule(() -> fjPool.execute(() -> {
                    try {
                        oneRound(d, scheduler);
                    } catch (Throwable e) {
                        log.error("unexpected error during gossip round", e);
                    }
                }), d.toMillis(), TimeUnit.MILLISECONDS);
            });
        } catch (Throwable e) {
            log.error("unexpected error during gossip round", e);
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
    AccusationGossip processAccusationDigests(BloomFilter<Digest> bff, int seed, double p) {
        AccusationGossip.Builder builder = AccusationGossip.newBuilder();
        // Add all updates that this view has that aren't reflected in the inbound
        // bff
        context.getActive()
               .stream()
               .flatMap(m -> m.getAccusations())
               .filter(a -> !bff.contains(a.getHash()))
               .forEach(a -> builder.addUpdates(a.getWrapped()));
        builder.setBff(getAccusationsBff(seed, p).toBff().toByteString());
        AccusationGossip gossip = builder.build();
        log.trace("process accusations produded updates: {}", gossip.getUpdatesCount());
        return gossip;
    }

    CertificateGossip processCertificateDigests(Digest from, BloomFilter<Digest> bff, int seed, double p) {
        log.trace("process cert digests");
        CertificateGossip.Builder builder = CertificateGossip.newBuilder();
        // Add all updates that this view has that aren't reflected in the inbound
        // bff
        view.values()
            .stream()
            .filter(m -> m.getId().equals(from))
            .filter(m -> !bff.contains(m.getCertificateHash()))
            .map(m -> m.getEncodedCertificate())
            .filter(cert -> cert != null)
            .forEach(cert -> builder.addUpdates(cert));
        builder.setBff(getCertificatesBff(seed, p).toBff().toByteString());
        CertificateGossip gossip = builder.build();
        log.trace("process certificates produced updates: {}", gossip.getUpdatesCount());
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
    NoteGossip processNoteDigests(Digest from, BloomFilter<Digest> bff, int seed, double p) {
        NoteGossip.Builder builder = NoteGossip.newBuilder();

        // Add all updates that this view has that aren't reflected in the inbound
        // bff
        context.getActive()
               .stream()
               .filter(m -> m.getNote() != null)
               .filter(m -> !bff.contains(m.getNote().getHash()))
               .map(m -> m.getNote())
               .forEach(n -> builder.addUpdates(n.getWrapped()));
        builder.setBff(getNotesBff(seed, p).toBff().toByteString());
        NoteGossip gossip = builder.build();
        log.trace("process notes produded updates: {}", gossip.getUpdatesCount());
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
     * @param noteUpdates
     * @param accusationUpdates
     */
    void processUpdates(List<EncodedCertificate> certificatUpdates, List<Note> noteUpdates,
                        List<Accusation> accusationUpdates) {
        certificatUpdates.stream()
                         .map(cert -> certificateFrom(cert))
                         .filter(cert -> cert != null)
                         .forEach(cert -> add(cert));
        noteUpdates.stream()
                   .map(s -> new NoteWrapper(getDigestAlgorithm().digest(s.toByteString()), s))
                   .forEach(note -> add(note));
        accusationUpdates.stream()
                         .map(s -> new AccusationWrapper(getDigestAlgorithm().digest(s.toByteString()), s))
                         .forEach(accusation -> add(accusation));

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
        if (context.isOffline(member)) {
            context.activate(member);
            member.setFailed(false);
            log.info("Recovering: {}", member.getId());
        } else {
            log.trace("Already active: {}", member.getId());
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
            log.debug("Cannot redirect from {} to {} on ring {} as note is null", node, successor, ring);
            return Gossip.getDefaultInstance();
        }

        log.debug("Redirecting from {} to {} on ring {}", node, successor, ring);

        return Gossip.newBuilder()
                     .setRedirect(true)
                     .setCertificates(CertificateGossip.newBuilder()
                                                       .addUpdates(successor.getEncodedCertificate())
                                                       .build())
                     .setNotes(NoteGossip.newBuilder().addUpdates(successor.getNote().getWrapped()).build())
                     .setAccusations(AccusationGossip.newBuilder()
                                                     .addAllUpdates(member.getEncodedAccusations(getParameters().rings)))
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
            FutureRebutal future = new FutureRebutal(round.get() + 2 * (diameter * getParameters().toleranceLevel), m);
            scheduledRebutals.add(future);
            return future;
        });
    }

    void stopRebutalTimer(Participant m) {
        m.clearAccusations();
        log.info("New note, epoch {}, clearing accusations on {}", m.getEpoch(), m.getId());
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
        view.values()
            .stream()
            .filter(m -> !certBff.contains((m.getCertificateHash())))
            .map(m -> m.getEncodedCertificate())
            .filter(ec -> ec != null)
            .forEach(cert -> builder.addCertificates(cert));

        // notes
        BloomFilter<Digest> notesBff = BloomFilter.from(gossip.getNotes().getBff());
        view.values()
            .stream()
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

    private FfClient linkFor(Participant m) {
        try {
            return comm.apply(m, node);
        } catch (Throwable e) {
            log.debug("error opening connection to {}: {}", m.getId(),
                      (e.getCause() != null ? e.getCause() : e).getMessage());
        }
        return null;
    }

    private void redirect(Participant member, Gossip gossip, int ring) {
        if (gossip.getCertificates().getUpdatesCount() != 1 && gossip.getNotes().getUpdatesCount() != 1) {
            log.warn("Redirect response from {} on ring {} did not contain redirect member certificate and note",
                     member, ring);
            return;
        }
        if (gossip.getAccusations().getUpdatesCount() > 0) {
            // Reset our epoch to whatever the group has recorded for this recovering node
            long max = gossip.getAccusations()
                             .getUpdatesList()
                             .stream()
                             .map(signed -> new AccusationWrapper(getDigestAlgorithm().digest(signed.toByteString()),
                                     signed))
                             .mapToLong(a -> a.getEpoch())
                             .max()
                             .orElse(-1);
            node.nextNote(max + 1);
        }
        CertWithHash certificate = certificateFrom(gossip.getCertificates().getUpdates(0));
        if (certificate != null) {
            add(certificate);
            Note signed = gossip.getNotes().getUpdates(0);
            NoteWrapper note = new NoteWrapper(getDigestAlgorithm().digest(signed.toByteString()), signed);
            add(note);
            gossip.getAccusations()
                  .getUpdatesList()
                  .forEach(s -> add(new AccusationWrapper(getDigestAlgorithm().digest(s.toByteString()), s)));
            log.debug("Redirected from {} to {} on ring {}", member, note.getId(), ring);
        } else {
            log.warn("Redirect certificate from {} on ring {} is null", member, ring);
        }
    }
}
