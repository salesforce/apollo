/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import static com.salesforce.apollo.fireflies.communications.FfClientCommunications.getCreate;

import java.io.ByteArrayInputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.proto.AccusationGossip;
import com.salesfoce.apollo.proto.AccusationGossip.Builder;
import com.salesfoce.apollo.proto.CertificateGossip;
import com.salesfoce.apollo.proto.Digests;
import com.salesfoce.apollo.proto.EncodedCertificate;
import com.salesfoce.apollo.proto.Gossip;
import com.salesfoce.apollo.proto.Message;
import com.salesfoce.apollo.proto.NoteGossip;
import com.salesfoce.apollo.proto.Signed;
import com.salesfoce.apollo.proto.Update;
import com.salesforce.apollo.comm.CommonCommunications;
import com.salesforce.apollo.comm.Communications;
import com.salesforce.apollo.comm.EndpointProvider;
import com.salesforce.apollo.comm.StandardEpProvider;
import com.salesforce.apollo.fireflies.View.MessageChannelHandler.Msg;
import com.salesforce.apollo.fireflies.communications.FfClientCommunications;
import com.salesforce.apollo.fireflies.communications.FfServerCommunications;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.Ring;
import com.salesforce.apollo.protocols.BloomFilter;
import com.salesforce.apollo.protocols.CaValidator;
import com.salesforce.apollo.protocols.HashFunction;
import com.salesforce.apollo.protocols.HashKey;

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
        public final HashKey id;
        public final int     ring;

        public AccTag(HashKey id, int ring) {
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
        public final byte[]          certificateHash;
        public final byte[]          derEncoded;

        public CertWithHash(byte[] certificateHash, X509Certificate certificate, byte[] derEncoded) {
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

    public interface MembershipListener {

        /**
         * A member has failed
         * 
         * @param member
         */
        void fail(Participant member);

        /**
         * A new member has recovered and is now live
         * 
         * @param member
         */
        void recover(Participant member);
    }

    @FunctionalInterface
    public interface MessageChannelHandler {
        class Msg {
            public final int         channel;
            public final byte[]      content;
            public final Participant from;

            public Msg(Participant from, int channel, byte[] content) {
                this.channel = channel;
                this.from = from;
                this.content = content;
            }
        }

        /**
         * Broadcast messages accepted on a channel
         * 
         * @param messages
         */
        void message(List<Msg> messages);
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
         */
        public void gossip() {
            FfClientCommunications link = nextRing();

            if (link == null) {
                log.debug("No members to gossip with on ring: {}", lastRing);
                return;
            }
            log.trace("gossiping with {} on {}", link.getMember(), lastRing);
            boolean success;
            try {
                success = View.this.gossip(lastRing, link);
            } catch (Exception e) {
                log.debug("Partial round of gossip with {}, ring {}", link.getMember(), lastRing, e);
                return;
            } finally {
                link.release();
            }
            if (!success) {
                try {
                    link = linkFor(lastRing);
                    if (link == null) {
                        log.debug("No link for ring {}", lastRing);
                        return;
                    }
                    success = View.this.gossip(lastRing, link);
                } catch (Exception e) {
                    log.debug("Partial round of gossip with {}, ring {}", link.getMember(), lastRing);
                    return;
                } finally {
                    link.release();
                }
                if (!success) {
                    log.trace("Partial redirect round of gossip with {}, ring {} not redirecting further",
                              link.getMember(), lastRing);
                } else {
                    log.trace("Successful redirect round of gossip with {}, ring {}", link.getMember(), lastRing);
                }
            }

            dispatcher.execute(() -> roundListeners.parallelStream().forEach(l -> {
                try {
                    l.run();
                } catch (Throwable e) {
                    log.error("error sending round() to listener: " + l, e);
                }
            }));
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
            int ring = lastRing;
            if (ring < 0) {
                return;
            }
            Participant successor = context.ring(ring).successor(node, m -> !m.isFailed() && !m.isAccused());
            if (successor == null) {
                log.info("No successor to node on ring: {}", ring);
                return;
            }

            FfClientCommunications link = linkFor(successor);
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
        public Gossip rumors(int ring, Digests digests, HashKey from, X509Certificate certificate, Signed note) {
            if (ring >= context.getRings().length || ring < 0) {
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

            add(new Note(note.getContent().toByteArray(), note.getSignature().toByteArray()));

            Participant successor = getRing(ring).successor(member, m -> !m.isFailed());
            if (successor == null) {
                return emptyGossip();
            }
            if (!successor.equals(node)) {
                redirectTo(member, ring, successor);
            }
            int seed = getParameters().entropy.nextInt();
            return Gossip.newBuilder()
                         .setRedirect(false)
                         .setMessages(messageBuffer.process(new BloomFilter(digests.getMessageBff()), seed,
                                                            getParameters().falsePositiveRate))
                         .setCertificates(processCertificateDigests(from, new BloomFilter(digests.getCertificateBff()),
                                                                    seed, getParameters().falsePositiveRate))
                         .setNotes(processNoteDigests(from, new BloomFilter(digests.getNoteBff()), seed,
                                                      getParameters().falsePositiveRate))
                         .setAccusations(processAccusationDigests(new BloomFilter(digests.getAccusationBff()), seed,
                                                                  getParameters().falsePositiveRate))
                         .build();
        }

        public void start(Duration d, List<X509Certificate> seeds) {
            if (!started.compareAndSet(false, true)) {
                return;
            }

            node.setFailed(false);
            node.nextNote();
            recover(node);
            List<HashKey> seedList = new ArrayList<>();
            seeds.stream()
                 .map(cert -> new Participant(cert, getParameters()))
                 .peek(m -> seedList.add(m.getId()))
                 .forEach(m -> addSeed(m));

            long interval = d.toMillis();
            int initialDelay = getParameters().entropy.nextInt((int) interval * 2);
            futureGossip = scheduler.scheduleWithFixedDelay(() -> {
                try {
                    oneRound();
                } catch (Throwable e) {
                    log.error("unexpected error during gossip round", e);
                }
            }, initialDelay, interval, TimeUnit.MILLISECONDS);
            log.info("{} started, initial delay: {} ms", node.getId(), initialDelay);
        }

        /**
         * stop the view from performing gossip and monitoring rounds
         */
        public void stop() {
            if (!started.compareAndSet(true, false)) {
                return;
            }
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
            messageBuffer.clear();
        }

        /**
         * The third and final message in the anti-entropy protocol. Process the inbound
         * update from another member.
         * 
         * @param ring
         * @param update
         * @param from
         */
        public void update(int ring, Update update, HashKey from) {
            processUpdates(update.getCertificatesList(), update.getNotesList(), update.getAccusationsList(),
                           update.getMessagesList());

        }

        /**
         * @return the next ClientCommunications in the next ring
         */
        FfClientCommunications nextRing() {
            FfClientCommunications link = null;
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

    public static int diameter(FirefliesParameters parameters) {
        double pN = ((double) (2 * parameters.toleranceLevel)) / ((double) parameters.cardinality);
        double logN = Math.log(parameters.cardinality);
        return (int) (logN / Math.log(parameters.cardinality * pN));
    }

    public static Gossip emptyGossip() {
        return Gossip.getDefaultInstance();
    }

    public static EndpointProvider getStandardEpProvider(Node member) {

        X509Certificate certificate = member.getCertificate();
        return new StandardEpProvider(Member.portsFrom(certificate), certificate, member.privateKey, ClientAuth.REQUIRE,
                new CaValidator(member.getCA()));
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
     * Message channel handlers
     */
    private final Map<Integer, MessageChannelHandler> channelHandlers = new ConcurrentHashMap<>();

    /**
     * Communications with other members
     */
    private final CommonCommunications<FfClientCommunications> comm;

    /**
     * View context
     */
    private final Context<Participant> context;

    /**
     * The analytical diameter of the graph of members
     */
    private final int diameter;

    /**
     * Single threaded event dispatcher
     */
    private final ExecutorService dispatcher;

    /**
     * Membership listeners
     */
    private final List<MembershipListener> membershipListeners = new CopyOnWriteArrayList<>();

    /**
     * Buffered store of broadcast messages gossiped between members
     */
    private final MessageBuffer messageBuffer;

    @SuppressWarnings("unused")
    private final FireflyMetrics metrics;

    /**
     * This member
     */
    private final Node node;

    /**
     * Pending rebutal timers by member id
     */
    private final ConcurrentMap<HashKey, FutureRebutal> pendingRebutals = new ConcurrentHashMap<>();

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
     * Scheduler for the view
     */
    private final ScheduledExecutorService scheduler;

    /**
     * The gossip service
     */
    private final Service service = new Service();

    /**
     * The view of all known members
     */
    private final ConcurrentMap<HashKey, Participant> view = new ConcurrentHashMap<>();

    public View(Node node, Communications communications, ScheduledExecutorService scheduler) {
        this(node, communications, scheduler, null);
    }

    public View(Node node, Communications communications, ScheduledExecutorService scheduler, FireflyMetrics metrics) {
        this.metrics = metrics;
        this.node = node;
        this.comm = communications.create(node, getCreate(metrics), new FfServerCommunications(service,
                communications.getClientIdentityProvider(), metrics));
        this.scheduler = scheduler;
        diameter = diameter(getParameters());
        assert diameter > 0 : "Diameter must be greater than zero: " + diameter;
        dispatcher = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread daemon = new Thread(r, "Event dispatcher " + node);
                daemon.setDaemon(true);
                return daemon;
            }
        });
        this.messageBuffer = new MessageBuffer(getParameters().bufferSize,
                getParameters().toleranceLevel * diameter + 1);
        context = new Context<>(HashKey.ORIGIN, getParameters().rings);
        add(node);
        log.info("View [{}]\n  Parameters: {}", node.getId(), getParameters());
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
    public ConcurrentMap<HashKey, Participant> getView() {
        return view;
    }

    /**
     * Publish a message to all members
     * 
     * @param message
     */
    public void publish(int channel, byte[] message) {
        messageBuffer.put(System.currentTimeMillis(), message, node, channel);
    }

    public void register(int channel, MessageChannelHandler listener) {
        channelHandlers.put(channel, listener);
    }

    public void register(MembershipListener listener) {
        membershipListeners.add(listener);
    }

    public void registerRoundListener(Runnable callback) {
        roundListeners.add(callback);
    }

    public Collection<? extends Member> sample(int range, SecureRandom entropy, Member excluded) {
        return context.sample(range, entropy, excluded);
    }

    /**
     * Accuse the member on the list of rings. If the member has disabled the ring
     * in its mask, do not issue the accusation
     * 
     * @param member
     * @param ring
     */
    void accuseOn(Participant member, int ring) {
        Note note = member.getNote();
        if (note != null && note.getMask().get(ring)) {
            log.info("{} accusing {} on {}", node.getId(), member.getId(), ring);
            add(node.accuse(member, ring));
        }
    }

    /**
     * Add an inbound accusation to the view.
     * 
     * @param accusation
     */
    void add(Accusation accusation) {
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
        if (!accusation.verify(accuser.forVerification(getParameters().signatureAlgorithm))) {
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
    void add(Accusation accusation, Participant accuser, Participant accused) {
        Ring<Participant> ring = context.ring(accusation.getRingNumber());

        if (accused.isAccusedOn(ring.getIndex())) {
            Accusation currentAccusation = accused.getAccusation(ring.getIndex());
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
        HashKey id = Member.getMemberId(cert.certificate);
        Participant member = view.get(id);
        if (member != null) {
            update(member, cert);
            return member;
        }
        member = new Participant(cert.certificate, cert.derEncoded, getParameters(), cert.certificateHash);
        log.trace("Adding member via cert: {}", member.getId());
        return add(member);
    }

    /**
     * add an inbound note to the view
     * 
     * @param note
     */
    boolean add(Note note) {
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

        BitSet mask = note.getMask();
        if (!isValidMask(mask, getParameters())) {
            log.debug("Note: {} mask invalid {}", note.getId(), mask);
            return false;
        }

        // verify the note after all other tests pass, as it's reasonably expensive and
        // we want to filter out all
        // the noise first, before going to all the trouble to validate
        if (!note.verify(m.forVerification(getParameters().signatureAlgorithm))) {
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
        seed.setNote(new Note(seed.getId(), -1,
                Node.createInitialMask(getParameters().toleranceLevel, getParameters().entropy), node.forSigning()));
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
            certificate.verify(getParameters().ca.getPublicKey());
        } catch (NoSuchAlgorithmException | InvalidKeyException | SignatureException | CertificateException
                | NoSuchProviderException e) {
            log.warn("Invalid cert: {}", certificate.getSubjectDN(), e);
            return null;
        }

        return new CertWithHash(encoded.getHash().toByteArray(), certificate, encoded.getContent().toByteArray());
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
        int seed = getParameters().entropy.nextInt();
        return Digests.newBuilder()
                      .setAccusationBff(getAccusationsBff(seed, getParameters().falsePositiveRate).toBff())
                      .setNoteBff(getNotesBff(seed, getParameters().falsePositiveRate).toBff())
                      .setMessageBff(getMessagesBff(seed, getParameters().falsePositiveRate).toBff())
                      .setCertificateBff(getCertificatesBff(seed, getParameters().falsePositiveRate).toBff())
                      .build();
    }

    void gc(Participant member) {
        context.offline(member);
        member.setFailed(true);
        dispatcher.execute(() -> membershipListeners.parallelStream().forEach(l -> {
            try {
                l.fail(member);
            } catch (Throwable e) {
                log.error("error sending fail to listener: " + l, e);
            }
        }));
    }

    BloomFilter getAccusationsBff(int seed, double p) {
        BloomFilter bff = new BloomFilter(
                new HashFunction(seed, getParameters().cardinality * getParameters().rings, p));
        context.getActive()
               .stream()
               .flatMap(m -> m.getAccusations())
               .filter(e -> e != null)
               .forEach(n -> bff.add(new HashKey(n.hash())));
        return bff;
    }

    BloomFilter getCertificatesBff(int seed, double p) {
        BloomFilter bff = new BloomFilter(new HashFunction(seed, getParameters().cardinality, p));
        view.values()
            .stream()
            .map(m -> m.getCertificateHash())
            .filter(e -> e != null)
            .forEach(n -> bff.add(new HashKey(n)));
        return bff;
    }

    BloomFilter getMessagesBff(int seed, double p) {
        return messageBuffer.getBff(seed, p);
    }

    BloomFilter getNotesBff(int seed, double p) {
        BloomFilter bff = new BloomFilter(new HashFunction(seed, getParameters().cardinality, p));
        view.values()
            .stream()
            .map(m -> m.getNote())
            .filter(e -> e != null)
            .forEach(n -> bff.add(new HashKey(n.hash())));
        return bff;
    }

    /**
     * for testing
     * 
     * @return
     */
    Map<HashKey, FutureRebutal> getPendingRebutals() {
        return pendingRebutals;
    }

    /**
     * Gossip with the member
     * 
     * @param ring - the index of the gossip ring the gossip is originating from in
     *             this view
     * @param link - the outbound communications to the paired member
     * @throws Exception
     */
    boolean gossip(int ring, FfClientCommunications link) throws Exception {
        Signed signedNote = node.getSignedNote();
        if (signedNote == null) {
            return true;
        }
        Digests outbound = commonDigests();
        Gossip gossip = link.gossip(signedNote, ring, outbound);
        if (log.isTraceEnabled()) {
            log.trace("inbound: redirect: {} updates: certs: {}, notes: {}, accusations: {}, messages: {}",
                      gossip.getRedirect(), gossip.getCertificates().getUpdatesCount(),
                      gossip.getNotes().getUpdatesCount(), gossip.getAccusations().getUpdatesCount(),
                      gossip.getMessages().getUpdatesCount());
        }
        if (gossip.getRedirect()) {
            if (gossip.getCertificates().getUpdatesCount() != 1 && gossip.getNotes().getUpdatesCount() != 1) {
                log.warn("Redirect response from {} on ring {} did not contain redirect member certificate and note",
                         link.getMember(), ring);
                return false;
            }
            if (gossip.getAccusations().getUpdatesCount() > 0) {
                // Reset our epoch to whatever the group has recorded for this recovering node
                long max = gossip.getAccusations()
                                 .getUpdatesList()
                                 .stream()
                                 .map(signed -> new Accusation(signed.getContent().toByteArray(),
                                         signed.getSignature().toByteArray()))
                                 .mapToLong(a -> a.getEpoch())
                                 .max()
                                 .orElse(-1);
                node.nextNote(max + 1);
            }
            CertWithHash certificate = certificateFrom(gossip.getCertificates().getUpdates(0));
            if (certificate != null) {
                link.release();
                add(certificate);
                Signed signed = gossip.getNotes().getUpdates(0);
                Note note = new Note(signed.getContent().toByteArray(), signed.getSignature().toByteArray());
                add(note);
                gossip.getAccusations()
                      .getUpdatesList()
                      .forEach(s -> add(new Accusation(s.getContent().toByteArray(), s.getSignature().toByteArray())));
                log.debug("Redirected from {} to {} on ring {}", link.getMember(), note.getId(), ring);
                return false;
            } else {
                log.warn("Redirect certificate from {} on ring {} is null", link.getMember(), ring);
                return false;
            }
        }
        Update update = response(gossip);
        if (!isEmpty(update)) {
            link.update(ring, update);
        }
        return true;
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
        Accusation qa = q.getAccusation(ring.getIndex());
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
        return update.getAccusationsCount() == 0 && update.getCertificatesCount() == 0 && update.getMessagesCount() == 0
                && update.getNotesCount() == 0;
    }

    /**
     * @param ring - the ring to gossip on
     * @return the communication link for this ring, based on current membership
     *         state
     */
    FfClientCommunications linkFor(Integer ring) {
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
    void monitor(FfClientCommunications link, int lastRing) {
        try {
            link.ping(200);
            log.trace("Successful ping from {} to {}", node.getId(), link.getMember().getId());
        } catch (Exception e) {
            log.error("Exception pinging {} : {} : {}", link.getMember().getId(), e.toString(),
                      e.getCause().getMessage());
            accuseOn(link.getMember(), lastRing);
        } finally {
            link.release();
        }
    }

    /**
     * Drive one round of the View. This involves a round of gossip() and a round of
     * monitor().
     */
    void oneRound() {
        com.codahale.metrics.Timer.Context timer = null;
        if (metrics != null) {
            timer = metrics.gossipRoundDuration().time();
        }
        try {
            round.incrementAndGet();
            try {
                service.gossip();
            } catch (Throwable e) {
                log.error("unexpected error during gossip round", e);
            }
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
    AccusationGossip processAccusationDigests(BloomFilter bff, int seed, double p) {
        Builder builder = AccusationGossip.newBuilder();
        // Add all updates that this view has that aren't reflected in the inbound
        // bff
        context.getActive()
               .stream()
               .flatMap(m -> m.getAccusations())
               .filter(a -> !bff.contains(new HashKey(a.hash())))
               .forEach(a -> builder.addUpdates(a.getSigned()));
        builder.setBff(getAccusationsBff(seed, p).toBff());
        AccusationGossip gossip = builder.build();
        log.trace("process accusations produded updates: {}", gossip.getUpdatesCount());
        return gossip;
    }

    CertificateGossip processCertificateDigests(HashKey from, BloomFilter bff, int seed, double p) {
        log.trace("process cert digests");
        com.salesfoce.apollo.proto.CertificateGossip.Builder builder = CertificateGossip.newBuilder();
        // Add all updates that this view has that aren't reflected in the inbound
        // bff
        view.values()
            .stream()
            .filter(m -> m.getId().equals(from))
            .filter(m -> !bff.contains(new HashKey(m.getCertificateHash())))
            .map(m -> m.getEncodedCertificate())
            .filter(cert -> cert != null)
            .forEach(cert -> builder.addUpdates(cert));
        builder.setBff(getCertificatesBff(seed, p).toBff());
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
    NoteGossip processNoteDigests(HashKey from, BloomFilter bff, int seed, double p) {
        com.salesfoce.apollo.proto.NoteGossip.Builder builder = NoteGossip.newBuilder();

        // Add all updates that this view has that aren't reflected in the inbound
        // bff
        context.getActive()
               .stream()
               .filter(m -> m.getNote() != null)
               .filter(m -> !bff.contains(new HashKey(m.getNote().hash())))
               .map(m -> m.getSignedNote())
               .forEach(n -> builder.addUpdates(n));
        builder.setBff(getNotesBff(seed, p).toBff());
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
                       gossip.getAccusations().getUpdatesList(), gossip.getMessages().getUpdatesList());
    }

    /**
     * Process the updates of the supplied juicy gossip. This is the Jesus Nut of
     * state change driving the view.
     * 
     * @param certificatUpdates
     * @param noteUpdates
     * @param accusationUpdates
     * @param messageUpdates
     */
    void processUpdates(List<EncodedCertificate> certificatUpdates, List<Signed> noteUpdates,
                        List<Signed> accusationUpdates, List<Message> messageUpdates) {
        certificatUpdates.stream()
                         .map(cert -> certificateFrom(cert))
                         .filter(cert -> cert != null)
                         .forEach(cert -> add(cert));
        noteUpdates.stream()
                   .map(s -> new Note(s.getContent().toByteArray(), s.getSignature().toByteArray()))
                   .forEach(note -> add(note));
        accusationUpdates.stream()
                         .map(s -> new Accusation(s.getContent().toByteArray(), s.getSignature().toByteArray()))
                         .forEach(accusation -> add(accusation));

        if (node.isAccused()) {
            // Rebut the accusations by creating a new note and clearing the accusations
            node.nextNote();
            node.clearAccusations();
        }

        Map<Integer, List<Msg>> newMessages = new HashMap<>();

        messageBuffer.merge(messageUpdates, message -> validate(message)).stream().map(m -> {
            HashKey id = new HashKey(m.getSource());
            Participant from = view.get(id);
            if (from == null) {
                log.trace("{} message from unknown member: {}", node, id);
                return null;
            } else {
                return new Msg(from, m.getChannel(), m.getContent().toByteArray());
            }
        }).filter(m -> m != null).forEach(msg -> {
            newMessages.computeIfAbsent(msg.channel, i -> new ArrayList<>()).add(msg);
        });
        newMessages.entrySet().forEach(e -> dispatcher.execute(() -> {
            MessageChannelHandler handler = channelHandlers.get(e.getKey());
            if (handler != null) {
                handler.message(e.getValue());
            }
        }));
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
            dispatcher.execute(() -> membershipListeners.parallelStream().forEach(l -> {
                try {
                    l.recover(member);
                } catch (Throwable e) {
                    log.error("error recoving member in listener: " + l, e);
                }
            }));
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
                     .setNotes(NoteGossip.newBuilder().addUpdates(successor.getSignedNote()).build())
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
        com.salesfoce.apollo.proto.Update.Builder builder = Update.newBuilder();

        // messages
        builder.addAllMessages(messageBuffer.updatesFor(new BloomFilter(gossip.getMessages().getBff())));

        // certificates
        BloomFilter certBff = new BloomFilter(gossip.getCertificates().getBff());
        view.values()
            .stream()
            .filter(m -> !certBff.contains(new HashKey(m.getCertificateHash())))
            .map(m -> m.getEncodedCertificate())
            .filter(ec -> ec != null)
            .forEach(cert -> builder.addCertificates(cert));

        // notes
        BloomFilter notesBff = new BloomFilter(gossip.getNotes().getBff());
        view.values()
            .stream()
            .filter(m -> m.getNote() != null)
            .filter(m -> !notesBff.contains(new HashKey(m.getNote().hash())))
            .map(m -> m.getSignedNote())
            .forEach(n -> builder.addNotes(n));

        BloomFilter accBff = new BloomFilter(gossip.getAccusations().getBff());
        context.getActive()
               .stream()
               .flatMap(m -> m.getAccusations())
               .filter(a -> !accBff.contains(new HashKey(a.hash())))
               .forEach(a -> builder.addAccusations(a.getSigned()));

        return builder.build();
    }

    /**
     * validate the message's signature
     * 
     * @param message
     * @return true if the message is valid, false if from an unknown member or
     *         signature doesn't validate
     */
    boolean validate(Message message) {
        HashKey from = new HashKey(message.getSource());
        Participant member = view.get(from);
        if (member == null) {
            return false;
        }
        Signature signature = member.forVerification(getParameters().signatureAlgorithm);
        try {
            signature.update(message.getContent().toByteArray());
            return signature.verify(message.getSignature().toByteArray());
        } catch (SignatureException e) {
            log.debug("invalid signature for message {}", new HashKey(message.getId()), from);
            return false;
        }
    }

    private FfClientCommunications linkFor(Participant m) {
        try {
            return comm.apply(m, node);
        } catch (Throwable e) {
            log.debug("error opening connection to {}: {}", m.getId(),
                      (e.getCause() != null ? e.getCause() : e).getMessage());
        }
        return null;
    }
}
