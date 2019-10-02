/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import static com.salesforce.apollo.fireflies.Member.getMemberId;

import java.io.ByteArrayInputStream;
import java.nio.channels.ClosedChannelException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.avro.AvroRemoteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.salesforce.apollo.avro.AccusationDigest;
import com.salesforce.apollo.avro.AccusationGossip;
import com.salesforce.apollo.avro.CertificateDigest;
import com.salesforce.apollo.avro.CertificateGossip;
import com.salesforce.apollo.avro.Digests;
import com.salesforce.apollo.avro.EncodedCertificate;
import com.salesforce.apollo.avro.Gossip;
import com.salesforce.apollo.avro.Message;
import com.salesforce.apollo.avro.MessageDigest;
import com.salesforce.apollo.avro.MessageGossip;
import com.salesforce.apollo.avro.NoteDigest;
import com.salesforce.apollo.avro.NoteGossip;
import com.salesforce.apollo.avro.Signed;
import com.salesforce.apollo.avro.Update;
import com.salesforce.apollo.avro.Uuid;
import com.salesforce.apollo.fireflies.View.MessageChannelHandler.Msg;
import com.salesforce.apollo.fireflies.communications.FfClientCommunications;
import com.salesforce.apollo.fireflies.communications.FirefliesCommunications;
import com.salesforce.apollo.protocols.Conversion;

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
		public final UUID id;
		public final int ring;

		public AccTag(UUID id, int ring) {
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
		public final byte[] certificateHash;
		public final byte[] derEncoded;

		public CertWithHash(byte[] certificateHash, X509Certificate certificate, byte[] derEncoded) {
			this.certificate = certificate;
			this.derEncoded = derEncoded;
			this.certificateHash = certificateHash;
		}
	}

	public interface MembershipListener {

		/**
		 * A member has failed
		 * 
		 * @param member
		 */
		void fail(Member member);

		/**
		 * A new member has recovered and is now live
		 * 
		 * @param member
		 */
		void recover(Member member);
	}

	@FunctionalInterface
	public interface MessageChannelHandler {
		class Msg {
			public final int channel;
			public final byte[] content;
			public final Member from;

			public Msg(Member from, int channel, byte[] content) {
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
		 * The interval between each round of the view
		 */
		private volatile Duration timeoutInterval;

		/**
		 * @return The Duration to wait before a failed member is garbage collected from
		 *         the view.
		 */
		public Duration getTimeoutInterval() {
			return timeoutInterval;
		}

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
			} catch (AvroRemoteException e) {
				connections.invalidate(link.getMember());
				log.debug("Partial round of gossip with {}, ring {}", link.getMember(), lastRing, e);
				return;
			}
			if (!success) {
				try {
					link = linkFor(lastRing);
					if (link == null) {
						log.debug("No link for ring {}", lastRing);
						return;
					}
					success = View.this.gossip(lastRing, link);
				} catch (AvroRemoteException e) {
					connections.invalidate(link.getMember());
					log.debug("Partial round of gossip with {}, ring {}", link.getMember(), lastRing);
					return;
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
			if (lastRing < 0) {
				return;
			}
			FfClientCommunications link = linkFor(lastRing);
			if (link != null) {
				View.this.monitor(link, lastRing);
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
		public Gossip rumors(int ring, Digests digests, UUID from, X509Certificate certificate, Signed note) {
			if (ring >= rings.size() || ring < 0) {
				log.info("invalid ring {} from {}", ring, from);
				return emptyGossip();
			}

			Member member = view.get(from);
			if (member == null) {
				add(certificate);
				member = view.get(from);
				if (member == null) {
					log.info("invalid credentials on ring {} from {}", ring, from);
					// invalid creds
					return emptyGossip();
				}
			}

			add(new Note(note.getContent().array(), note.getSignature().array()));

			Member successor = getRing(ring).successor(member, m -> !m.isFailed());
			return successor == null || !successor.equals(node) ? redirectTo(member, ring, successor)
					: new Gossip(false, messageBuffer.process(digests.getMessages()),
							processCertificateDigests(from, digests.getCertificates()),
							processNoteDigests(from, digests.getNotes()),
							processAccusationDigests(digests.getAccusations()));
		}

		public void start(Duration d) {
			if (!started.compareAndSet(false, true)) {
				return;
			}
			startServer();
			calculateTimeoutInterval(d);
			long interval = d.toMillis();
			futureGossip = scheduler.scheduleWithFixedDelay(() -> {
				try {
					oneRound();
				} catch (Throwable e) {
					log.error("unexpected error during gossip round", e);
				}
			}, parameters.entropy.nextInt((int) interval * 2), interval, TimeUnit.MILLISECONDS);
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
			comm.close();
		}

		/**
		 * The third and final message in the anti-entropy protocol. Process the inbound
		 * update from another member.
		 * 
		 * @param ring
		 * @param update
		 * @param from
		 */
		public void update(int ring, Update update, UUID from) {
			assert from != null;
			processUpdates(update.getCertificates(), update.getNotes(), update.getAccusations(), update.getMessages());

		}

		void calculateTimeoutInterval(Duration interval) {
			timeoutInterval = interval.multipliedBy(parameters.toleranceLevel * diameter + 1);
		}

		/**
		 * @return the next ClientCommunications in the next ring
		 */
		FfClientCommunications nextRing() {
			FfClientCommunications link = null;
			int last = lastRing;
			int current = (last + 1) % parameters.rings;
			for (int i = 0; i < parameters.rings; i++) {
				link = linkFor(current);
				if (link != null) {
					break;
				}
				current = (current + 1) % parameters.rings;
			}
			lastRing = current;
			return link;
		}

		void startServer() {
			comm.start();
		}
	}

	private static final CertificateFactory cf;
	private static final Duration DEFAULT_INTERVAL = Duration.ofMillis(3 * 1000);
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
		return new Gossip(true, new MessageGossip(Collections.emptyList(), Collections.emptyList()),
				new CertificateGossip(Collections.emptyList(), Collections.emptyList()),
				new NoteGossip(Collections.emptyList(), Collections.emptyList()),
				new AccusationGossip(Collections.emptyList(), Collections.emptyList()));
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

	public static UUID uuid(Uuid bits) {
		return Conversion.uuid(bits);
	}

	/**
	 * Message channel handlers
	 */
	private final Map<Integer, MessageChannelHandler> channelHandlers = new ConcurrentHashMap<>();

	/**
	 * Communications with other members
	 */
	private final FirefliesCommunications comm;

	/**
	 * The mapped set of open outbound connections with other members, mapped by
	 * member
	 */
	private final LoadingCache<Member, FfClientCommunications> connections;

	/**
	 * The analytical diameter of the graph of members
	 */
	private final int diameter;

	/**
	 * Single threaded event dispatcher
	 */
	private final ExecutorService dispatcher;

	/**
	 * The set of failed members
	 */
	private final ConcurrentMap<UUID, Member> failed = new ConcurrentHashMap<>();

	/**
	 * The set of live members
	 */
	private final ConcurrentMap<UUID, Member> live = new ConcurrentHashMap<>();

	/**
	 * Membership listeners
	 */
	private final List<MembershipListener> membershipListeners = new CopyOnWriteArrayList<>();

	/**
	 * Buffered store of broadcast messages gossiped between members
	 */
	private final MessageBuffer messageBuffer;

	/**
	 * This member
	 */
	private final Node node;

	private final FirefliesParameters parameters;

	/**
	 * Pending rebutal timers
	 */
	private final ConcurrentMap<UUID, Future<?>> pendingRebutals = new ConcurrentHashMap<>();

	/**
	 * Gossip rings - the list of rings that determine the magical low diameter
	 * connectivity
	 */
	private final List<Ring> rings = new ArrayList<>();

	/**
	 * Core round listeners
	 */
	private final List<Runnable> roundListeners = new CopyOnWriteArrayList<>();

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
	private final ConcurrentMap<UUID, Member> view = new ConcurrentHashMap<>();

	public View(Node node, FirefliesCommunications communications, List<X509Certificate> seeds,
			ScheduledExecutorService scheduler) {
		this.node = node;
		this.comm = communications;
		this.parameters = this.node.getParameters();
		this.scheduler = scheduler;
		diameter = diameter(parameters);
		assert diameter > 0 : "Diameter must be greater than zero: " + diameter;
		dispatcher = Executors.newSingleThreadExecutor(new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread daemon = new Thread(r, "Event dispatcher " + node);
				daemon.setDaemon(true);
				return daemon;
			}
		});
		this.messageBuffer = new MessageBuffer(parameters.bufferSize, parameters.toleranceLevel * diameter + 1);

		service.calculateTimeoutInterval(DEFAULT_INTERVAL);

		connections = cacheBuilder().build(new CacheLoader<Member, FfClientCommunications>() {
			@Override
			public FfClientCommunications load(Member to) throws Exception {
				return comm.connectTo(to, node);
			}
		});

		for (int i = 0; i < parameters.rings; i++) {
			rings.add(new Ring(i));
		}

		if (node.getNote() == null) {
			node.nextNote();
		}
		node.setFailed(false);
		add(node);
		List<UUID> seedList = new ArrayList<>();
		seeds.stream().map(cert -> new Member(cert, parameters)).peek(m -> seedList.add(m.getId()))
				.forEach(m -> addSeed(m));
		comm.initialize(this);
		log.info("{} using seeds: {}", node.getId(), seedList);
	}

	/**
	 * @return the analytical diameter of the graph of members
	 */
	public int getDiameter() {
		return diameter;
	}

	/**
	 * @return the Map of all failed members
	 */
	public Map<UUID, Member> getFailed() {
		return failed;
	}

	/**
	 * @return the Map of all live members
	 */
	public Map<UUID, Member> getLive() {
		return live;
	}

	/**
	 * @return the maximum number of members allowed for the view
	 */
	public int getMaximumCardinality() {
		return parameters.cardinality;
	}

	/**
	 * @return The member that represents this View
	 */
	public Node getNode() {
		return node;
	}

	/**
	 * @param ring
	 * @return the Ring corresponding to the index
	 */
	public Ring getRing(int ring) {
		return rings.get(ring);
	}

	/**
	 * @return the List of Rings that this view maintains
	 */
	public List<Ring> getRings() {
		return rings;
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
	public ConcurrentMap<UUID, Member> getView() {
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

	/**
	 * Accuse the member on the list of rings. If the member has disabled the ring
	 * in its mask, do not issue the accusation
	 * 
	 * @param member
	 * @param ring
	 */
	void accuseOn(Member member, int ring) {
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
		Member accuser = view.get(accusation.getAccuser());
		Member accused = view.get(accusation.getAccused());
		if (accuser == null || accused == null) {
			log.info("Accusation discarded, accused or accuser do not exist in view");
			return;
		}

		if (accusation.getRingNumber() > parameters.rings) {
			log.debug("Invalid ring in accusation: {}", accusation.getRingNumber());
			return;
		}

		if (accused.getEpoch() != accusation.getEpoch()) {
			log.debug("Accusation discarded in epoch: {}  for: {} epoch: {}" + accusation.getEpoch(), accused.getId(),
					accused.getEpoch());
			return;
		}

		if (!accused.getNote().getMask().get(accusation.getRingNumber())) {
			log.warn("Member {} accussed on disabled ring {} by {}", accused.getId(), accusation.getRingNumber(),
					accuser.getId());
			return;
		}

		// verify the accusation after all other tests pass, as it's reasonably
		// expensive and we want to filter out all the noise first, before going to all
		// the trouble (and cost) to validate the sig
		if (!accusation.verify(accuser.forVerification(parameters.signatureAlgorithm))) {
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
	void add(Accusation accusation, Member accuser, Member accused) {
		Ring ring = rings.get(accusation.getRingNumber());

		if (accused.isAccusedOn(ring.getIndex())) {
			Accusation currentAccusation = accused.getAccusation(ring.getIndex());
			Member currentAccuser = view.get(currentAccusation.getAccuser());

			if (ring.isBetween(currentAccuser, accuser, accused)) {
				accused.addAccusation(accusation);
				log.info("{} accused by {} on ring {} (replacing {})", accused, accuser, ring.getIndex(),
						currentAccuser);
			}
		} else {
			Member predecessor = ring.predecessor(accused, x -> (!x.isAccused()) || (x.equals(accuser)));
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
	Member add(CertWithHash cert) {
		UUID id = getMemberId(cert.certificate);
		Member member = view.get(id);
		if (member != null) {
			update(member, cert);
			return member;
		}
		member = new Member(cert.certificate, cert.derEncoded, parameters, cert.certificateHash);
		return add(member);
	}

	/**
	 * Add a new member to the view
	 * 
	 * @param member
	 */
	Member add(Member member) {
		Member previous = view.putIfAbsent(member.getId(), member);
		if (previous == null) {
			log.info("Adding member: {}", member.getId(), member.getCertificate().getSubjectDN());
			rings.forEach(ring -> ring.insert(member));
			if (!member.isFailed()) {
				recover(member);
			} else {
				failed.put(member.getId(), member);
			}
			return member;
		}
		return previous;
	}

	/**
	 * add an inbound note to the view
	 * 
	 * @param note
	 */
	boolean add(Note note) {
		Member m = view.get(note.getId());
		if (m == null) {
			log.debug("No member for note: " + note.getId());
			return false;
		}

		if (m.getEpoch() >= note.getEpoch()) {
			// log.trace("Note redundant in epoch: {} for: {}", note.getEpoch(),
			// note.getId());
			return false;
		}

		BitSet mask = note.getMask();
		if (!isValidMask(mask, parameters)) {
			log.debug("Note: {} mask invalid {}", note.getId(), mask);
			return false;
		}

		// verify the note after all other tests pass, as it's reasonably expensive and
		// we want to filter out all
		// the noise first, before going to all the trouble to validate
		if (!note.verify(m.forVerification(parameters.signatureAlgorithm))) {
			log.debug("Note signature invalid: {}", note.getId());
			return false;
		}

		if (m.isAccused()) {
			stopRebutalTimer(m);
			checkInvalidations(m);
		}

		if (m.isFailed() || m.getEpoch() == 0) {
			recover(m);
		}

		m.setNote(note);

		return true;
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
	void addSeed(Member seed) {
		seed.setNote(new Note(seed.getId(), -1, Node.createInitialMask(parameters.toleranceLevel, parameters.entropy),
				node.forSigning()));
		rings.forEach(ring -> ring.insert(seed));
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
			certificate = (X509Certificate) cf
					.generateCertificate(new ByteArrayInputStream(encoded.getContent().array()));
		} catch (CertificateException e) {
			log.warn("Invalid DER encoded certificate", e);
			return null;
		}
		try {
			certificate.verify(parameters.ca.getPublicKey());
		} catch (NoSuchAlgorithmException | InvalidKeyException | SignatureException | CertificateException
				| NoSuchProviderException e) {
			log.warn("Invalid cert: {}", certificate.getSubjectDN(), e);
			return null;
		}

		return new CertWithHash(encoded.getDigest().getHash().array(), certificate, encoded.getContent().array());
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
	void checkInvalidations(Member m) {
		Deque<Member> check = new ArrayDeque<>();
		check.add(m);
		while (!check.isEmpty()) {
			Member checked = check.pop();
			for (Ring ring : rings) {
				for (Member q : ring.successors(checked, member -> !member.isAccused())) {
					if (q.isAccusedOn(ring.getIndex())) {
						invalidate(q, ring, check);
					}
				}
			}
		}
	}

	/**
	 * @return the digests common for gossip with all neighbors
	 */
	Digests commonDigests() {
		return new Digests(gatherMessageDigests(), gatherCertificateDigests(), gatherNoteDigests(),
				gatherAccusationDigests());
	}

	/**
	 * @return the AccusationGossip for this view. This is the list of all
	 *         accusation digests in the view. Do not add accusation digests for
	 *         crashed members
	 */
	List<AccusationDigest> gatherAccusationDigests() {
		return view.values().stream().filter(m -> !m.equals(node)) // Never send accusations from the view's node
				.flatMap(m -> m.getAccusationDigests()).filter(digest -> !failed.containsKey(uuid(digest.getId())))
				.collect(Collectors.toList());
	}

	/**
	 * @return the CertificateGossip for this view. This is the list of all
	 *         certicate digests of all members in the view
	 */
	List<CertificateDigest> gatherCertificateDigests() {
		return view.values().stream().filter(m -> !m.equals(node)).map(m -> m.getCertificateDigest())
				.filter(e -> e != null).collect(Collectors.toList());
	}

	/**
	 * @return the MessageGossip for this view. This is the list of all message
	 *         digests of the view's messageBuffer
	 */
	List<MessageDigest> gatherMessageDigests() {
		return messageBuffer.getDigests();
	}

	/**
	 * @return the NoteGossip for this view. This is the list of the current Note
	 *         digest for all members in the view
	 */
	List<NoteDigest> gatherNoteDigests() {
		return view.values().stream().filter(m -> !m.equals(node)).map(m -> m.getNoteDigest()).filter(e -> e != null)
				.collect(Collectors.toList());
	}

	void gc(Member member) {
		failed.put(member.getId(), member);
		live.remove(member.getId());
		member.setFailed(true);
		dispatcher.execute(() -> membershipListeners.parallelStream().forEach(l -> {
			try {
				l.fail(member);
			} catch (Throwable e) {
				log.error("error sending fail to listener: " + l, e);
			}
		}));
	}

	/**
	 * for testing
	 * 
	 * @return
	 */
	Map<UUID, Future<?>> getPendingRebutals() {
		return pendingRebutals;
	}

	/**
	 * Gossip with the member
	 * 
	 * @param ring - the index of the gossip ring the gossip is originating from in
	 *             this view
	 * @param link - the outbound communications to the paired member
	 * @throws AvroRemoteException
	 */
	boolean gossip(int ring, FfClientCommunications link) throws AvroRemoteException {
		Gossip gossip = link.gossip(node.getSignedNote(), ring, commonDigests());
		if (gossip.getRedirect()) {
			if (gossip.getCertificates().getUpdates().size() != 1 && gossip.getNotes().getUpdates().size() != 1) {
				log.warn("Redirect response from {} on ring {} did not contain redirect member certificate and note",
						link.getMember(), ring);
				return false;
			}
			if (gossip.getAccusations().getUpdates().size() > 0) {
				// Reset our epoch to whatever the group has recorded for this recovering node
				long max = gossip.getAccusations().getUpdates().stream()
						.map(signed -> new Accusation(signed.getContent().array(), signed.getSignature().array()))
						.mapToLong(a -> a.getEpoch()).max().orElse(-1);
				node.nextNote(max + 1);
			}
			CertWithHash certificate = certificateFrom(gossip.getCertificates().getUpdates().get(0));
			if (certificate != null) {
				connections.invalidate(link.getMember());
				add(certificate);
				Signed signed = gossip.getNotes().getUpdates().get(0);
				Note note = new Note(signed.getContent().array(), signed.getSignature().array());
				add(note);
				gossip.getAccusations().getUpdates()
						.forEach(s -> add(new Accusation(s.getContent().array(), s.getSignature().array())));
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
	void invalidate(Member q, Ring ring, Deque<Member> check) {
		Accusation qa = q.getAccusation(ring.getIndex());
		Member accuser = view.get(qa.getAccuser());
		Member accused = view.get(qa.getAccused());
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
		return update.getAccusations().isEmpty() && update.getCertificates().isEmpty() && update.getMessages().isEmpty()
				&& update.getNotes().isEmpty();
	}

	/**
	 * @param ring - the ring to gossip on
	 * @return the communication link for this ring, based on current membership
	 *         state
	 */
	FfClientCommunications linkFor(Integer ring) {
		Member successor = rings.get(ring).successor(node, m -> !m.isFailed());
		if (successor == null) {
			log.debug("No successor to node on ring: {}", ring);
			return null;
		}

		return linkFor(successor);
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
		} catch (AvroRemoteException e) {
			connections.invalidate(link.getMember());
			if (e.getCause() instanceof ClosedChannelException) {
				log.error("Closed channel when pinging {} -> {} : {}", link.getMember(),
						link.getMember().getFirefliesEndpoint(), e.toString());
				return;
			}
			log.error("Exception pinging {} -> {} : {}", link.getMember(), link.getMember().getFirefliesEndpoint(),
					e.toString());
			accuseOn(link.getMember(), lastRing);
		}
	}

	/**
	 * @param ring - the ring to gossip on
	 * @return the communication link for this ring, based on current membership
	 *         state
	 */
	FfClientCommunications monitorLinkFor(Integer ring) {
		Member successor = rings.get(ring).successor(node, m -> !m.isFailed() && !m.isAccused());
		if (successor == null) {
			log.debug("No successor to node on ring: {}", ring);
			return null;
		}

		return linkFor(successor);
	}

	/**
	 * Drive one round of the View. This involves a round of gossip() and a round of
	 * monitor().
	 */
	void oneRound() {
		comm.logDiag();
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
	}

	/**
	 * Process the inbound accusations from the gossip. Reconcile the differences
	 * between the view's state and the digests of the gossip. Update the reply with
	 * the list of digests the view requires, as well as proposed updates based on
	 * the inbound digets that the view has more recent information. Do not forward
	 * accusations from crashed members
	 * 
	 * @param digests
	 * @return
	 */
	AccusationGossip processAccusationDigests(List<AccusationDigest> digests) {
		Set<AccTag> received = new HashSet<>(digests.size());
		List<Signed> updates = new ArrayList<>();
		AccusationGossip accusations = new AccusationGossip();
		accusations.setDigests(digests.stream().filter(accusation -> {
			UUID id = uuid(accusation.getId());
			Member member = view.get(id);
			if (member == null) {
				return true;
			}
			Long epoch = accusation.getEpoch();
			int ring = accusation.getRing();
			received.add(new AccTag(id, ring));
			if (epoch < member.getEpoch()) {
				Accusation existing = member.getAccusation(ring);
				if (existing != null && !failed.containsKey(existing.getAccuser())) {
					updates.add(existing.getSigned());
					return false;
				}
			}
			return epoch > member.getEpoch() || !member.isAccusedOn(ring);
		}).collect(Collectors.toList()));
		Sets.difference(view.values().stream().flatMap(e -> e.getAccusationTags().stream()).collect(Collectors.toSet()),
				received).stream().map(tag -> {
					Member member = view.get(tag.id);
					return member == null ? null : member.getAccusation(tag.ring);
				}).filter(acc -> acc != null).filter(acc -> !failed.containsKey(acc.getAccuser()))
				.map(acc -> acc.getSigned()).forEach(signed -> updates.add(signed));
		accusations.setUpdates(updates);
		return accusations;
	}

	/**
	 * The "second" message - response - in the anti-entropy protocol. Process the
	 * inbound certificates from the gossip. Reconcile the differences between the
	 * view's state and the digests of the gossip. Update the reply with the list of
	 * digests the view requires, as well as proposed updates based on the inbound
	 * digets that the view has more recent information for
	 * 
	 * @param from
	 * @param certificates
	 * @return the CertificateGossip based on the processing
	 */
	CertificateGossip processCertificateDigests(UUID from, List<CertificateDigest> certificates) {
		Set<UUID> received = new HashSet<>(certificates.size());
		List<EncodedCertificate> updates = new ArrayList<>();
		CertificateGossip gossip = new CertificateGossip();

		// Process all inbound digests
		gossip.setDigests(certificates.stream().filter(cert -> {
			UUID id = uuid(cert.getId());
			received.add(id);
			if (from.equals(id)) {
				return false;
			}
			Member member = view.get(id);
			if (member == null) {
				return true;
			}
			byte[] hash = cert.getHash().array();
			// Add an update if this view has a newer version than the inbound digest
			Long epoch = cert.getEpoch();
			if (epoch < member.getEpoch() || !Arrays.equals(hash, member.getCertificateHash())) {
				updates.add(member.getEncodedCertificate());
				return false;
			}
			return epoch > member.getEpoch();
		}).collect(Collectors.toList()));

		// Add all updates that this view has that aren't reflected in the inbound
		// digests
		Sets.difference(view.keySet(), received).stream().filter(id -> id.equals(from)).map(id -> view.get(id))
				.filter(m -> m != null).map(m -> m.getEncodedCertificate()).filter(cert -> cert != null)
				.forEach(cert -> updates.add(cert));
		gossip.setUpdates(updates);
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
	 */
	NoteGossip processNoteDigests(UUID from, List<NoteDigest> digests) {
		NoteGossip notes = new NoteGossip();
		List<Signed> updates = new ArrayList<>();
		Set<UUID> received = new HashSet<>(digests.size());
		// Process all inbound digests
		notes.setDigests(digests.stream().filter(note -> {
			UUID id = uuid(note.getId());
			received.add(id);
			if (from.equals(id)) {
				return false;
			}
			Member member = view.get(id);
			if (member == null) {
				return true;
			}
			Long epoch = note.getEpoch();
			// Add an update if this view has a newer version than the inbound digest
			if (epoch < member.getEpoch()) {
				updates.add(member.getSignedNote());
				return false;
			}
			return epoch > member.getEpoch();
		}).collect(Collectors.toList()));
		// Add all digests that this view has that aren't reflected in the inbound
		// digests
		Sets.difference(view.keySet(), received).stream().filter(id -> id.equals(from)).map(id -> view.get(id))
				.filter(m -> m != null).map(m -> m.getSignedNote()).filter(note -> note != null)
				.forEach(note -> updates.add(note));
		notes.setUpdates(updates);
		return notes;
	}

	/**
	 * Process the updates of the supplied juicy gossip. This is the Jesus Nut of
	 * state change driving the view.
	 * 
	 * @param gossip
	 */
	void processUpdates(Gossip gossip) {
		processUpdates(gossip.getCertificates().getUpdates(), gossip.getNotes().getUpdates(),
				gossip.getAccusations().getUpdates(), gossip.getMessages().getUpdates());
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
		certificatUpdates.stream().map(cert -> certificateFrom(cert)).filter(cert -> cert != null)
				.forEach(cert -> add(cert));
		noteUpdates.stream().map(s -> new Note(s.getContent().array(), s.getSignature().array()))
				.forEach(note -> add(note));
		accusationUpdates.stream().map(s -> new Accusation(s.getContent().array(), s.getSignature().array()))
				.forEach(accusation -> add(accusation));

		if (node.isAccused()) {
			// Rebut the accusations by creating a new note and clearing the accusations
			node.nextNote();
			node.clearAccusations();
		}

		Map<Integer, List<Msg>> newMessages = new HashMap<>();

		messageBuffer.merge(messageUpdates, message -> validate(message)).stream().map(m -> {
			UUID id = uuid(m.getDigest().getSource());
			Member from = view.get(id);
			if (from == null) {
				log.trace("{} message from unknown member: {}", node, id);
				return null;
			} else {
				return new Msg(from, m.getChannel(), m.getContent().array());
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
	void recover(Member member) {
		member.setFailed(false);
		failed.remove(member.getId());
		live.putIfAbsent(member.getId(), member);
		rings.forEach(ring -> ring.insert(member));
		log.debug("Recovering: {}", member.getId());
		dispatcher.execute(() -> membershipListeners.parallelStream().forEach(l -> {
			try {
				l.recover(member);
			} catch (Throwable e) {
				log.error("error recoving member in listener: " + l, e);
			}
		}));
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
	Gossip redirectTo(Member member, int ring, Member successor) {
		log.info("Redirecting from {} to {} on ring {}", node, successor, ring);
		return new Gossip(true, new MessageGossip(Collections.emptyList(), Collections.emptyList()),
				new CertificateGossip(Collections.emptyList(),
						Collections.singletonList(successor.getEncodedCertificate())),
				new NoteGossip(Collections.emptyList(), Collections.singletonList(successor.getSignedNote())),
				new AccusationGossip(Collections.emptyList(), member.getEncodedAccusations()));
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
	void startRebutalTimer(Member m) {
		pendingRebutals.computeIfAbsent(m.getId(),
				id -> scheduler.schedule(() -> gc(m), service.getTimeoutInterval().toMillis(), TimeUnit.MILLISECONDS));
	}

	void stopRebutalTimer(Member m) {
		m.clearAccusations();
		log.info("New note, epoch {}, clearing accusations on {}", m.getEpoch(), m.getId());
		Future<?> pending = pendingRebutals.remove(m.getId());
		if (pending != null) {
			pending.cancel(true);
		}
	}

	/**
	 * Update the member with a new certificate
	 * 
	 * @param member
	 * @param cert
	 */
	void update(Member member, CertWithHash cert) {
		// TODO Auto-generated method stub

	}

	/**
	 * gather ye accusal gossip using the common digests and the list of updates
	 * based on the inboud requested digests
	 * 
	 * @param common
	 * @param requested
	 * @return
	 */
	AccusationGossip updateAccusations(List<AccusationDigest> common, List<AccusationDigest> requested) {
		return new AccusationGossip(common,
				requested.stream().filter(a -> view.get(uuid(a.getId())) != null)
						.map(a -> view.get(uuid(a.getId())).getEncodedAccusation(a.getRing())).filter(e -> e != null)
						.collect(Collectors.toList()));
	}

	/**
	 * gather ye certificate gossip using the common digests and the list of updates
	 * based on the inboud requested digests
	 * 
	 * @param common
	 * @param requested
	 * @return
	 */
	CertificateGossip updateCertificates(List<CertificateDigest> common, List<CertificateDigest> requested) {
		return new CertificateGossip(common,
				requested.stream().map(digest -> uuid(digest.getId())).map(id -> view.get(id)).filter(e -> e != null)
						.map(m -> m.getEncodedCertificate()).collect(Collectors.toList()));
	}

	/**
	 * gather ye accusal note using the common digests and the list of updates based
	 * on the inboud requested digests
	 * 
	 * @param common
	 * @param requested
	 * @return
	 */
	NoteGossip updateNotes(List<NoteDigest> common, List<NoteDigest> requested) {
		return new NoteGossip(common, requested.stream().map(digest -> uuid(digest.getId())).map(id -> view.get(id))
				.filter(e -> e != null).map(m -> m.getSignedNote()).collect(Collectors.toList()));
	}

	/**
	 * Process the gossip reply. Return the gossip with the updates determined from
	 * the inbound digests.
	 * 
	 * @param gossip
	 * @return
	 */
	Update updatesForDigests(Gossip gossip) {
		return new Update(messageBuffer.updatesFor(gossip.getMessages().getDigests()),
				gossip.getCertificates().getDigests().stream().map(digest -> view.get(uuid(digest.getId())))
						.filter(member -> member != null).map(member -> member.getEncodedCertificate())
						.collect(Collectors.toList()),
				gossip.getNotes().getDigests().stream().map(digest -> view.get(uuid(digest.getId())))
						.filter(member -> member != null).map(member -> member.getSignedNote())
						.collect(Collectors.toList()),
				gossip.getAccusations().getDigests().stream().filter(digest -> view.containsKey(uuid(digest.getId())))
						.map(digest -> view.get(uuid(digest.getId())).getAccusation(digest.getRing()))
						.filter(accusation -> accusation != null).map(accusation -> accusation.getSigned())
						.collect(Collectors.toList()));
	}

	/**
	 * validate the message's signature
	 * 
	 * @param message
	 * @return true if the message is valid, false if from an unknown member or
	 *         signature doesn't validate
	 */
	boolean validate(Message message) {
		UUID from = uuid(message.getDigest().getSource());
		Member member = view.get(from);
		if (member == null) {
			return false;
		}
		Signature signature = member.forVerification(parameters.signatureAlgorithm);
		try {
			signature.update(message.getContent().array());
			return signature.verify(message.getSignature().array());
		} catch (SignatureException e) {
			log.debug("invalid signature for message {}", uuid(message.getDigest().getId()), from);
			return false;
		}
	}

	private CacheBuilder<Member, FfClientCommunications> cacheBuilder() {
		CacheBuilder<?, ?> builder = CacheBuilder.newBuilder();
		@SuppressWarnings("unchecked")
		CacheBuilder<Member, FfClientCommunications> castBuilder = (CacheBuilder<Member, FfClientCommunications>) builder;
		castBuilder.maximumSize(parameters.rings + 2)
				.removalListener(new RemovalListener<Member, FfClientCommunications>() {
					@Override
					public void onRemoval(RemovalNotification<Member, FfClientCommunications> notification) {
						notification.getValue().close();
					}
				});
		return castBuilder;
	}

	private FfClientCommunications linkFor(Member m) {
		try {
			return connections.get(m);
		} catch (UncheckedExecutionException e) {
			log.debug("error caching connections to " + m, e.getCause());
		} catch (InvalidCacheLoadException e) {
			log.debug("error caching connections to " + m, e.getCause());
		} catch (ExecutionException e) {
			log.debug("error caching connections to " + m, e.getCause());
		}
		return null;
	}
}
