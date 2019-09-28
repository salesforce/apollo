/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import static com.salesforce.apollo.fireflies.PregenPopulation.getCa;
import static com.salesforce.apollo.fireflies.PregenPopulation.getMember;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.salesforce.apollo.fireflies.View.MembershipListener;
import com.salesforce.apollo.fireflies.View.MessageChannelHandler;
import com.salesforce.apollo.fireflies.communications.FfLocalCommSim;
import com.salesforce.apollo.fireflies.stats.DropWizardStatsPlugin;
import com.salesforce.apollo.protocols.Utils;

import io.github.olivierlemasle.ca.RootCertificate;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class MessageTest {

	class Receiver implements MessageChannelHandler, MembershipListener {
		final Set<Member> counted = Collections.newSetFromMap(new ConcurrentHashMap<>());
		final AtomicInteger current;
		final Set<Member> discovered = Collections.newSetFromMap(new ConcurrentHashMap<>());
		final AtomicInteger dups = new AtomicInteger(0);
		final Set<Member> live = Collections.newSetFromMap(new ConcurrentHashMap<>());
		volatile CountDownLatch round;

		Receiver(int cardinality, AtomicInteger current) {
			this.current = current;
		}

		@Override
		public void fail(Member member) {
			live.add(member);
		}

		@Override
		public void message(List<Msg> messages) {
			messages.forEach(message -> {
				assert message.from != null : "null member";
				ByteBuffer buf = ByteBuffer.wrap(message.content);
				if (buf.getInt() == current.get() + 1) {
					if (counted.add(message.from)) {
						if (totalReceived.incrementAndGet() % 1_000 == 0) {
							System.out.print(".");
						}
						if (counted.size() == certs.size() - 1) {
							round.countDown();
						}
					} else {
						dups.incrementAndGet();
						System.out.print("!");
					}
				}
			});
		}

		@Override
		public void recover(Member member) {
			discovered.add(member);
			live.add(member);
		}

		public void setRound(CountDownLatch round) {
			this.round = round;
		}

		void reset() {
			dups.set(0);
			counted.clear();
		}

	}

	private static final RootCertificate ca = getCa();
	private static Map<UUID, CertWithKey> certs;
	private static final FirefliesParameters parameters = new FirefliesParameters(ca.getX509Certificate());

	@BeforeClass
	public static void beforeClass() {
		certs = IntStream.range(1, 101).parallel().mapToObj(i -> getMember(i))
				.collect(Collectors.toMap(cert -> Member.getMemberId(cert.getCertificate()), cert -> cert));
	}

	private final AtomicInteger totalReceived = new AtomicInteger(0);

	@Test
	public void broadcast() throws Exception {
		Random entropy = new Random(0x666);

		List<X509Certificate> seeds = new ArrayList<>();
		List<Node> members = certs.values().parallelStream()
				.map(cert -> new CertWithKey(cert.getCertificate(), cert.getPrivateKey()))
				.map(cert -> new Node(cert, parameters)).collect(Collectors.toList());
		MetricRegistry registry = new MetricRegistry();
		FfLocalCommSim communications = new FfLocalCommSim(new DropWizardStatsPlugin(registry));
		assertEquals(certs.size(), members.size());

		while (seeds.size() < parameters.toleranceLevel + 1) {
			CertWithKey cert = certs.get(members.get(entropy.nextInt(members.size())).getId());
			if (!seeds.contains(cert.getCertificate())) {
				seeds.add(cert.getCertificate());
			}
		}
		ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(members.size());

		List<View> views = members.stream().map(node -> new View(node, communications, seeds, scheduler))
				.collect(Collectors.toList());
		assertEquals(members.size(), communications.getServers().size());

		long then = System.currentTimeMillis();
		views.forEach(view -> view.getService().start(Duration.ofMillis(1000)));

		Utils.waitForCondition(15_000, 1_000, () -> {
			return views.stream().map(view -> view.getLive().size() != views.size() ? view : null)
					.filter(view -> view != null).count() == 0;
		});

		System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
				+ views.size() + " members");

		Map<Member, Receiver> receivers = new HashMap<>();
		AtomicInteger current = new AtomicInteger(-1);
		for (View view : views) {
			Receiver receiver = new Receiver(views.size(), current);
			view.register(0, receiver);
			view.register(receiver);
			receivers.put(view.getNode(), receiver);
		}
		int rounds = 5;
		for (int r = 0; r < rounds; r++) {
			CountDownLatch round = new CountDownLatch(views.size());
			for (Receiver receiver : receivers.values()) {
				receiver.setRound(round);
			}
			ByteBuffer buf = ByteBuffer.wrap(new byte[4]);
			buf.putInt(r);
			views.parallelStream().forEach(view -> view.publish(0, buf.array()));
			boolean success = round.await(10, TimeUnit.SECONDS);
			assertTrue("Did not complete round: " + r + " waiting for: " + round.getCount(), success);

			round = new CountDownLatch(views.size());
			current.incrementAndGet();
			for (Receiver receiver : receivers.values()) {
				assertEquals(0, receiver.dups.get());
				receiver.reset();
			}
		}
		System.out.println();
	}
}
