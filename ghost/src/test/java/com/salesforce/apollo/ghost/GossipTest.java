/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost;

import static com.salesforce.apollo.fireflies.PregenPopulation.getCa;
import static com.salesforce.apollo.fireflies.PregenPopulation.getMember;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.salesforce.apollo.avro.Entry;
import com.salesforce.apollo.avro.EntryType;
import com.salesforce.apollo.fireflies.CertWithKey;
import com.salesforce.apollo.fireflies.FirefliesParameters;
import com.salesforce.apollo.fireflies.Member;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.fireflies.View;
import com.salesforce.apollo.fireflies.stats.DropWizardStatsPlugin;
import com.salesforce.apollo.ghost.Ghost.GhostParameters;
import com.salesforce.apollo.ghost.communications.GhostLocalCommSim;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.Utils;

import io.github.olivierlemasle.ca.RootCertificate;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class GossipTest {

	private static final RootCertificate ca = getCa();
	private static Map<UUID, CertWithKey> certs;
	private static int initial = 50;
	private static final FirefliesParameters parameters = new FirefliesParameters(ca.getX509Certificate());

	@BeforeClass
	public static void beforeClass() {
		certs = IntStream.range(1, 101).parallel().mapToObj(i -> getMember(i))
				.collect(Collectors.toMap(cert -> Member.getMemberId(cert.getCertificate()), cert -> cert));
	}

	private List<Node> members;
	private ScheduledExecutorService scheduler;
	private List<View> views;
	private List<X509Certificate> seeds;

	@After
	public void after() {
		views.forEach(e -> e.getService().stop());
	}

	@Before
	public void before() {
		Random entropy = new Random(0x666);

		seeds = new ArrayList<>();
		members = certs.values().parallelStream().map(cert -> new Node(cert, parameters)).collect(Collectors.toList());
		MetricRegistry registry = new MetricRegistry();
		com.salesforce.apollo.fireflies.communications.FfLocalCommSim ffComms = new com.salesforce.apollo.fireflies.communications.FfLocalCommSim(
				new DropWizardStatsPlugin(registry));
		assertEquals(certs.size(), members.size());

		while (seeds.size() < parameters.toleranceLevel + 1) {
			CertWithKey cert = certs.get(members.get(entropy.nextInt(20)).getId());
			if (!seeds.contains(cert.getCertificate())) {
				seeds.add(cert.getCertificate());
			}
		}

		System.out.println("Seeds: " + seeds.stream().map(e -> Member.getMemberId(e)).collect(Collectors.toList()));
		scheduler = Executors.newScheduledThreadPool(members.size() * 3);

		views = members.stream().map(node -> new View(node, ffComms, seeds, scheduler)).collect(Collectors.toList());
	}

	@Test
	public void gossip() throws Exception {
		long then = System.currentTimeMillis();

		List<View> testViews = new ArrayList<>();

		for (int i = 0; i < initial; i++) {
			testViews.add(views.get(i));
		}

		testViews.forEach(e -> e.getService().start(Duration.ofMillis(1000)));

		assertTrue("view did not stabilize", Utils.waitForCondition(15_000, 1_000, () -> {
			return testViews.stream().filter(view -> view.getLive().size() != testViews.size()).count() == 0;
		}));

		System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
				+ testViews.size() + " members");

		GhostLocalCommSim communications = new GhostLocalCommSim();
		List<Ghost> ghosties = testViews.stream()
				.map(view -> new Ghost(new GhostParameters(), communications, view, new MemoryStore()))
				.collect(Collectors.toList());
		ghosties.forEach(e -> e.getService().start());
		assertEquals("Not all nodes joined the cluster", ghosties.size(), ghosties.parallelStream()
				.map(g -> Utils.waitForCondition(30_000, () -> g.joined())).filter(e -> e).count());

		int rounds = 3;
		Map<HashKey, Entry> stored = new HashMap<>();
		for (int i = 0; i < rounds; i++) {
			for (Ghost ghost : ghosties) {
				Entry entry = new Entry(EntryType.DAG,
						ByteBuffer.wrap(String.format("Member: %s round: %s", ghost.getNode().getId(), i).getBytes()));
				stored.put(ghost.putEntry(entry), entry);
			}
		}

		for (java.util.Map.Entry<HashKey, Entry> entry : stored.entrySet()) {
			for (Ghost ghost : ghosties) {
				Entry found = ghost.getEntry(entry.getKey());
				assertNotNull(found);
				assertArrayEquals(entry.getValue().getData().array(), found.getData().array());
			}
		}

		while (testViews.size() < views.size()) {
			int start = testViews.size() + 1;
			for (int i = 0; i < views.get(0).getParameters().toleranceLevel
					&& testViews.size() < views.size(); i++) {
				View view = views.get(i + start);
				testViews.add(view);
				ghosties.add(new Ghost(new GhostParameters(), communications, view, new MemoryStore()));
			}
			then = System.currentTimeMillis();
			testViews.forEach(e -> e.getService().start(Duration.ofMillis(1000)));
			assertTrue("view did not stabilize", Utils.waitForCondition(30_000, 1_000, () -> {
				return testViews.stream().filter(view -> view.getLive().size() != testViews.size()).count() == 0;
			}));
			System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
					+ testViews.size() + " members");
			ghosties.forEach(e -> e.getService().start());
			assertEquals("Not all nodes joined the cluster", ghosties.size(), ghosties.parallelStream()
					.map(g -> Utils.waitForCondition(60_000, () -> g.joined())).filter(e -> e).count());

		}

		for (java.util.Map.Entry<HashKey, Entry> entry : stored.entrySet()) {
			for (Ghost ghost : ghosties) {
				Entry found = ghost.getEntry(entry.getKey());
				assertNotNull("Not found: " + entry.getKey(), found);
				assertArrayEquals(entry.getValue().getData().array(), found.getData().array());
			}
		}
	}
}
