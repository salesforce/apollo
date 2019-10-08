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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.salesforce.apollo.avro.DagEntry;
import com.salesforce.apollo.avro.HASH;
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

public class DagTest {

	private static final RootCertificate ca = getCa();
	private static Map<UUID, CertWithKey> certs;
	private static final FirefliesParameters parameters = new FirefliesParameters(ca.getX509Certificate());

	@BeforeClass
	public static void beforeClass() {
		certs = IntStream.range(1, 101).parallel().mapToObj(i -> getMember(i))
				.collect(Collectors.toMap(cert -> Member.getMemberId(cert.getCertificate()), cert -> cert));
	}

	private List<Node> members;
	private ScheduledExecutorService scheduler;
	private List<X509Certificate> seeds;
	private List<View> views;
	private Random entropy;

	@After
	public void after() {
		views.forEach(e -> e.getService().stop());
	}

	@Before
	public void before() {
		entropy = new Random(0x666);

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
	public void smoke() {
		long then = System.currentTimeMillis();

		List<View> testViews = new ArrayList<>();

		for (int i = 0; i < 50; i++) {
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

		Map<HashKey, DagEntry> stored = new ConcurrentSkipListMap<>();

		DagEntry root = new DagEntry();
		root.setData(ByteBuffer.wrap("root node".getBytes()));
		stored.put(ghosties.get(0).putDagEntry(root), root);

		int rounds = 10;

		for (int i = 0; i < rounds; i++) {
			for (Ghost ghost : ghosties) {
				DagEntry entry = new DagEntry();
				entry.setData(
						ByteBuffer.wrap(String.format("Member: %s round: %s", ghost.getNode().getId(), i).getBytes()));
				entry.setLinks(randomLinksTo(stored));
				stored.put(ghost.putDagEntry(entry), entry);
			}
		}

		for (Entry<HashKey, DagEntry> entry : stored.entrySet()) {
			for (Ghost ghost : ghosties) {
				DagEntry found = ghost.getDagEntry(entry.getKey());
				assertNotNull(found);
				assertArrayEquals(entry.getValue().getData().array(), found.getData().array());
			}
		}
		int start = testViews.size();

		int add = 25;
		for (int i = 0; i < add; i++) {
			View view = views.get(i + start);
			testViews.add(view);
			ghosties.add(new Ghost(new GhostParameters(), communications, view, new MemoryStore()));
		}

		then = System.currentTimeMillis();
		testViews.forEach(e -> e.getService().start(Duration.ofMillis(1000)));
		ghosties.forEach(e -> e.getService().start());
		assertEquals("Not all nodes joined the cluster", ghosties.size(), ghosties.parallelStream()
				.map(g -> Utils.waitForCondition(240_000, () -> g.joined())).filter(e -> e).count());

		assertTrue("view did not stabilize", Utils.waitForCondition(30_000, 1_000, () -> {
			return testViews.stream().filter(view -> view.getLive().size() != testViews.size()).count() == 0;
		}));

		System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
				+ testViews.size() + " members");

		for (Entry<HashKey, DagEntry> entry : stored.entrySet()) {
			for (Ghost ghost : ghosties) {
				DagEntry found = ghost.getDagEntry(entry.getKey());
				assertNotNull(ghost.getNode() + " not found: " + entry.getKey(), found);
				assertArrayEquals(entry.getValue().getData().array(), found.getData().array());
				if (entry.getValue().getLinks() == null) {
					assertNull(found.getLinks());
				} else {
					assertEquals(entry.getValue().getLinks().size(), found.getLinks().size());
				}
			}
		}
	}

	private List<HASH> randomLinksTo(Map<HashKey, DagEntry> stored) {
		List<HASH> links = new ArrayList<>();
		Set<HashKey> keys = stored.keySet();
		for (int i = 0; i < entropy.nextInt(10); i++) {
			Iterator<HashKey> it = keys.iterator();
			for (int j = 0; j < entropy.nextInt(keys.size()); j++) {
				it.next();
			}
			links.add(it.next().toHash());
		}
		return links;
	}
}
