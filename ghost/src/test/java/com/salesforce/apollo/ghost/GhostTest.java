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
public class GhostTest {

	private static final RootCertificate ca = getCa();
	private static Map<UUID, CertWithKey> certs;
	private static final FirefliesParameters parameters = new FirefliesParameters(ca.getX509Certificate());

	@BeforeClass
	public static void beforeClass() {
		certs = IntStream.range(1, 101).parallel().mapToObj(i -> getMember(i))
				.collect(Collectors.toMap(cert -> Member.getMemberId(cert.getCertificate()), cert -> cert));
	}

	@Test
	public void smoke() {
		Random entropy = new Random(0x666);

		List<X509Certificate> seeds = new ArrayList<>();
		List<Node> members = certs.values().parallelStream().map(cert -> new Node(cert, parameters))
				.collect(Collectors.toList());
		MetricRegistry registry = new MetricRegistry();
		com.salesforce.apollo.fireflies.communications.FfLocalCommSim ffComms = new com.salesforce.apollo.fireflies.communications.FfLocalCommSim(
				new DropWizardStatsPlugin(registry));
		assertEquals(certs.size(), members.size());

		while (seeds.size() < parameters.toleranceLevel + 1) {
			CertWithKey cert = certs.get(members.get(entropy.nextInt(members.size())).getId());
			if (!seeds.contains(cert.getCertificate())) {
				seeds.add(cert.getCertificate());
			}
		}

		ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(members.size());

		List<View> views = members.stream().map(node -> new View(node, ffComms, seeds, scheduler))
				.collect(Collectors.toList());
		assertEquals(members.size(), ffComms.getServers().size());

		long then = System.currentTimeMillis();
		views.forEach(view -> view.getService().start(Duration.ofMillis(1000)));

		Utils.waitForCondition(15_000, 1_000, () -> {
			return views.stream().map(view -> view.getLive().size() != views.size() ? view : null)
					.filter(view -> view != null).count() == 0;
		});

		System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
				+ views.size() + " members");

		GhostLocalCommSim communications = new GhostLocalCommSim();
		List<Ghost> ghosties = views.stream()
				.map(view -> new Ghost(new GhostParameters(), communications, view, new MemoryStore()))
				.collect(Collectors.toList());
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
	}
}
