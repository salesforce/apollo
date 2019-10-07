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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.salesforce.apollo.avro.MessageGossip;
import com.salesforce.apollo.fireflies.communications.netty.FirefliesNettyCommunications;
import com.salesforce.apollo.protocols.Utils;

import io.github.olivierlemasle.ca.RootCertificate;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class MtlsTest {

	private static final RootCertificate ca = getCa();
	private static Map<UUID, CertWithKey> certs;
	private static final FirefliesParameters parameters = new FirefliesParameters(ca.getX509Certificate());

	@BeforeClass
	public static void beforeClass() {
		certs = IntStream.range(1, 101).parallel().mapToObj(i -> getMember(i))
				.collect(Collectors.toMap(cert -> Member.getMemberId(cert.getCertificate()), cert -> cert));
	}

	@Test
	public void smoke() throws Exception {
		Random entropy = new Random(0x666);

		List<X509Certificate> seeds = new ArrayList<>();
		List<Node> members = certs.values().parallelStream()
				.map(cert -> new CertWithKey(cert.getCertificate(), cert.getPrivateKey()))
				.map(cert -> new Node(cert, parameters)).collect(Collectors.toList());
		assertEquals(certs.size(), members.size());

		while (seeds.size() < parameters.toleranceLevel + 1) {
			CertWithKey cert = certs.get(members.get(entropy.nextInt(members.size())).getId());
			if (!seeds.contains(cert.getCertificate())) {
				seeds.add(cert.getCertificate());
			}
		}
		MessageBuffer messageBuffer = mock(MessageBuffer.class);
		when(messageBuffer.process(any()))
				.thenReturn(new MessageGossip(Collections.emptyList(), Collections.emptyList()));

		ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(members.size());

		List<View> views = members.stream()
				.map(node -> new View(node, new FirefliesNettyCommunications(), seeds, scheduler))
				.collect(Collectors.toList());

		long then = System.currentTimeMillis();
		views.forEach(view -> view.getService().start(Duration.ofMillis(1_000)));

		assertTrue("view did not stabilize", Utils.waitForCondition(30_000, 1_000, () -> {
			return views.stream().map(view -> view.getLive().size() != views.size() ? view : null)
					.filter(view -> view != null).count() == 0;
		}));
		System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
				+ views.size() + " members");

		System.out.println("Checking views for consistency");
		List<View> invalid = views.stream().map(view -> view.getLive().size() != views.size() ? view : null)
				.filter(view -> view != null).collect(Collectors.toList());
		assertEquals(invalid.stream().map(view -> {
			Set<?> difference = Sets.difference(
					views.stream().map(v -> v.getNode().getId()).collect(Collectors.toSet()), view.getLive().keySet());
			return "Invalid membership: " + view.getNode() + ", missing: " + difference.size();
		}).collect(Collectors.toList()).toString(), 0, invalid.size());

		System.out.println("Stoping views");
		views.forEach(view -> view.getService().stop());
		
		System.out.println("Restarting views");
		views.forEach(view -> view.getService().start(Duration.ofMillis(1000)));

		assertTrue("view did not stabilize", Utils.waitForCondition(30_000, 100, () -> {
			return views.stream().map(view -> view.getLive().size() != views.size() ? view : null)
					.filter(view -> view != null).count() == 0;
		}));
		
		System.out.println("Stabilized, now sleeping to see if views remain stabilized");
		Thread.sleep(10_000);
		assertTrue("view did not stabilize", Utils.waitForCondition(30_000, 100, () -> {
			return views.stream().map(view -> view.getLive().size() != views.size() ? view : null)
					.filter(view -> view != null).count() == 0;
		}));
		System.out.println("View has stabilized after restart in " + (System.currentTimeMillis() - then) + " Ms");

		System.out.println("Checking views for consistency");
		invalid = views.stream().map(view -> view.getLive().size() != views.size() ? view : null)
				.filter(view -> view != null).collect(Collectors.toList());
		assertEquals(invalid.stream().map(view -> {
			Set<?> difference = Sets.difference(
					views.stream().map(v -> v.getNode().getId()).collect(Collectors.toSet()), view.getLive().keySet());
			return "Invalid membership: " + view.getNode() + ", missing: " + difference.size();
		}).collect(Collectors.toList()).toString(), 0, invalid.size());

		System.out.println("Stoping views");
		views.forEach(view -> view.getService().stop());
	}
}
