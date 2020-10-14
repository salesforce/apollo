/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import static com.salesforce.apollo.fireflies.PregenPopulation.getCa;
import static com.salesforce.apollo.fireflies.PregenPopulation.getMember;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Sets;
import com.salesforce.apollo.comm.Communications;
import com.salesforce.apollo.comm.LocalCommSimm;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.membership.CertWithKey;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.Ring;
import com.salesforce.apollo.protocols.HashKey;

import io.github.olivierlemasle.ca.RootCertificate;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class FunctionalTest {
    private static final RootCertificate     ca         = getCa();
    private static Map<HashKey, CertWithKey> certs;
    private static final FirefliesParameters parameters = new FirefliesParameters(ca.getX509Certificate());

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(1, 11)
                         .parallel()
                         .mapToObj(i -> getMember(i))
                         .collect(Collectors.toMap(cert -> Member.getMemberId(cert.getCertificate()), cert -> cert));
    }

    private final List<Communications> communications = new ArrayList<>();

    @AfterEach
    public void after() {
        communications.forEach(e -> e.close());
    }

    @Test
    public void e2e() throws Exception {
        Random entropy = new Random(0x666);
        MetricRegistry registry = new MetricRegistry();
        FireflyMetrics metrics = new FireflyMetricsImpl(registry);

        List<X509Certificate> seeds = new ArrayList<>();
        List<Node> members = certs.values()
                                  .parallelStream()
                                  .map(cert -> new CertWithKey(cert.getCertificate(), cert.getPrivateKey()))
                                  .map(cert -> new Node(cert, parameters))
                                  .collect(Collectors.toList());
        assertEquals(certs.size(), members.size());

        while (seeds.size() < parameters.toleranceLevel + 1) {
            CertWithKey cert = certs.get(members.get(entropy.nextInt(members.size())).getId());
            if (!seeds.contains(cert.getCertificate())) {
                seeds.add(cert.getCertificate());
            }
        }

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);

        List<View> views = members.parallelStream().map(node -> {
            Communications comms = new LocalCommSimm(
                    ServerConnectionCache.newBuilder().setTarget(30).setMetrics(metrics), node.getId());
            communications.add(comms);
            return new View(node, comms, scheduler, metrics);
        }).peek(view -> view.getService().start(Duration.ofMillis(20_000), seeds)).collect(Collectors.toList());

        for (int j = 0; j < 20; j++) {
            for (int i = 0; i < parameters.rings + 2; i++) {
                views.forEach(view -> view.getService().gossip());
            }
        }

        List<View> invalid = views.stream()
                                  .map(view -> view.getLive().size() != views.size() ? view : null)
                                  .filter(view -> view != null)
                                  .collect(Collectors.toList());
        assertEquals(0, invalid.size(), invalid.stream().map(view -> {
            Set<?> difference = Sets.difference(views.stream()
                                                     .map(v -> v.getNode().getId())
                                                     .collect(Collectors.toSet()),
                                                view.getLive()
                                                    .stream()
                                                    .map(v -> v.getId())
                                                    .collect(Collectors.toSet()));
            return "Invalid membership: " + view.getNode() + ", missing: " + difference.size();
        }).collect(Collectors.toList()).toString());

        View frist = views.get(0);
        for (View view : views) {
            for (int ring = 0; ring < parameters.rings; ring++) {
                Ring<Participant> trueRing = frist.getRing(ring);
                Ring<Participant> comparedTo = view.getRing(ring);
                assertEquals(trueRing.getRing(), comparedTo.getRing());
                assertEquals(trueRing.successor(view.getNode()), comparedTo.successor(view.getNode()));
            }
        }
    }
}
