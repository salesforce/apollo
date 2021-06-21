/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import static com.salesforce.apollo.test.pregen.PregenPopulation.getMember;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Sets;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.crypto.ssl.CertificateValidator;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.Ring;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class FunctionalTest {
    private static int                                    CARDINALITY = 10;
    private static Map<Digest, CertificateWithPrivateKey> certs;
    private static final FirefliesParameters              parameters;

    static {
        parameters = FirefliesParameters.newBuilder()
                                        .setCardinality(CARDINALITY)
                                        .setCertificateValidator(CertificateValidator.NONE)
                                        .build();
    }

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(0, CARDINALITY)
                         .parallel()
                         .mapToObj(i -> getMember(i))
                         .collect(Collectors.toMap(cert -> Member.getMemberIdentifier(cert.getX509Certificate()),
                                                   cert -> cert));
    }

    private final List<Router> communications = new ArrayList<>();
    private List<View>         views;

    @AfterEach
    public void after() {
        if (views != null) {
            views.forEach(e -> e.getService().stop());
            views.clear();
        }
        communications.forEach(e -> e.close());
        communications.clear();
    }

    @Test
    public void e2e() throws Exception {
        Random entropy = new Random(0x666);
        MetricRegistry registry = new MetricRegistry();
        FireflyMetrics metrics = new FireflyMetricsImpl(registry);

        List<X509Certificate> seeds = new ArrayList<>();
        List<Node> members = certs.values()
                                  .stream()
                                  .map(cert -> new Node(
                                          new SigningMemberImpl(Member.getMemberIdentifier(cert.getX509Certificate()),
                                                  cert.getX509Certificate(), cert.getPrivateKey(),
                                                  new Signer(0, cert.getPrivateKey()),
                                                  cert.getX509Certificate().getPublicKey()),
                                          parameters))
                                  .collect(Collectors.toList());
        assertEquals(certs.size(), members.size());

        while (seeds.size() < parameters.toleranceLevel + 1) {
            CertificateWithPrivateKey cert = certs.get(members.get(entropy.nextInt(members.size())).getId());
            if (!seeds.contains(cert.getX509Certificate())) {
                seeds.add(cert.getX509Certificate());
            }
        }

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);

        ExecutorService serverThreads = new ForkJoinPool();
        views = members.stream().map(node -> {
            Router comms = new LocalRouter(node, ServerConnectionCache.newBuilder().setTarget(30).setMetrics(metrics),
                    serverThreads);
            comms.start();
            communications.add(comms);
            return new View(DigestAlgorithm.DEFAULT.getOrigin(), node, comms, metrics);
        })
                       .peek(view -> view.getService().start(Duration.ofMillis(20_000), seeds, scheduler))
                       .collect(Collectors.toList());

        for (int j = 0; j < 40; j++) {
            for (int i = 0; i < parameters.rings + 2; i++) {
                views.forEach(view -> view.getService().gossip(() -> {
                }));
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
