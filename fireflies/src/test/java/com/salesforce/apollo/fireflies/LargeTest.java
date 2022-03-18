/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.comm.ServerConnectionCacheMetricsImpl;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.Signer.SignerImpl;
import com.salesforce.apollo.crypto.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.crypto.ssl.CertificateValidator;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class LargeTest {

    private static Map<Digest, CertificateWithPrivateKey> certs;
    private static final FirefliesParameters              parameters;
    private static final int                              CARDINALITY = 100;

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
                         .mapToObj(i -> Utils.getMember(i))
                         .collect(Collectors.toMap(cert -> Member.getMemberIdentifier(cert.getX509Certificate()),
                                                   cert -> cert));
    }

    private List<Node>            members;
    private List<View>            views;
    private List<Router>          communications = new ArrayList<>();
    private List<X509Certificate> seeds;
    private MetricRegistry        registry;
    private MetricRegistry        node0Registry;

    @AfterEach
    public void after() {
        if (views != null) {
            views.forEach(v -> v.stop());
            views.clear();
        }

        communications.forEach(e -> e.close());
        communications.clear();
    }

//     @Test
    public void swarm() throws Exception {
        initialize();

        long then = System.currentTimeMillis();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(100);
        views.forEach(view -> view.start(Duration.ofMillis(10_000), seeds, scheduler));

        assertTrue(Utils.waitForCondition(600_000, 10_000, () -> {
            return views.stream().filter(view -> view.getLive().size() != views.size()).count() == 0;
        }));

        System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
        + views.size() + " members");

        Thread.sleep(5_000);

        for (int i = 0; i < views.get(0).getContext().getRingCount(); i++) {
            for (View view : views) {
                Set<Digest> difference = views.get(0).getContext().ring(i).difference(view.getContext().ring(i));
                assertEquals(0, difference.size(), "difference in ring sets: " + difference);
            }
        }

        List<View> invalid = views.stream()
                                  .map(view -> view.getLive().size() != views.size() ? view : null)
                                  .filter(view -> view != null)
                                  .collect(Collectors.toList());
        assertEquals(0, invalid.size());

        Graph<Participant> testGraph = new Graph<>();
        for (View v : views) {
            for (int i = 0; i < views.get(0).getContext().getRingCount(); i++) {
                testGraph.addEdge(v.getNode(), v.getContext().ring(i).successor(v.getNode()));
            }
        }
        assertTrue(testGraph.isSC());

        for (View view : views) {
            for (int ring = 0; ring < view.getContext().getRingCount(); ring++) {
                final Collection<Participant> membership = view.getContext().ring(ring).members();
                for (Node node : members) {
                    assertTrue(membership.contains(node));
                }
            }
        }

        views.forEach(view -> view.stop());
        ConsoleReporter.forRegistry(node0Registry)
                       .convertRatesTo(TimeUnit.SECONDS)
                       .convertDurationsTo(TimeUnit.MILLISECONDS)
                       .build()
                       .report();
    }

    private void initialize() {
        Random entropy = new Random(0x666);
        registry = new MetricRegistry();
        node0Registry = new MetricRegistry();

        seeds = new ArrayList<>();
        members = certs.values()
                       .stream()
                       .map(cert -> new Node(new SigningMemberImpl(Member.getMemberIdentifier(cert.getX509Certificate()),
                                                                   cert.getX509Certificate(), cert.getPrivateKey(),
                                                                   new SignerImpl(cert.getPrivateKey()),
                                                                   cert.getX509Certificate().getPublicKey()),
                                             cert, parameters))
                       .collect(Collectors.toList());
        assertEquals(certs.size(), members.size());
        var builder = Context.<Participant>newBuilder().setCardinality(CARDINALITY);

        while (seeds.size() < builder.build().getRingCount() + 1) {
            CertificateWithPrivateKey cert = certs.get(members.get(entropy.nextInt(24)).getId());
            if (!seeds.contains(cert.getX509Certificate())) {
                seeds.add(cert.getX509Certificate());
            }
        }

        AtomicBoolean frist = new AtomicBoolean(true);
        ForkJoinPool executor = new ForkJoinPool();
        final var prefix = UUID.randomUUID().toString();
        views = members.stream().map(node -> {
            Context<Participant> context = builder.build();
            FireflyMetricsImpl fireflyMetricsImpl = new FireflyMetricsImpl(context.getId(),
                                                                           frist.getAndSet(false) ? node0Registry
                                                                                                  : registry);
            LocalRouter comms = new LocalRouter(prefix, node,
                                                ServerConnectionCache.newBuilder()
                                                                     .setTarget(2)
                                                                     .setMetrics(new ServerConnectionCacheMetricsImpl(frist.getAndSet(false) ? node0Registry
                                                                                                                                             : registry)),
                                                executor);
            communications.add(comms);
            return new View(context, node, comms, parameters.falsePositiveRate, fireflyMetricsImpl);
        }).collect(Collectors.toList());
    }
}
