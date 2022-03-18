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
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
public class SwarmTest {

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
            views.forEach(v -> v.getService().stop());
            views.clear();
        }

        communications.forEach(e -> e.close());
        communications.clear();
    }

    @Test
    public void churn() throws Exception {
        initialize();

        List<View> testViews = new ArrayList<>();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);

        for (int i = 0; i < 4; i++) {
            int start = testViews.size();
            for (int j = 0; j < 25; j++) {
                testViews.add(views.get(start + j));
            }
            long then = System.currentTimeMillis();
            testViews.forEach(view -> view.getService().start(Duration.ofMillis(100), seeds, scheduler));

            assertTrue(Utils.waitForCondition(15_000, 1_000, () -> {
                return testViews.stream().filter(view -> view.getLive().size() != testViews.size()).count() == 0;
            }), " size: " + testViews.size() + " views: "
            + testViews.stream()
                       .filter(e -> e.getLive().size() != testViews.size())
                       .map(v -> String.format("%s : %s", v.getContext().getId(),
                                               v.getContext().getOffline().stream().map(p -> p.getId()).toList()))
                       .toList());

            System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
            + testViews.size() + " members");
        }
        System.out.println("Stopping views");
        testViews.forEach(e -> e.getService().stop());
        testViews.clear();
//        communications.close();
        for (int i = 0; i < 4; i++) {
            System.out.println("Restarting views " + (i * 25) + " to " + (i + 1) * 25);
            int start = testViews.size();
            for (int j = 0; j < 25; j++) {
                testViews.add(views.get(start + j));
            }
            long then = System.currentTimeMillis();
            testViews.forEach(view -> view.getService().start(Duration.ofMillis(10), seeds, scheduler));

            boolean stabilized = Utils.waitForCondition(20_000, 1_000, () -> {
                return testViews.stream().filter(view -> view.getLive().size() != testViews.size()).count() == 0;
            });

            assertTrue(stabilized, "Views have not reached: " + testViews.size() + " currently: "
            + testViews.stream()
                       .filter(e -> e.getLive().size() != testViews.size())
                       .map(v -> String.format("%s : %s", v.getContext().getId(),
                                               v.getContext().getOffline().stream().map(p -> p.getId()).toList()))
                       .toList());

            System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
            + testViews.size() + " members");
        }

        Graph<Participant> testGraph = new Graph<>();
        for (View v : views) {
            for (int i = 0; i < parameters.rings; i++) {
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
        ConsoleReporter.forRegistry(registry)
                       .convertRatesTo(TimeUnit.SECONDS)
                       .convertDurationsTo(TimeUnit.MILLISECONDS)
                       .build()
                       .report();
    }

    @Test
    public void swarm() throws Exception {
        initialize();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);

        long then = System.currentTimeMillis();
        views.forEach(view -> view.getService().start(Duration.ofMillis(100), seeds, scheduler));

        assertTrue(Utils.waitForCondition(15_000, 1_000, () -> {
            return views.stream().filter(view -> view.getLive().size() != views.size()).count() == 0;
        }));

        System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
        + views.size() + " members");

        Thread.sleep(5_000);

        for (int i = 0; i < parameters.rings; i++) {
            for (View view : views) {
                assertEquals(views.get(0).getContext().ring(i).getRing(), view.getContext().ring(i).getRing());
            }
        }

        List<View> invalid = views.stream()
                                  .map(view -> view.getLive().size() != views.size() ? view : null)
                                  .filter(view -> view != null)
                                  .collect(Collectors.toList());
        assertEquals(0, invalid.size());

        Graph<Participant> testGraph = new Graph<>();
        for (View v : views) {
            for (int i = 0; i < parameters.rings; i++) {
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

        views.forEach(view -> view.getService().stop());
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

        while (seeds.size() < parameters.toleranceLevel + 1) {
            CertificateWithPrivateKey cert = certs.get(members.get(entropy.nextInt(24)).getId());
            if (!seeds.contains(cert.getX509Certificate())) {
                seeds.add(cert.getX509Certificate());
            }
        }

        AtomicBoolean frist = new AtomicBoolean(true);
        final var prefix = UUID.randomUUID().toString();
        views = members.stream().map(node -> {
            Context<Participant> context = Context.<Participant>newBuilder().setCardinality(CARDINALITY).build();
            FireflyMetricsImpl fireflyMetricsImpl = new FireflyMetricsImpl(context.getId(),
                                                                           frist.getAndSet(false) ? node0Registry
                                                                                                  : registry);
            Router comms = new LocalRouter(prefix, node,
                                           ServerConnectionCache.newBuilder()
                                                                .setTarget(2)
                                                                .setMetrics(new ServerConnectionCacheMetricsImpl(frist.getAndSet(false) ? node0Registry
                                                                                                                                        : registry)),
                                           Executors.newFixedThreadPool(3));
            comms.start();
            communications.add(comms);
            return new View(context, node, comms, fireflyMetricsImpl);
        }).collect(Collectors.toList());
    }
}
