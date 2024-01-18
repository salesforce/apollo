/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.salesforce.apollo.archipelago.LocalServer;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.ServerConnectionCache;
import com.salesforce.apollo.archipelago.ServerConnectionCacheMetricsImpl;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.fireflies.View.Participant;
import com.salesforce.apollo.fireflies.View.Seed;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.*;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.utils.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author hal.hildebrand
 */
public class ChurnTest {

    private static final int                                                         CARDINALITY    = 100;
    private static final double                                                      P_BYZ          = 0.3;
    private static       Map<Digest, ControlledIdentifier<SelfAddressingIdentifier>> identities;
    private static       KERL.AppendKERL                                             kerl;
    private final        List<Router>                                                communications = new ArrayList<>();
    private final        List<Router>                                                gateways       = new ArrayList<>();
    private              Map<Digest, ControlledIdentifierMember>                     members;
    private              MetricRegistry                                              node0Registry;
    private              MetricRegistry                                              registry;
    private              List<View>                                                  views;

    @BeforeAll
    public static void beforeClass() throws Exception {
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        kerl = new MemKERL(DigestAlgorithm.DEFAULT).cached();
        var stereotomy = new StereotomyImpl(new MemKeyStore(), kerl, entropy);
        identities = IntStream.range(0, CARDINALITY)
                              .mapToObj(i -> {
                                  return stereotomy.newIdentifier();
                              })
                              .collect(Collectors.toMap(controlled -> controlled.getIdentifier().getDigest(),
                                                        controlled -> controlled, (a, b) -> a, TreeMap::new));
    }

    @AfterEach
    public void after() {
        if (views != null) {
            views.forEach(v -> v.stop());
            views.clear();
        }

        communications.forEach(e -> e.close(Duration.ofSeconds(1)));
        communications.clear();

        gateways.forEach(e -> e.close(Duration.ofSeconds(1)));
        gateways.clear();
    }

    @Test
    public void churn() throws Exception {
        initialize();

        Set<View> testViews = new HashSet<>();

        System.out.println();
        System.out.println("Starting views");
        System.out.println();

        var seeds = members.values()
                           .stream()
                           .map(m -> new Seed(m.getIdentifier().getIdentifier(), new InetSocketAddress(0)))
                           .limit(25)
                           .toList();

        // Bootstrap the kernel

        final var bootstrapSeed = seeds.subList(0, 1);

        final var gossipDuration = Duration.ofMillis(5);
        var countdown = new AtomicReference<>(new CountDownLatch(1));
        long then = System.currentTimeMillis();

        views.get(0).start(() -> countdown.get().countDown(), gossipDuration, Collections.emptyList());

        assertTrue(countdown.get().await(30, TimeUnit.SECONDS), "Kernel did not bootstrap");

        testViews.add(views.get(0));

        var bootstrappers = views.subList(1, seeds.size());
        countdown.set(new CountDownLatch(bootstrappers.size()));

        bootstrappers.forEach(v -> v.start(() -> countdown.get().countDown(), gossipDuration, bootstrapSeed));

        // Test that all seeds up
        var success = countdown.get().await(30, TimeUnit.SECONDS);
        testViews.addAll(bootstrappers);

        var failed = testViews.stream()
                              .filter(e -> e.getContext().activeCount() != testViews.size())
                              .map(v -> String.format("%s : %s ", v.getNode().getId(), v.getContext().activeCount()))
                              .toList();
        assertTrue(success, " expected: " + testViews.size() + " failed: " + failed.size() + " views: " + failed);

        System.out.println(
        "Seeds have stabilized in " + (System.currentTimeMillis() - then) + " Ms across all " + testViews.size()
        + " members");

        // Bring up the remaining members stepwise
        for (int i = 0; i < 3; i++) {
            int start = testViews.size();
            var toStart = new ArrayList<View>();
            for (int j = 0; j < 25; j++) {
                final var v = views.get(start + j);
                testViews.add(v);
                toStart.add(v);
            }
            then = System.currentTimeMillis();
            countdown.set(new CountDownLatch(toStart.size()));

            toStart.forEach(view -> view.start(() -> countdown.get().countDown(), gossipDuration, seeds));

            success = countdown.get().await(30, TimeUnit.SECONDS);
            failed = testViews.stream()
                              .filter(e -> e.getContext().activeCount() != testViews.size()
                              || e.getContext().totalCount() != testViews.size())
                              .sorted(Comparator.comparing(v -> v.getContext().activeCount()))
                              .map(v -> String.format("%s : %s : %s ", v.getNode().getId(), v.getContext().totalCount(),
                                                      v.getContext().activeCount()))
                              .toList();
            assertTrue(success, " expected: " + testViews.size() + " failed: " + failed.size() + " views: " + failed);

            success = Utils.waitForCondition(30_000, 1_000, () -> {
                return testViews.stream()
                                .map(v -> v.getContext())
                                .filter(
                                ctx -> ctx.totalCount() != testViews.size() || ctx.activeCount() != testViews.size())
                                .count() == 0;
            });
            failed = testViews.stream()
                              .filter(e -> e.getContext().activeCount() != testViews.size()
                              || e.getContext().totalCount() != testViews.size())
                              .sorted(Comparator.comparing(v -> v.getContext().activeCount()))
                              .map(v -> String.format("%s : %s : %s ", v.getNode().getId(), v.getContext().totalCount(),
                                                      v.getContext().activeCount()))
                              .toList();
            assertTrue(success, " expected: " + testViews.size() + " failed: " + failed.size() + " views: " + failed);

            success = Utils.waitForCondition(30_000, 1_000, () -> {
                return testViews.stream()
                                .map(v -> v.getContext())
                                .filter(
                                ctx -> ctx.totalCount() != testViews.size() || ctx.activeCount() != testViews.size())
                                .count() == 0;
            });
            failed = testViews.stream()
                              .filter(e -> e.getContext().activeCount() != testViews.size()
                              || e.getContext().totalCount() != testViews.size())
                              .sorted(Comparator.comparing(v -> v.getContext().activeCount()))
                              .map(v -> String.format("%s : %s : %s ", v.getNode().getId(), v.getContext().totalCount(),
                                                      v.getContext().activeCount()))
                              .toList();
            assertTrue(success, " expected: " + testViews.size() + " failed: " + failed.size() + " views: " + failed);

            System.out.println(
            "View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all " + testViews.size()
            + " members");
        }
        System.out.println();
        System.out.println("Stopping views");
        System.out.println();

        testViews.clear();
        List<View> c = new ArrayList<>(views);
        List<Router> r = new ArrayList<>(communications);
        List<Router> g = new ArrayList<>(gateways);
        int delta = 5;
        for (int i = 0; i < (CARDINALITY / delta - 4); i++) {
            var removed = new ArrayList<Digest>();
            for (int j = c.size() - 1; j >= c.size() - delta; j--) {
                final var view = c.get(j);
                view.stop();
                r.get(j).close(Duration.ofSeconds(1));
                g.get(j).close(Duration.ofSeconds(1));
                removed.add(view.getNode().getId());
            }
            c = c.subList(0, c.size() - delta);
            r = r.subList(0, r.size() - delta);
            g = g.subList(0, g.size() - delta);
            final var expected = c;
            //            System.out.println("** Removed: " + removed);
            then = System.currentTimeMillis();
            success = Utils.waitForCondition(60_000, 1_000, () -> {
                return expected.stream().filter(view -> view.getContext().totalCount() > expected.size()).count() == 0;
            });
            failed = expected.stream()
                             .filter(e -> e.getContext().activeCount() != testViews.size())
                             .sorted(Comparator.comparing(v -> v.getContext().activeCount()))
                             .map(v -> String.format("%s : %s ", v.getNode().getId(), v.getContext().activeCount()))
                             .toList();
            assertTrue(success, " expected: " + expected.size() + " failed: " + failed.size() + " views: " + failed);

            System.out.println(
            "View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all " + c.size()
            + " members");
        }

        views.forEach(e -> e.stop());
        communications.forEach(e -> e.close(Duration.ofSeconds(1)));

        System.out.println();

        for (View v : views) {
            Graph<Participant> testGraph = new Graph<>();
            for (int i = 0; i < v.getContext().getRingCount(); i++) {
                testGraph.addEdge(v.getNode(), v.getContext().ring(i).successor(v.getNode()));
            }
            assertTrue(testGraph.isSC());
        }

        System.out.println("Node 0 metrics");
        ConsoleReporter.forRegistry(node0Registry)
                       .convertRatesTo(TimeUnit.SECONDS)
                       .convertDurationsTo(TimeUnit.MILLISECONDS)
                       .build()
                       .report();
    }

    private void initialize() {
        var parameters = Parameters.newBuilder().setMaximumTxfr(20).build();
        registry = new MetricRegistry();
        node0Registry = new MetricRegistry();

        members = identities.values()
                            .stream()
                            .map(identity -> new ControlledIdentifierMember(identity))
                            .collect(Collectors.toMap(m -> m.getId(), m -> m));
        var ctxBuilder = Context.<Participant>newBuilder().setpByz(P_BYZ).setCardinality(CARDINALITY);

        AtomicBoolean frist = new AtomicBoolean(true);
        final var prefix = UUID.randomUUID().toString();
        final var gatewayPrefix = UUID.randomUUID().toString();
        views = members.values().stream().map(node -> {
            Context<Participant> context = ctxBuilder.build();
            FireflyMetricsImpl metrics = new FireflyMetricsImpl(context.getId(),
                                                                frist.getAndSet(false) ? node0Registry : registry);
            var comms = new LocalServer(prefix, node).router(ServerConnectionCache.newBuilder()
                                                                                  .setTarget(200)
                                                                                  .setMetrics(
                                                                                  new ServerConnectionCacheMetricsImpl(
                                                                                  frist.getAndSet(false) ? node0Registry
                                                                                                         : registry)));
            var gateway = new LocalServer(gatewayPrefix, node).router(ServerConnectionCache.newBuilder()
                                                                                           .setTarget(200)
                                                                                           .setMetrics(
                                                                                           new ServerConnectionCacheMetricsImpl(
                                                                                           frist.getAndSet(false)
                                                                                           ? node0Registry
                                                                                           : registry)));
            comms.start();
            communications.add(comms);

            gateway.start();
            gateways.add(comms);
            return new View(context, node, new InetSocketAddress(0), EventValidation.NONE, Verifiers.from(kerl), comms,
                            parameters, gateway, DigestAlgorithm.DEFAULT, metrics);
        }).collect(Collectors.toList());
    }
}
