/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.salesforce.apollo.archipelago.LocalServer;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.ServerConnectionCache;
import com.salesforce.apollo.archipelago.ServerConnectionCacheMetricsImpl;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.fireflies.View.Participant;
import com.salesforce.apollo.fireflies.View.Seed;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.EventValidation;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class SwarmTest {

    private static final int                                                   BIAS       = 3;
    private static final int                                                   CARDINALITY;
    private static Map<Digest, ControlledIdentifier<SelfAddressingIdentifier>> identities;
    private static boolean                                                     largeTests = Boolean.getBoolean("large_tests");
    private static final double                                                P_BYZ      = 0.1;

    static {
        CARDINALITY = largeTests ? 3000 : 100;
    }

    @BeforeAll
    public static void beforeClass() throws Exception {
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);
        identities = IntStream.range(0, CARDINALITY).mapToObj(i -> {
            try {
                return stereotomy.newIdentifier().get();
            } catch (InterruptedException | ExecutionException e) {
                throw new IllegalStateException(e);
            }
        })
                              .collect(Collectors.toMap(controlled -> controlled.getIdentifier().getDigest(),
                                                        controlled -> controlled, (a, b) -> a, TreeMap::new));
    }

    private List<Router>                            communications = new ArrayList<>();
    private List<Router>                            gateways       = new ArrayList<>();
    private Map<Digest, ControlledIdentifierMember> members;
    private MetricRegistry                          node0Registry;
    private MetricRegistry                          registry;
    private List<View>                              views;

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
    public void swarm() throws Exception {
        initialize();
        final var scheduler = Executors.newScheduledThreadPool(2);
        long then = System.currentTimeMillis();

        // Bootstrap the kernel

        final var seeds = members.values()
                                 .stream()
                                 .map(m -> new Seed(m.getEvent().getCoordinates(), new InetSocketAddress(0)))
                                 .limit(largeTests ? 100 : 10)
                                 .toList();
        final var bootstrapSeed = seeds.subList(0, 1);

        final var gossipDuration = Duration.ofMillis(largeTests ? 70 : 5);

        var countdown = new AtomicReference<>(new CountDownLatch(1));
        views.get(0).start(() -> countdown.get().countDown(), gossipDuration, Collections.emptyList(), scheduler);

        assertTrue(countdown.get().await(largeTests ? 2400 : 30, TimeUnit.SECONDS), "Kernel did not bootstrap");

        var bootstrappers = views.subList(0, seeds.size());
        countdown.set(new CountDownLatch(seeds.size() - 1));
        bootstrappers.subList(1, bootstrappers.size())
                     .forEach(v -> v.start(() -> countdown.get().countDown(), gossipDuration, bootstrapSeed,
                                           scheduler));

        // Test that all bootstrappers up
        var success = countdown.get().await(largeTests ? 2400 : 30, TimeUnit.SECONDS);
        var failed = bootstrappers.stream()
                                  .filter(e -> e.getContext().activeCount() != bootstrappers.size())
                                  .map(v -> String.format("%s : %s ", v.getNode().getId(),
                                                          v.getContext().activeCount()))
                                  .toList();
        assertTrue(success, " expected: " + bootstrappers.size() + " failed: " + failed.size() + " views: " + failed);

        // Start remaining views
        countdown.set(new CountDownLatch(views.size() - seeds.size()));
        views.forEach(v -> v.start(() -> countdown.get().countDown(), gossipDuration, seeds, scheduler));

        success = countdown.get().await(largeTests ? 2400 : 30, TimeUnit.SECONDS);

        // Test that all views are up
        failed = views.stream()
                      .filter(e -> e.getContext().activeCount() != CARDINALITY)
                      .map(v -> String.format("%s : %s : %s ", v.getNode().getId(), v.getContext().activeCount(),
                                              v.getContext().totalCount()))
                      .toList();
        assertTrue(success, "Views did not start, expected: " + views.size() + " failed: " + failed.size() + " views: "
        + failed);

        success = Utils.waitForCondition(largeTests ? 2400_000 : 30, 1_000, () -> {
            return views.stream().filter(view -> view.getContext().activeCount() != CARDINALITY).count() == 0;
        });

        // Test that all views are up
        failed = views.stream()
                      .filter(e -> e.getContext().activeCount() != CARDINALITY)
                      .map(v -> String.format("%s : %s : %s ", v.getNode().getId(), v.getContext().activeCount(),
                                              v.getContext().totalCount()))
                      .toList();
        assertTrue(success, "Views did not stabilize, expected: " + views.size() + " failed: " + failed.size()
        + " views: " + failed);

        System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
        + views.size() + " members");

        if (!largeTests) {
            for (int i = 0; i < views.get(0).getContext().getRingCount(); i++) {
                final var reference = views.get(0).getContext().ring(i).getRing();
                for (View view : views) {
                    assertEquals(reference, view.getContext().ring(i).getRing());
                }
            }

            failed = views.stream()
                          .filter(e -> e.getContext().activeCount() != CARDINALITY)
                          .map(v -> String.format("%s : %s ", v.getNode().getId(), v.getContext().activeCount()))
                          .toList();
            assertEquals(0, failed.size(),
                         " expected: " + views.size() + " failed: " + failed.size() + " views: " + failed);

            for (View v : views) {
                Graph<Participant> testGraph = new Graph<>();
                for (int i = 0; i < views.get(0).getContext().getRingCount(); i++) {
                    testGraph.addEdge(v.getNode(), v.getContext().ring(i).successor(v.getNode()));
                }
                assertTrue(testGraph.isSC());
            }
        }
        communications.forEach(e -> e.close(Duration.ofSeconds(1)));
        views.forEach(view -> view.stop());
        System.out.println("Node 0 metrics");
        ConsoleReporter.forRegistry(node0Registry)
                       .convertRatesTo(TimeUnit.SECONDS)
                       .convertDurationsTo(TimeUnit.MILLISECONDS)
                       .build()
                       .report();
    }

    private void initialize() {
        var parameters = Parameters.newBuilder()
                                   .setMaxPending(largeTests ? 10 : 10)
                                   .setMaximumTxfr(largeTests ? 100 : 20)
                                   .build();
        registry = new MetricRegistry();
        node0Registry = new MetricRegistry();

        members = identities.values()
                            .stream()
                            .map(identity -> new ControlledIdentifierMember(identity))
                            .collect(Collectors.toMap(m -> m.getId(), m -> m));
        var ctxBuilder = Context.<Participant>newBuilder().setBias(BIAS).setpByz(P_BYZ).setCardinality(CARDINALITY);

        AtomicBoolean frist = new AtomicBoolean(true);
        final var prefix = UUID.randomUUID().toString();
        final var gatewayPrefix = UUID.randomUUID().toString();
        final var executor = new ForkJoinPool(ForkJoinPool.getCommonPoolParallelism() * 2);
        final var commExec = new ForkJoinPool(ForkJoinPool.getCommonPoolParallelism());
        final var gatewayExec = ForkJoinPool.commonPool();
        views = members.values().stream().map(node -> {
            Context<Participant> context = ctxBuilder.build();
            FireflyMetricsImpl metrics = new FireflyMetricsImpl(context.getId(),
                                                                frist.getAndSet(false) ? node0Registry : registry);
            var comms = new LocalServer(prefix, node,
                                        commExec).router(ServerConnectionCache.newBuilder().setTarget(200).setMetrics(new ServerConnectionCacheMetricsImpl(frist.getAndSet(false) ? node0Registry : registry)), commExec);
            var gateway = new LocalServer(gatewayPrefix, node,
                                          gatewayExec).router(ServerConnectionCache.newBuilder().setTarget(200).setMetrics(new ServerConnectionCacheMetricsImpl(frist.getAndSet(false) ? node0Registry : registry)), gatewayExec);
            comms.start();
            communications.add(comms);

            gateway.start();
            gateways.add(comms);
            return new View(context, node, new InetSocketAddress(0), EventValidation.NONE, comms, parameters, gateway,
                            DigestAlgorithm.DEFAULT, metrics, executor);
        }).collect(Collectors.toList());
    }
}
