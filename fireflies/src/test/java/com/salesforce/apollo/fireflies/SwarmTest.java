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
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
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

    private static final int                                                   CARDINALITY = 100;
    private static Map<Digest, ControlledIdentifier<SelfAddressingIdentifier>> identities;
    private static final double                                                P_BYZ       = 0.3;

    @BeforeAll
    public static void beforeClass() throws Exception {
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);
        identities = IntStream.range(0, CARDINALITY)
                              .mapToObj(i -> stereotomy.newIdentifier().get())
                              .collect(Collectors.toMap(controlled -> controlled.getIdentifier().getDigest(),
                                                        controlled -> controlled, (a, b) -> a, TreeMap::new));
    }

    private List<Router>                            communications = new ArrayList<>();
    private Map<Digest, ControlledIdentifierMember> members;
    private MetricRegistry                          node0Registry;
    private MetricRegistry                          registry;
    private List<Seed>                              seeds;
    private List<View>                              views;

    @AfterEach
    public void after() {
        if (views != null) {
            views.forEach(v -> v.stop());
            views.clear();
        }

        communications.forEach(e -> e.close());
        communications.clear();
    }

    @Test
    public void swarm() throws Exception {
        initialize();
        final var scheduler = Executors.newScheduledThreadPool(20);
        long then = System.currentTimeMillis();

        // Bootstrap the kernel

        final var bootstrapSeed = seeds.subList(0, 1);

        final var gossipDuration = Duration.ofMillis(25);
        views.get(0).start(gossipDuration, Collections.emptyList(), scheduler);

        var bootstrappers = views.subList(0, 25);
        bootstrappers.forEach(v -> v.start(gossipDuration, bootstrapSeed, scheduler));

        // Test that all bootstrappers up
        var success = Utils.waitForCondition(20_000, 1_000, () -> {
            return bootstrappers.stream()
                                .filter(view -> view.getContext().activeCount() != bootstrappers.size())
                                .count() == 0;
        });
        var failed = bootstrappers.stream()
                                  .filter(e -> e.getContext().activeCount() != bootstrappers.size())
                                  .map(v -> String.format("%s : %s ", v.getNode().getId(),
                                                          v.getContext().activeCount()))
                                  .toList();
        assertTrue(success, " expected: " + bootstrappers.size() + " failed: " + failed.size() + " views: " + failed);

        // Start remaining views
        views.forEach(v -> v.start(gossipDuration, seeds, scheduler));
        success = Utils.waitForCondition(60_000, 1_000, () -> {
            return views.stream().filter(view -> view.getContext().activeCount() != CARDINALITY).count() == 0;
        });

        // Test that all views are up
        failed = views.stream()
                      .filter(e -> e.getContext().activeCount() != CARDINALITY)
                      .map(v -> String.format("%s : %s ", v.getNode().getId(), v.getContext().activeCount()))
                      .toList();
        assertTrue(success, " expected: " + views.size() + " failed: " + failed.size() + " views: " + failed);

        System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
        + views.size() + " members");

        Thread.sleep(5_000);

        for (int i = 0; i < views.get(0).getContext().getRingCount(); i++) {
            for (View view : views) {
                assertEquals(views.get(0).getContext().ring(i).getRing(), view.getContext().ring(i).getRing());
            }
        }

//        failed = views.stream()
//                      .filter(e -> e.getContext().activeCount() != CARDINALITY)
//                      .map(v -> String.format("%s : %s ", v.getNode().getId(), v.getContext().activeCount()))
//                      .toList();
//        assertEquals(0, failed.size(),
//                     " expected: " + views.size() + " failed: " + failed.size() + " views: " + failed);

        for (View v : views) {
            Graph<Participant> testGraph = new Graph<>();
            for (int i = 0; i < views.get(0).getContext().getRingCount(); i++) {
                testGraph.addEdge(v.getNode(), v.getContext().ring(i).successor(v.getNode()));
            }
            assertTrue(testGraph.isSC());
        }

        for (View view : views) {
            for (int ring = 0; ring < view.getContext().getRingCount(); ring++) {
                final var deRing = view.getContext().ring(ring);
                for (var node : members.values()) {
                    assertTrue(deRing.contains(node.getId()));
                }
            }
        }

        views.forEach(view -> view.stop());
        System.out.println("Node 0 metrics");
        ConsoleReporter.forRegistry(node0Registry)
                       .convertRatesTo(TimeUnit.SECONDS)
                       .convertDurationsTo(TimeUnit.MILLISECONDS)
                       .build()
                       .report();
    }

    private void initialize() {
        var parameters = Parameters.newBuilder().build();
        registry = new MetricRegistry();
        node0Registry = new MetricRegistry();

        seeds = new ArrayList<>();
        members = identities.values()
                            .stream()
                            .map(identity -> new ControlledIdentifierMember(identity))
                            .collect(Collectors.toMap(m -> m.getId(), m -> m));
        var ctxBuilder = Context.<Participant>newBuilder().setpByz(P_BYZ).setCardinality(CARDINALITY);

        seeds = members.values()
                       .stream()
                       .map(m -> new Seed(m.getEvent().getCoordinates(), new InetSocketAddress(0)))
                       .limit(24)
                       .toList();
        var commExec = ForkJoinPool.commonPool();
        var viewExec = commExec;
        AtomicBoolean frist = new AtomicBoolean(true);
        final var prefix = UUID.randomUUID().toString();
        views = members.values().stream().map(node -> {
            Context<Participant> context = ctxBuilder.build();
            FireflyMetricsImpl metrics = new FireflyMetricsImpl(context.getId(),
                                                                frist.getAndSet(false) ? node0Registry : registry);
            var comms = new LocalRouter(prefix,
                                        ServerConnectionCache.newBuilder()
                                                             .setTarget(2)
                                                             .setMetrics(new ServerConnectionCacheMetricsImpl(frist.getAndSet(false) ? node0Registry
                                                                                                                                     : registry)),
                                        commExec, metrics.limitsMetrics());
            comms.setMember(node);
            comms.start();
            communications.add(comms);
            return new View(context, node, new InetSocketAddress(0), EventValidation.NONE, comms, parameters,
                            DigestAlgorithm.DEFAULT, metrics, viewExec);
        }).collect(Collectors.toList());
    }
}
