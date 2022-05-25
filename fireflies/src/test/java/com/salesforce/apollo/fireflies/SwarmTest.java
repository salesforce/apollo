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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executor;
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
import com.salesfoce.apollo.fireflies.proto.Identity;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.comm.ServerConnectionCacheMetricsImpl;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.fireflies.View.Participant;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.stereotomy.services.EventValidation;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class SwarmTest {

    private static Map<Digest, ControlledIdentifier<SelfAddressingIdentifier>> identities;
    private static final int                                                   CARDINALITY = 100;

    @BeforeAll
    public static void beforeClass() {
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT),
                                            new SecureRandom());
        identities = IntStream.range(0, CARDINALITY)
                              .parallel()
                              .mapToObj(i -> stereotomy.newIdentifier().get())
                              .map(ci -> {
                                  @SuppressWarnings("unchecked")
                                  var casted = (ControlledIdentifier<SelfAddressingIdentifier>) ci;
                                  return casted;
                              })
                              .collect(Collectors.toMap(controlled -> controlled.getIdentifier().getDigest(),
                                                        controlled -> controlled));
    }

    private Map<Digest, ControlledIdentifierMember> members;
    private List<View>                              views;
    private List<Router>                            communications = new ArrayList<>();
    private List<Identity>                          seeds;
    private MetricRegistry                          registry;
    private MetricRegistry                          node0Registry;

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
    public void churn() throws Exception {
        Executor exec = Executors.newFixedThreadPool(CARDINALITY);
        initialize(exec);

        List<View> testViews = new ArrayList<>();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);

        for (int i = 0; i < 4; i++) {
            int start = testViews.size();
            for (int j = 0; j < 25; j++) {
                testViews.add(views.get(start + j));
            }
            long then = System.currentTimeMillis();
            testViews.forEach(view -> view.start(exec, Duration.ofMillis(100), seeds, scheduler));

            assertTrue(Utils.waitForCondition(15_000, 1_000, () -> {
                return testViews.stream()
                                .filter(view -> view.getContext().getActive().size() != testViews.size())
                                .count() == 0;
            }), " size: " + testViews.size() + " views: "
            + testViews.stream()
                       .filter(e -> e.getContext().getActive().size() != testViews.size())
                       .map(v -> String.format("%s : %s", v.getContext().getId(),
                                               v.getContext().getOffline().stream().map(p -> p.getId()).toList()))
                       .toList());

            System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
            + testViews.size() + " members");
        }
        System.out.println("Stopping views");
        testViews.forEach(e -> e.stop());
        testViews.clear();
        communications.forEach(e -> e.close());

        communications.forEach(e -> e.start());
        for (int i = 0; i < 4; i++) {
            System.out.println("Restarting views " + (i * 25) + " to " + (i + 1) * 25);
            int start = testViews.size();
            for (int j = 0; j < 25; j++) {
                testViews.add(views.get(start + j));
            }
            long then = System.currentTimeMillis();
            testViews.forEach(view -> view.start(exec, Duration.ofMillis(10), seeds, scheduler));

            boolean stabilized = Utils.waitForCondition(20_000, 1_000, () -> {
                return testViews.stream()
                                .filter(view -> view.getContext().getActive().size() != testViews.size())
                                .count() == 0;
            });

            assertTrue(stabilized, "Views have not reached: " + testViews.size() + " currently: "
            + testViews.stream()
                       .filter(e -> e.getContext().getActive().size() != testViews.size())
                       .map(v -> String.format("%s : %s", v.getContext().getId(),
                                               v.getContext().getOffline().stream().map(p -> p.getId()).toList()))
                       .toList());

            System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
            + testViews.size() + " members");
        }

        Graph<Participant> testGraph = new Graph<>();
        for (View v : views) {
            for (int i = 0; i < v.getContext().getRingCount(); i++) {
                testGraph.addEdge(v.getNode(), v.getContext().ring(i).successor(v.getNode()));
            }
        }
        assertTrue(testGraph.isSC());

        for (View view : views) {
            for (int ring = 0; ring < view.getContext().getRingCount(); ring++) {
                final var membership = view.getContext()
                                           .ring(ring)
                                           .members()
                                           .stream()
                                           .map(p -> members.get(p.getId()))
                                           .toList();
                for (Member node : members.values()) {
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
        Executor exec = Executors.newFixedThreadPool(CARDINALITY);
        initialize(exec);
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);
        long then = System.currentTimeMillis();
        views.forEach(view -> view.start(exec, Duration.ofMillis(100), seeds, scheduler));

        assertTrue(Utils.waitForCondition(15_000, 1_000, () -> {
            return views.stream().filter(view -> view.getContext().getActive().size() != views.size()).count() == 0;
        }));

        System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
        + views.size() + " members");

        Thread.sleep(5_000);

        for (int i = 0; i < views.get(0).getContext().getRingCount(); i++) {
            for (View view : views) {
                assertEquals(views.get(0).getContext().ring(i).getRing(), view.getContext().ring(i).getRing());
            }
        }

        List<View> invalid = views.stream()
                                  .map(view -> view.getContext().getActive().size() != views.size() ? view : null)
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
                final var membership = view.getContext()
                                           .ring(ring)
                                           .members()
                                           .stream()
                                           .map(p -> members.get(p.getId()))
                                           .toList();
                for (Member node : members.values()) {
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

    private void initialize(Executor exec) {
        Random entropy = new Random(0x666);
        registry = new MetricRegistry();
        node0Registry = new MetricRegistry();

        seeds = new ArrayList<>();
        members = identities.values()
                            .stream()
                            .map(identity -> new ControlledIdentifierMember(identity))
                            .collect(Collectors.toMap(m -> m.getId(), m -> m));
        var ctxBuilder = Context.<Participant>newBuilder().setCardinality(CARDINALITY);

        var randomized = members.values().stream().collect(Collectors.toList());
        while (seeds.size() < ctxBuilder.build().getRingCount() + 1) {
            var id = View.identityFor(0, new InetSocketAddress(0), randomized.get(entropy.nextInt(24)).getEvent());
            if (!seeds.contains(id)) {
                seeds.add(id);
            }
        }

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
                                        exec, metrics.limitsMetrics());
            comms.setMember(node);
            comms.start();
            communications.add(comms);
            return new View(context, node, new InetSocketAddress(0), EventValidation.NONE, comms, 0.0125,
                            DigestAlgorithm.DEFAULT, metrics, exec);
        }).collect(Collectors.toList());
    }
}
