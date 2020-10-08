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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.comm.LocalCommSimm;
import com.salesforce.apollo.membership.CertWithKey;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.Utils;

import io.github.olivierlemasle.ca.RootCertificate;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class SwarmTest {

    private static final RootCertificate     ca         = getCa();
    private static Map<HashKey, CertWithKey> certs;
    private static final FirefliesParameters parameters = new FirefliesParameters(ca.getX509Certificate());

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(1, 101)
                         .parallel()
                         .mapToObj(i -> getMember(i))
                         .collect(Collectors.toMap(cert -> Member.getMemberId(cert.getCertificate()), cert -> cert));
    }

    private List<Node>            members;
    private List<View>            views;
    private LocalCommSimm         communications;
    private List<X509Certificate> seeds;

    @AfterEach
    public void after() {
        if (views != null) {
            views.forEach(v -> v.getService().stop());
        }
        communications.close();
    }

    @Test
    public void churn() throws Exception {
        initialize();

        List<View> testViews = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            int start = testViews.size();
            for (int j = 0; j < 25; j++) {
                testViews.add(views.get(start + j));
            }
            long then = System.currentTimeMillis();
            testViews.forEach(view -> view.getService().start(Duration.ofMillis(100), seeds));

            assertTrue(Utils.waitForCondition(15_000, 1_000, () -> {
                return testViews.stream().filter(view -> view.getLive().size() != testViews.size()).count() == 0;
            }), "views: " + testViews.stream().map(view -> view.getLive().size()).collect(Collectors.toList()));

            System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
                    + testViews.size() + " members");
        }
        System.out.println("Stopping views");
        testViews.forEach(e -> e.getService().stop());
        testViews.clear();
//        communications.close();
        for (int i = 0; i < 4; i++) {
            int start = testViews.size();
            for (int j = 0; j < 25; j++) {
                testViews.add(views.get(start + j));
            }
            long then = System.currentTimeMillis();
            testViews.forEach(view -> view.getService().start(Duration.ofMillis(10), seeds));

            boolean stabilized = Utils.waitForCondition(15_000, 1_000, () -> {
                return testViews.stream().filter(view -> view.getLive().size() != testViews.size()).count() == 0;
            });

            assertTrue(stabilized);

            System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
                    + testViews.size() + " members");
        }

        Graph<Participant> testGraph = new Graph<>();
        for (View v : views) {
            for (int i = 0; i < parameters.rings; i++) {
                testGraph.addEdge(v.getNode(), v.getRing(i).successor(v.getNode()));
            }
        }
        assertTrue(testGraph.isSC());

        for (View view : views) {
            for (int ring = 0; ring < view.getRings().size(); ring++) {
                final Collection<Participant> membership = view.getRing(ring).members();
                for (Node node : members) {
                    assertTrue(membership.contains(node));
                }
            }
        }
    }

    @Test
    public void swarm() throws Exception {
        initialize();

        long then = System.currentTimeMillis();
        views.forEach(view -> view.getService().start(Duration.ofMillis(100), seeds));

        assertTrue(Utils.waitForCondition(15_000, 1_000, () -> {
            return views.stream().filter(view -> view.getLive().size() != views.size()).count() == 0;
        }));

        System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
                + views.size() + " members");

        Thread.sleep(5_000);

        for (int i = 0; i < parameters.rings; i++) {
            for (View view : views) {
                assertEquals(views.get(0).getRing(i).getRing(), view.getRing(i).getRing());
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
                testGraph.addEdge(v.getNode(), v.getRing(i).successor(v.getNode()));
            }
        }
        assertTrue(testGraph.isSC());

        for (View view : views) {
            for (int ring = 0; ring < view.getRings().size(); ring++) {
                final Collection<Participant> membership = view.getRing(ring).members();
                for (Node node : members) {
                    assertTrue(membership.contains(node));
                }
            }
        }

        views.forEach(view -> view.getService().stop());
    }

    private void initialize() {
        Random entropy = new Random(0x666);

        seeds = new ArrayList<>();
        members = certs.values()
                       .parallelStream()
                       .map(cert -> new CertWithKey(cert.getCertificate(), cert.getPrivateKey()))
                       .map(cert -> new Node(cert, parameters))
                       .collect(Collectors.toList());
        communications = new LocalCommSimm();
        assertEquals(certs.size(), members.size());

        while (seeds.size() < parameters.toleranceLevel + 1) {
            CertWithKey cert = certs.get(members.get(entropy.nextInt(24)).getId());
            if (!seeds.contains(cert.getCertificate())) {
                seeds.add(cert.getCertificate());
            }
        }

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(members.size());

        views = members.stream().map(node -> new View(node, communications, scheduler)).collect(Collectors.toList());
    }
}
