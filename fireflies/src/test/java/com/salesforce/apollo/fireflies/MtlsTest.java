/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import static com.salesforce.apollo.fireflies.View.getStandardEpProvider;
import static com.salesforce.apollo.test.pregen.PregenPopulation.getCa;
import static com.salesforce.apollo.test.pregen.PregenPopulation.getMember;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
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
import com.google.common.collect.Sets;
import com.salesforce.apollo.comm.EndpointProvider;
import com.salesforce.apollo.comm.MtlsRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.comm.ServerConnectionCache.Builder;
import com.salesforce.apollo.membership.CertWithKey;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.Utils;

import io.github.olivierlemasle.ca.CertificateWithPrivateKey;
import io.github.olivierlemasle.ca.RootCertificate;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class MtlsTest {

    private static final RootCertificate                   ca             = getCa();
    private static Map<HashKey, CertificateWithPrivateKey> certs;
    private static final FirefliesParameters               parameters     = new FirefliesParameters(
            ca.getX509Certificate());
    private List<Router>                                   communications = new ArrayList<>();
    private List<View>                                     views;

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(1, 101)
                         .parallel()
                         .mapToObj(i -> getMember(i))
                         .collect(Collectors.toMap(cert -> Member.getMemberId(cert.getX509Certificate()),
                                                   cert -> cert));
    }

    @AfterEach
    public void after() {
        if (views != null) {
            views.forEach(e -> e.getService().stop());
            views.clear();
        }
        if (communications != null) {
            communications.forEach(e -> e.close());
            communications.clear();
        }
    }

    @Test
    public void smoke() throws Exception {
        Random entropy = new Random(0x666);
        MetricRegistry registry = new MetricRegistry();
        MetricRegistry node0Registry = new MetricRegistry();

        List<X509Certificate> seeds = new ArrayList<>();
        List<Node> members = certs.values()
                                  .parallelStream()
                                  .map(cert -> new CertWithKey(cert.getX509Certificate(), cert.getPrivateKey()))
                                  .map(cert -> new Node(cert, parameters))
                                  .collect(Collectors.toList());
        assertEquals(certs.size(), members.size());

        while (seeds.size() < parameters.toleranceLevel + 1) {
            CertificateWithPrivateKey cert = certs.get(members.get(entropy.nextInt(members.size())).getId());
            if (!seeds.contains(cert.getX509Certificate())) {
                seeds.add(cert.getX509Certificate());
            }
        }

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(members.size());

        Builder builder = ServerConnectionCache.newBuilder().setTarget(2);
        AtomicBoolean frist = new AtomicBoolean(true);
        views = members.stream().map(node -> {
            FireflyMetricsImpl metrics = new FireflyMetricsImpl(frist.getAndSet(false) ? node0Registry : registry);
            EndpointProvider ep = getStandardEpProvider(node);
            builder.setMetrics(metrics);
            MtlsRouter comms = new MtlsRouter(builder, ep, Executors.newFixedThreadPool(3));
            communications.add(comms);
            return new View(HashKey.ORIGIN, node, comms, metrics);
        }).collect(Collectors.toList());

        long then = System.currentTimeMillis();
        communications.forEach(e -> e.start());
        views.forEach(view -> view.getService().start(Duration.ofMillis(500), seeds, scheduler));

        assertTrue(Utils.waitForCondition(60_000, 1_000, () -> {
            return views.stream()
                        .map(view -> view.getLive().size() != views.size() ? view : null)
                        .filter(view -> view != null)
                        .count() == 0;
        }), "view did not stabilize: "
                + views.stream().map(view -> view.getLive().size()).collect(Collectors.toList()));
        System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
                + views.size() + " members");

        System.out.println("Checking views for consistency");
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
                                                    .map(m -> m.getId())
                                                    .collect(Collectors.toSet()));
            return "Invalid membership: " + view.getNode() + ", missing: " + difference.size();
        }).collect(Collectors.toList()).toString());

        System.out.println("Stoping views");
        views.forEach(view -> view.getService().stop());

        System.out.println("Restarting views");
        views.forEach(view -> view.getService().start(Duration.ofMillis(1000), seeds, scheduler));

        assertTrue(Utils.waitForCondition(30_000, 100, () -> {
            return views.stream()
                        .map(view -> view.getLive().size() != views.size() ? view : null)
                        .filter(view -> view != null)
                        .count() == 0;
        }));

        System.out.println("Stabilized, now sleeping to see if views remain stabilized");
        Thread.sleep(10_000);
        assertTrue(Utils.waitForCondition(30_000, 100, () -> {
            return views.stream()
                        .map(view -> view.getLive().size() != views.size() ? view : null)
                        .filter(view -> view != null)
                        .count() == 0;
        }));
        System.out.println("View has stabilized after restart in " + (System.currentTimeMillis() - then) + " Ms");

        System.out.println("Checking views for consistency");
        invalid = views.stream()
                       .map(view -> view.getLive().size() != views.size() ? view : null)
                       .filter(view -> view != null)
                       .collect(Collectors.toList());
        assertEquals(0, invalid.size());

        System.out.println("Stoping views");
        views.forEach(view -> view.getService().stop());

        ConsoleReporter.forRegistry(node0Registry)
                       .convertRatesTo(TimeUnit.SECONDS)
                       .convertDurationsTo(TimeUnit.MILLISECONDS)
                       .build()
                       .report();
    }
}
