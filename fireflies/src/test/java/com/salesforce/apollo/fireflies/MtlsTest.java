/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.SocketAddress;
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
import java.util.function.Function;
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
import com.salesforce.apollo.comm.ServerConnectionCacheMetricsImpl;
import com.salesforce.apollo.comm.StandardEpProvider;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.Signer.SignerImpl;
import com.salesforce.apollo.crypto.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.crypto.ssl.CertificateValidator;
import com.salesforce.apollo.fireflies.View.IdentityWrapper;
import com.salesforce.apollo.fireflies.View.Participant;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.stereotomy.services.EventValidation;
import com.salesforce.apollo.utils.Utils;

import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class MtlsTest {
    private static final int                    CARDINALITY;
    private static Map<Digest, IdentityWrapper> certs;
    private static final boolean                LARGE_TESTS = Boolean.getBoolean("large_tests");
    static {
//        ProviderUtils.getProviderBC();
        CARDINALITY = LARGE_TESTS ? 100 : 10;
    }

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(0, CARDINALITY)
                         .parallel()
                         .mapToObj(i -> Utils.getMember(i))
                         .collect(Collectors.toMap(cert -> Member.getMemberIdentifier(cert.getX509Certificate()),
                                                   cert -> cert));
    }

    private List<Router> communications = new ArrayList<>();

    private List<View> views;

    @AfterEach
    public void after() {
        if (views != null) {
            views.forEach(e -> e.stop());
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

        List<IdentityWrapper> seeds = new ArrayList<>();
        List<SigningMember> members = certs.values()
                                           .stream()
                                           .map(cert -> new SigningMemberImpl(Member.getMemberIdentifier(cert.getX509Certificate()),
                                                                              cert.getX509Certificate(),
                                                                              cert.getPrivateKey(),
                                                                              new SignerImpl(cert.getPrivateKey()),
                                                                              cert.getX509Certificate().getPublicKey()))
                                           .collect(Collectors.toList());
        assertEquals(certs.size(), members.size());

        var ctxBuilder = Context.<Participant>newBuilder().setCardinality(CARDINALITY);

        while (seeds.size() < ctxBuilder.build().getRingCount() + 1) {
            IdentityWrapper identity = certs.get(members.get(entropy.nextInt(members.size())).getId());
            if (!seeds.contains(identity)) {
                seeds.add(identity);
            }
        }

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(members.size());

        Builder builder = ServerConnectionCache.newBuilder().setTarget(2);
        AtomicBoolean frist = new AtomicBoolean(true);
        views = members.stream().map(node -> {
            Context<Participant> context = ctxBuilder.build();
            FireflyMetricsImpl metrics = new FireflyMetricsImpl(context.getId(),
                                                                frist.getAndSet(false) ? node0Registry : registry);
            Function<Member, SocketAddress> resolver = null;
            EndpointProvider ep = new StandardEpProvider(null, ClientAuth.REQUIRE, CertificateValidator.NONE, resolver);
            builder.setMetrics(new ServerConnectionCacheMetricsImpl(frist.getAndSet(false) ? node0Registry : registry));
            MtlsRouter comms = new MtlsRouter(builder, ep, node, Executors.newFixedThreadPool(3));
            communications.add(comms);
            return new View(context, node, EventValidation.NONE, comms, 0.0125, DigestAlgorithm.DEFAULT, metrics);
        }).collect(Collectors.toList());

        long then = System.currentTimeMillis();
        communications.forEach(e -> e.start());
        views.forEach(view -> view.start(Duration.ofMillis(200), seeds, scheduler));

        assertTrue(Utils.waitForCondition(60_000, 1_000, () -> {
            return views.stream()
                        .map(view -> view.getContext().getActive().size() != views.size() ? view : null)
                        .filter(view -> view != null)
                        .count() == 0;
        }), "view did not stabilize: "
        + views.stream().map(view -> view.getContext().getActive().size()).collect(Collectors.toList()));
        System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
        + views.size() + " members");

        System.out.println("Checking views for consistency");
        List<View> invalid = views.stream()
                                  .map(view -> view.getContext().getActive().size() != views.size() ? view : null)
                                  .filter(view -> view != null)
                                  .collect(Collectors.toList());
        assertEquals(0, invalid.size(), invalid.stream().map(view -> {
            Set<?> difference = Sets.difference(views.stream()
                                                     .map(v -> v.getNode().getId())
                                                     .collect(Collectors.toSet()),
                                                view.getContext()
                                                    .getActive()
                                                    .stream()
                                                    .map(m -> m.getId())
                                                    .collect(Collectors.toSet()));
            return "Invalid membership: " + view.getNode() + ", missing: " + difference.size();
        }).collect(Collectors.toList()).toString());

        System.out.println("Stoping views");
        views.forEach(view -> view.stop());

        System.out.println("Restarting views");
        views.forEach(view -> view.start(Duration.ofMillis(1000), seeds, scheduler));

        assertTrue(Utils.waitForCondition(30_000, 100, () -> {
            return views.stream()
                        .map(view -> view.getContext().getActive().size() != views.size() ? view : null)
                        .filter(view -> view != null)
                        .count() == 0;
        }));

        System.out.println("Stabilized, now sleeping to see if views remain stabilized");
        Thread.sleep(10_000);
        assertTrue(Utils.waitForCondition(30_000, 100, () -> {
            return views.stream()
                        .map(view -> view.getContext().getActive().size() != views.size() ? view : null)
                        .filter(view -> view != null)
                        .count() == 0;
        }));
        System.out.println("View has stabilized after restart in " + (System.currentTimeMillis() - then) + " Ms");

        System.out.println("Checking views for consistency");
        invalid = views.stream()
                       .map(view -> view.getContext().getActive().size() != views.size() ? view : null)
                       .filter(view -> view != null)
                       .collect(Collectors.toList());
        assertEquals(0, invalid.size());

        System.out.println("Stoping views");
        views.forEach(view -> view.stop());

        ConsoleReporter.forRegistry(node0Registry)
                       .convertRatesTo(TimeUnit.SECONDS)
                       .convertDurationsTo(TimeUnit.MILLISECONDS)
                       .build()
                       .report();
    }
}
