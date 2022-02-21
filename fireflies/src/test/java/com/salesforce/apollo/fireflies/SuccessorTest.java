/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.codahale.metrics.MetricRegistry;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.comm.ServerConnectionCacheMetricsImpl;
import com.salesforce.apollo.comm.ServerConnectionCache.Builder;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.Signer.SignerImpl;
import com.salesforce.apollo.crypto.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.crypto.ssl.CertificateValidator;
import com.salesforce.apollo.fireflies.View.Service;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.Ring;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class SuccessorTest {

    private static Map<Digest, CertificateWithPrivateKey> certs;
    private static final FirefliesParameters              parameters;
    private static final int                              CARDINALITY = 10;

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

    private final List<Router> communications = new ArrayList<>();

    @AfterEach
    public void after() {
        communications.forEach(e -> e.close());
        communications.clear();
    }

    @Test
    public void allSuccessors() throws Exception {
        Random entropy = new Random(0x666);
        MetricRegistry registry = new MetricRegistry();
        FireflyMetrics metrics = new FireflyMetricsImpl(DigestAlgorithm.DEFAULT.getOrigin(), registry);

        List<X509Certificate> seeds = new ArrayList<>();
        List<Node> members = certs.values()
                                  .stream()
                                  .map(cert -> new Node(new SigningMemberImpl(Member.getMemberIdentifier(cert.getX509Certificate()),
                                                                              cert.getX509Certificate(),
                                                                              cert.getPrivateKey(),
                                                                              new SignerImpl(cert.getPrivateKey()),
                                                                              cert.getX509Certificate().getPublicKey()),
                                                        cert, parameters))
                                  .collect(Collectors.toList());
        assertEquals(certs.size(), members.size());

        while (seeds.size() < parameters.toleranceLevel + 1) {
            CertificateWithPrivateKey cert = certs.get(members.get(entropy.nextInt(members.size())).getId());
            if (!seeds.contains(cert.getX509Certificate())) {
                seeds.add(cert.getX509Certificate());
            }
        }

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(members.size());

        Builder builder = ServerConnectionCache.newBuilder()
                                               .setTarget(30)
                                               .setMetrics(new ServerConnectionCacheMetricsImpl(registry));
        ForkJoinPool executor = new ForkJoinPool();
        final var prefix = UUID.randomUUID().toString();
        Map<Participant, View> views = members.stream().map(node -> {
            LocalRouter comms = new LocalRouter(prefix, node, builder, executor);
            communications.add(comms);
            comms.start();
            Context<Participant> context = Context.<Participant>newBuilder().setCardinality(CARDINALITY).build();
            return new View(context, node, comms, metrics);
        }).collect(Collectors.toMap(v -> v.getNode(), v -> v));

        views.values().forEach(view -> view.getService().start(Duration.ofMillis(10), seeds, scheduler));

        try {
            Utils.waitForCondition(15_000, 1_000, () -> {
                return views.values()
                            .stream()
                            .map(view -> view.getLive().size() != views.size() ? view : null)
                            .filter(view -> view != null)
                            .count() == 0;
            });

            for (View view : views.values()) {
                for (Participant m : view.getContext().allMembers().toList()) {
                    assertTrue(m.getEpoch() > 0, "Participant epoch <= 0: " + m);
                }
                for (int r = 0; r < parameters.rings; r++) {
                    Ring<Participant> ring = view.getRing(r);
                    Participant successor = ring.successor(view.getNode());
                    View successorView = views.get(successor);
                    Participant test = successorView.getRing(r).successor(view.getNode());
                    assertEquals(successor, test);
                }
            }

            View test = views.get(members.get(0));
            System.out.println("Test member: " + test.getNode());
            Field lastRing = Service.class.getDeclaredField("lastRing");
            lastRing.setAccessible(true);
            int ring = (lastRing.getInt(test.getService()) + 1) % test.getRings().size();
            Participant successor = test.getRing(ring).successor(test.getNode(), m -> test.getContext().isActive(m));
            System.out.println("ring: " + ring + " successor: " + successor);
            assertEquals(successor,
                         views.get(successor)
                              .getRing(ring)
                              .successor(test.getNode(), m -> test.getContext().isActive(m)));
            assertTrue(test.getContext().isActive(successor));
            test.getService().gossip(() -> {
            });

            ring = (ring + 1) % test.getRings().size();
            successor = test.getRing(ring).successor(test.getNode(), m -> test.getContext().isActive(m));
            System.out.println("ring: " + ring + " successor: " + successor);
            assertEquals(successor,
                         views.get(successor)
                              .getRing(ring)
                              .successor(test.getNode(), m -> test.getContext().isActive(m)));
            assertTrue(test.getContext().isActive(successor));
            test.getService().gossip(null);
        } finally {
            views.values().forEach(e -> e.getService().stop());
        }
    }
}
