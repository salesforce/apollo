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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.codahale.metrics.MetricRegistry;
import com.salesfoce.apollo.fireflies.proto.Identity;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.comm.ServerConnectionCache.Builder;
import com.salesforce.apollo.comm.ServerConnectionCacheMetricsImpl;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.fireflies.View.Participant;
import com.salesforce.apollo.fireflies.View.Service;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Ring;
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
public class SuccessorTest {

    private static Map<Digest, ControlledIdentifier<SelfAddressingIdentifier>> identities;
    private static final int                                                   CARDINALITY = 10;

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

        List<Identity> seeds = new ArrayList<>();
        List<ControlledIdentifierMember> members = identities.values()
                                                             .stream()
                                                             .map(identity -> new ControlledIdentifierMember(identity))
                                                             .collect(Collectors.toList());
        assertEquals(identities.size(), members.size());
        var ctxBuilder = Context.<Participant>newBuilder().setCardinality(CARDINALITY);

        while (seeds.size() < ctxBuilder.build().getRingCount() + 1) {
            var id = View.identityFor(0, new InetSocketAddress(0),
                                      members.get(entropy.nextInt(members.size())).getEvent());
            if (!seeds.contains(id)) {
                seeds.add(id);
            }
        }

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(members.size());

        Builder builder = ServerConnectionCache.newBuilder()
                                               .setTarget(30)
                                               .setMetrics(new ServerConnectionCacheMetricsImpl(registry));
        Executor executor = Executors.newFixedThreadPool(CARDINALITY);
        final var prefix = UUID.randomUUID().toString();
        Map<Digest, View> views = members.stream().map(node -> {
            LocalRouter comms = new LocalRouter(prefix, builder, executor, metrics.limitsMetrics());
            communications.add(comms);
            comms.setMember(node);
            comms.start();
            Context<Participant> context = ctxBuilder.build();
            return new View(context, node, new InetSocketAddress(0), EventValidation.NONE, comms, 0.0125,
                            DigestAlgorithm.DEFAULT, metrics, executor);
        }).collect(Collectors.toMap(v -> v.getNode().getId(), v -> v));

        views.values().forEach(view -> view.start(Duration.ofMillis(10), seeds, scheduler));

        try {
            Utils.waitForCondition(15_000, 1_000, () -> {
                return views.values()
                            .stream()
                            .map(view -> view.getContext().getActive().size() != views.size() ? view : null)
                            .filter(view -> view != null)
                            .count() == 0;
            });

            for (View view : views.values()) {
                for (Participant m : view.getContext().allMembers().toList()) {
                    assertTrue(m.getEpoch() > 0, "Participant epoch <= 0: " + m);
                }
                for (int r = 0; r < view.getContext().getRingCount(); r++) {
                    Ring<Participant> ring = view.getContext().ring(r);
                    Participant successor = ring.successor(view.getNode());
                    View successorView = views.get(successor.getId());
                    Participant test = successorView.getContext().ring(r).successor(view.getNode());
                    assertEquals(successor, test);
                }
            }

            View test = views.get(members.get(0).getId());
            System.out.println("Test member: " + test.getNode());
            Field serviceF = View.class.getDeclaredField("service");
            serviceF.setAccessible(true);
            Service service = (Service) serviceF.get(test);
            Field lastRingF = Service.class.getDeclaredField("lastRing");
            lastRingF.setAccessible(true);
            int ring = (lastRingF.getInt(service) + 1) % test.getContext().getRingCount();
            Participant successor = test.getContext()
                                        .ring(ring)
                                        .successor(test.getNode(), m -> test.getContext().isActive(m));
            System.out.println("ring: " + ring + " successor: " + successor);
            assertEquals(successor,
                         views.get(successor.getId())
                              .getContext()
                              .ring(ring)
                              .successor(test.getNode(), m -> test.getContext().isActive(m)));
            assertTrue(test.getContext().isActive(successor));
            service.gossip(() -> {
            });

            ring = (ring + 1) % test.getContext().getRingCount();
            successor = test.getContext().ring(ring).successor(test.getNode(), m -> test.getContext().isActive(m));
            System.out.println("ring: " + ring + " successor: " + successor);
            assertEquals(successor,
                         views.get(successor.getId())
                              .getContext()
                              .ring(ring)
                              .successor(test.getNode(), m -> test.getContext().isActive(m)));
            assertTrue(test.getContext().isActive(successor));
            service.gossip(null);
        } finally {
            views.values().forEach(e -> e.stop());
        }
    }
}
