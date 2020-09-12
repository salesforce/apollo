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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.avro.MessageGossip;
import com.salesforce.apollo.fireflies.View.Service;
import com.salesforce.apollo.fireflies.communications.FfLocalCommSim;
import com.salesforce.apollo.protocols.Utils;

import io.github.olivierlemasle.ca.RootCertificate;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class SuccessorTest {

    private static final RootCertificate     ca         = getCa();
    private static Map<UUID, CertWithKey>    certs;
    private static final FirefliesParameters parameters = new FirefliesParameters(ca.getX509Certificate());

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(1, 10)
                         .parallel()
                         .mapToObj(i -> getMember(i))
                         .collect(Collectors.toMap(cert -> Member.getMemberId(cert.getCertificate()), cert -> cert));
    }

    @Test
    public void allSuccessors() throws Exception {
        Random entropy = new Random(0x666);

        List<X509Certificate> seeds = new ArrayList<>();
        List<Node> members = certs.values()
                                  .parallelStream()
                                  .map(cert -> new CertWithKey(cert.getCertificate(), cert.getPrivateKey()))
                                  .map(cert -> new Node(cert, parameters))
                                  .collect(Collectors.toList());
        FfLocalCommSim communications = new FfLocalCommSim();
        assertEquals(certs.size(), members.size());

        while (seeds.size() < parameters.toleranceLevel + 1) {
            CertWithKey cert = certs.get(members.get(entropy.nextInt(members.size())).getId());
            if (!seeds.contains(cert.getCertificate())) {
                seeds.add(cert.getCertificate());
            }
        }
        MessageBuffer messageBuffer = mock(MessageBuffer.class);
        when(messageBuffer.process(any())).thenReturn(new MessageGossip(Collections.emptyList(),
                Collections.emptyList()));

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(members.size());

        Map<Member, View> views = members.stream()
                                         .map(node -> new View(node, communications, scheduler))
                                         .collect(Collectors.toMap(v -> v.getNode(), v -> v));

        views.values().forEach(view -> view.getService().start(Duration.ofMillis(10), seeds));

        Utils.waitForCondition(15_000, 1_000, () -> {
            return views.values()
                        .stream()
                        .map(view -> view.getLive().size() != views.size() ? view : null)
                        .filter(view -> view != null)
                        .count() == 0;
        });

        for (View view : views.values()) {
            for (Member m : view.getView().values()) {
                assertTrue(m.getEpoch() > 0);
            }
            for (int r = 0; r < parameters.rings; r++) {
                Ring ring = view.getRing(r);
                Member successor = ring.successor(view.getNode());
                View successorView = views.get(successor);
                Member test = successorView.getRing(r).successor(view.getNode());
                assertEquals(successor, test);
            }
        }

        View test = views.get(members.get(0));
        System.out.println("Test member: " + test.getNode());
        Field lastRing = Service.class.getDeclaredField("lastRing");
        lastRing.setAccessible(true);
        int ring = (lastRing.getInt(test.getService()) + 1) % test.getRings().size();
        Member successor = test.getRing(ring).successor(test.getNode(), m -> !m.isFailed());
        System.out.println("ring: " + ring + " successor: " + successor);
        assertEquals(successor, views.get(successor).getRing(ring).successor(test.getNode(), m -> !m.isFailed()));
        assertTrue(successor.isLive());
        test.getService().gossip();

        ring = (ring + 1) % test.getRings().size();
        successor = test.getRing(ring).successor(test.getNode(), m -> !m.isFailed());
        System.out.println("ring: " + ring + " successor: " + successor);
        assertEquals(successor, views.get(successor).getRing(ring).successor(test.getNode(), m -> !m.isFailed()));
        assertTrue(successor.isLive());
        test.getService().gossip();
    }
}
