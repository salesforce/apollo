/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost;
 
import static com.salesforce.apollo.test.pregen.PregenPopulation.getMember;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.messaging.proto.ByteMessage;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.fireflies.FirefliesParameters;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.fireflies.View;
import com.salesforce.apollo.ghost.Ghost.GhostParameters;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class GhostTest {
 
    private static Map<Digest, CertificateWithPrivateKey> certs;
    private static final FirefliesParameters parameters = new FirefliesParameters(ca.getX509Certificate());

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(1, 101)
                         .parallel()
                         .mapToObj(i -> getMember(i))
                         .collect(Collectors.toMap(cert -> Member.getMemberId(cert.getX509Certificate()),
                                                   cert -> new CertificateWithPrivateKey(cert.getX509Certificate(),
                                                           cert.getPrivateKey())));
    }

    private final List<Router> comms = new ArrayList<>();
    private List<View>         views;

    @AfterEach
    public void after() {
        if (views != null) {
            views.forEach(e -> e.getService().stop());
        }
        comms.forEach(e -> e.close());
    }

    // @Test
    public void smoke() throws Exception {
        Random entropy = new Random(0x666);

        List<X509Certificate> seeds = new ArrayList<>();
        List<Node> members = certs.values()
                                  .parallelStream()
                                  .map(cert -> new Node(cert, parameters))
                                  .collect(Collectors.toList());
        assertEquals(certs.size(), members.size());

        while (seeds.size() < parameters.toleranceLevel + 1) {
            CertificateWithPrivateKey cert = certs.get(members.get(entropy.nextInt(members.size())).getId());
            if (!seeds.contains(cert.getCertificate())) {
                seeds.add(cert.getCertificate());
            }
        }

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);

        ForkJoinPool executor = new ForkJoinPool();
        views = members.stream().map(node -> {
            Router com = new LocalRouter(node, ServerConnectionCache.newBuilder(), executor);
            comms.add(com);
            View view = new View(Digest.ORIGIN, node, com, null);
            return view;
        }).collect(Collectors.toList());

        long then = System.currentTimeMillis();
        views.forEach(view -> view.getService().start(Duration.ofMillis(500), seeds, scheduler));

        Utils.waitForCondition(30_000, 3_000, () -> {
            return views.stream()
                        .map(view -> view.getLive().size() != views.size() ? view : null)
                        .filter(view -> view != null)
                        .count() == 0;
        });

        System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
                + views.size() + " members");

        Iterator<Router> communications = comms.iterator();
        List<Ghost> ghosties = views.stream()
                                    .map(view -> new Ghost(new GhostParameters(), communications.next(), view,
                                            new MemoryStore(DigestAlgorithm.DEFAULT)))
                                    .collect(Collectors.toList());
        ghosties.forEach(e -> e.getService().start());
        assertEquals(ghosties.size(),
                     ghosties.parallelStream()
                             .map(g -> Utils.waitForCondition(150_000, () -> g.joined()))
                             .filter(e -> e)
                             .count(),
                     "Not all nodes joined the cluster");
        int rounds = 3;
        Map<Digest, DagEntry> stored = new HashMap<>();
        for (int i = 0; i < rounds; i++) {
            for (Ghost ghost : ghosties) {
                Builder builder = DagEntry.newBuilder()
                                          .setData(Any.pack(ByteMessage.newBuilder()
                                                                       .setContents(ByteString.copyFromUtf8(String.format("Member: %s round: %s",
                                                                                                                          ghost.getNode()
                                                                                                                               .getId(),
                                                                                                                          i)))
                                                                       .build()));
                DagEntry entry = builder.build();
                stored.put(ghost.putDagEntry(entry), entry);
            }
        }

        Thread.sleep(3000);
        for (Entry<Digest, DagEntry> entry : stored.entrySet()) {
            for (Ghost ghost : ghosties) {
                DagEntry found = ghost.getDagEntry(entry.getKey());
                assertNotNull(found);
                assertArrayEquals(entry.getValue().getData().toByteArray(), found.getData().toByteArray());
            }
        }
    }
}
