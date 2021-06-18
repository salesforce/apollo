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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

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
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.utils.Utils;

public class DagTest {
 
    private static Map<Digest, CertificateWithPrivateKey> certs; 

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(1, 101)
                .parallel()
                .mapToObj(i -> getMember(i))
                .collect(Collectors.toMap(cert -> Member.getMemberIdentifier(cert.getX509Certificate()),
                                          cert -> cert));
}

    private List<SigningMember>               members;
    private ScheduledExecutorService scheduler; 
    private Random                   entropy;
    private final List<Router>       comms = new ArrayList<>();

    @AfterEach
    public void after() { 
        comms.forEach(e -> e.close());
        comms.clear();
    }

    @BeforeEach
    public void before() {
        entropy = new Random(0x666);
 
        members = certs.values().parallelStream().map(cert -> new Node(cert, parameters)).collect(Collectors.toList());
        assertEquals(certs.size(), members.size()); 
 
        scheduler = Executors.newScheduledThreadPool(100);

    }

    // @Test
    public void smoke() {
        long then = System.currentTimeMillis();

        List<View> testViews = new ArrayList<>();

        for (int i = 0; i < 50; i++) {
            testViews.add(views.get(i));
        }

        testViews.forEach(e -> e.getService().start(Duration.ofMillis(1000), seeds, scheduler));

        assertTrue(Utils.waitForCondition(30_000, 3_000, () -> {
            return testViews.stream().filter(view -> view.getLive().size() != testViews.size()).count() == 0;
        }), "view did not stabilize");

        System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
                + testViews.size() + " members");

        Iterator<Router> communicatons = comms.iterator();

        List<Ghost> ghosties = testViews.stream()
                                        .map(view -> new Ghost(new GhostParameters(), communicatons.next(), view,
                                                new MemoryStore(null)))
                                        .collect(Collectors.toList());
        ghosties.forEach(e -> e.getService().start());
        assertEquals(ghosties.size(),
                     ghosties.parallelStream()
                             .map(g -> Utils.waitForCondition(15_000, () -> g.joined()))
                             .filter(e -> e)
                             .count(),
                     "Not all nodes joined the cluster");

        Map<Digest, DagEntry> stored = new ConcurrentSkipListMap<>();

        Builder builder = DagEntry.newBuilder()
                                  .setData(Any.pack(ByteMessage.newBuilder()
                                                               .setContents(ByteString.copyFromUtf8("root node"))
                                                               .build()));
        DagEntry root = builder.build();
        stored.put(ghosties.get(0).putDagEntry(root), root);

        int rounds = 10;

        for (int i = 0; i < rounds; i++) {
            for (Ghost ghost : ghosties) {
                Builder b = DagEntry.newBuilder()
                                    .setData(Any.pack(ByteMessage.newBuilder()
                                                                 .setContents(ByteString.copyFromUtf8("root node"))
                                                                 .build()));
                b.setData(Any.pack(ByteMessage.newBuilder()
                                              .setContents(ByteString.copyFromUtf8(String.format("Member: %s round: %s",
                                                                                                 ghost.getNode()
                                                                                                      .getId(),
                                                                                                 i)))
                                              .build()));
                randomLinksTo(stored).forEach(e -> b.addLinks(e.toID()));

                DagEntry entry = builder.build();
                stored.put(ghost.putDagEntry(entry), entry);
            }
        }

        for (Entry<Digest, DagEntry> entry : stored.entrySet()) {
            for (Ghost ghost : ghosties) {
                DagEntry found = ghost.getDagEntry(entry.getKey());
                assertNotNull(found);
                assertArrayEquals(entry.getValue().getData().toByteArray(), found.getData().toByteArray());
            }
        }
        int start = testViews.size();

        int add = 25;
        for (int i = 0; i < add; i++) {
            View view = views.get(i + start);
            testViews.add(view);
            ghosties.add(new Ghost(new GhostParameters(), communicatons.next(), view, new MemoryStore()));
        }

        then = System.currentTimeMillis();
        testViews.forEach(e -> e.getService().start(Duration.ofMillis(1000), seeds, scheduler));
        ghosties.forEach(e -> e.getService().start());
        assertEquals(ghosties.size(),
                     ghosties.parallelStream()
                             .map(g -> Utils.waitForCondition(240_000, () -> g.joined()))
                             .filter(e -> e)
                             .count(),
                     "Not all nodes joined the cluster");

        assertTrue(Utils.waitForCondition(30_000, 1_000, () -> {
            return testViews.stream().filter(view -> view.getLive().size() != testViews.size()).count() == 0;
        }), "view did not stabilize");

        System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
                + testViews.size() + " members");

        for (Entry<Digest, DagEntry> entry : stored.entrySet()) {
            for (Ghost ghost : ghosties) {
                DagEntry found = ghost.getDagEntry(entry.getKey());
                assertNotNull(found, ghost.getNode() + " not found: " + entry.getKey());
                assertArrayEquals(entry.getValue().getData().toByteArray(), found.getData().toByteArray());
                if (entry.getValue().getLinksList() == null) {
                    assertNull(found.getLinksList());
                } else {
                    assertEquals(entry.getValue().getLinksList().size(), found.getLinksList().size());
                }
            }
        }
    }

    private List<Digest> randomLinksTo(Map<Digest, DagEntry> stored) {
        List<Digest> links = new ArrayList<>();
        Set<Digest> keys = stored.keySet();
        for (int i = 0; i < entropy.nextInt(10); i++) {
            Iterator<Digest> it = keys.iterator();
            for (int j = 0; j < entropy.nextInt(keys.size()); j++) {
                it.next();
            }
            links.add(it.next());
        }
        return links;
    }
}
