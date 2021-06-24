/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost;

import static com.salesforce.apollo.test.pregen.PregenPopulation.getMember;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
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
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.ghost.Ghost.GhostParameters;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;

@SuppressWarnings("unused")
public class DagTest {

    private static final int                              testCardinality = 100;
    private static Map<Digest, CertificateWithPrivateKey> certs;

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(0, testCardinality)
                         .parallel()
                         .mapToObj(i -> getMember(i))
                         .collect(Collectors.toMap(cert -> Member.getMemberIdentifier(cert.getX509Certificate()),
                                                   cert -> cert));
    }

    private List<SigningMember>      members;
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

        members = certs.values()
                       .stream()
                       .map(cert -> new SigningMemberImpl(Member.getMemberIdentifier(cert.getX509Certificate()),
                               cert.getX509Certificate(), cert.getPrivateKey(), new Signer(0, cert.getPrivateKey()),
                               cert.getX509Certificate().getPublicKey()))
                       .collect(Collectors.toList());
        assertEquals(certs.size(), members.size());

        scheduler = Executors.newScheduledThreadPool(100);

    }

    private static Duration gossipDelay = Duration.ofMillis(500);

    // @Test
    public void smoke() throws Exception {
        long then = System.currentTimeMillis();

        Duration timeout = Duration.ofSeconds(20);
        Context<Member> context = new Context<>(DigestAlgorithm.DEFAULT.getOrigin().prefix(1), 0, testCardinality);
        members.forEach(e -> context.activate(e));

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);

        ForkJoinPool executor = new ForkJoinPool();

        List<Ghost> ghosties = members.stream().map(member -> {
            LocalRouter c = new LocalRouter(member, ServerConnectionCache.newBuilder().setTarget(30), executor);
            comms.add(c);
            c.start();
            return new Ghost(member, GhostParameters.newBuilder().build(), c, context,
                    new MemoryStore(DigestAlgorithm.DEFAULT));
        }).collect(Collectors.toList());

        ghosties.forEach(e -> e.start(scheduler, gossipDelay));

        Map<Digest, Any> stored = new ConcurrentSkipListMap<>();

        Any r = Any.pack(ByteMessage.newBuilder()
                                    .setContents(ByteString.copyFromUtf8(String.format("Member: %s round: %s",
                                                                                       ghosties.get(0)
                                                                                               .getMember()
                                                                                               .getId(),
                                                                                       0)))
                                    .build());

        stored.put(ghosties.get(0).put(r, timeout), r);

        int rounds = 10;

        for (int i = 0; i < rounds; i++) {
            for (Ghost ghost : ghosties) {

                Any entry = Any.pack(ByteMessage.newBuilder()
                                                .setContents(ByteString.copyFromUtf8(String.format("Member: %s round: %s",
                                                                                                   ghosties.get(i)
                                                                                                           .getMember()
                                                                                                           .getId(),
                                                                                                   i)))
                                                .build());
                stored.put(ghost.put(entry, timeout), entry);
            }
        }

        for (Entry<Digest, Any> entry : stored.entrySet()) {
            for (Ghost ghost : ghosties) {
                Any found = ghost.get(entry.getKey(), timeout).get();
                assertNotNull(found);
                assertEquals(entry.getValue(), found);
            }
        }

//        int add = 25;
//        for (int i = 0; i < add; i++) {
//            View view = views.get(i + start);
//            testViews.add(view);
//            ghosties.add(new Ghost(new GhostParameters(), communicatons.next(), view, new MemoryStore()));
//        }
//
//        then = System.currentTimeMillis();
//        testViews.forEach(e -> e.getService().start(Duration.ofMillis(1000), seeds, scheduler));
//        ghosties.forEach(e -> e.getService().start());
//        assertEquals(ghosties.size(),
//                     ghosties.parallelStream()
//                             .map(g -> Utils.waitForCondition(240_000, () -> g.joined()))
//                             .filter(e -> e)
//                             .count(),
//                     "Not all nodes joined the cluster");
//
//        assertTrue(Utils.waitForCondition(30_000, 1_000, () -> {
//            return testViews.stream().filter(view -> view.getLive().size() != testViews.size()).count() == 0;
//        }), "view did not stabilize");
//
//        System.out.println("View has stabilized in " + (System.currentTimeMillis() - then) + " Ms across all "
//                + testViews.size() + " members");
//
//        for (Entry<Digest, DagEntry> entry : stored.entrySet()) {
//            for (Ghost ghost : ghosties) {
//                DagEntry found = ghost.getDagEntry(entry.getKey());
//                assertNotNull(found, ghost.getNode() + " not found: " + entry.getKey());
//                assertArrayEquals(entry.getValue().getData().toByteArray(), found.getData().toByteArray());
//                if (entry.getValue().getLinksList() == null) {
//                    assertNull(found.getLinksList());
//                } else {
//                    assertEquals(entry.getValue().getLinksList().size(), found.getLinksList().size());
//                }
//            }
//        }
    }
}
