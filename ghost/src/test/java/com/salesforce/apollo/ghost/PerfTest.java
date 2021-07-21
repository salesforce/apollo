/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost;

import static com.google.protobuf.ByteString.copyFromUtf8;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.h2.mvstore.MVStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

import com.google.protobuf.Any;
import com.salesfoce.apollo.messaging.proto.ByteMessage;
import com.salesforce.apollo.causal.BloomClock;
import com.salesforce.apollo.causal.IntCausalClock;
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
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class PerfTest {

    private static Map<Digest, CertificateWithPrivateKey> certs;
    private static Duration                               gossipDelay     = Duration.ofMillis(10_000);
    private static final int                              testCardinality = 10;

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(0, testCardinality).parallel().mapToObj(i -> Utils.getMember(i))
                         .collect(Collectors.toMap(cert -> Member.getMemberIdentifier(cert.getX509Certificate()),
                                                   cert -> cert));
    }

    private final List<Router>   communications = new ArrayList<>();
    private List<Ghost<Integer>> ghosties;

    @AfterEach
    public void after() {
        if (ghosties != null) {
            ghosties.forEach(e -> e.stop());
        }
        communications.forEach(e -> e.close());
    }

//    @Test
    public void puts() throws Exception {
        List<SigningMember> members = certs.values().stream()
                                           .map(cert -> new SigningMemberImpl(Member.getMemberIdentifier(cert.getX509Certificate()),
                                                                              cert.getX509Certificate(),
                                                                              cert.getPrivateKey(),
                                                                              new Signer(0, cert.getPrivateKey()),
                                                                              cert.getX509Certificate().getPublicKey()))
                                           .collect(Collectors.toList());
        assertEquals(certs.size(), members.size());
        Context<Member> context = new Context<>(DigestAlgorithm.DEFAULT.getOrigin().prefix(1), 0.2, testCardinality);
        System.out.println("Redundancy: " + context.getRingCount());
        members.forEach(e -> context.activate(e));

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);

        ForkJoinPool executor = new ForkJoinPool();

        List<Ghost<Integer>> ghosties = members.stream().map(member -> {
            LocalRouter comms = new LocalRouter(member, ServerConnectionCache.newBuilder().setTarget(1000), executor);
            communications.add(comms);
            comms.start();
            return new Ghost<Integer>(member, GhostParameters.newBuilder().build(), comms, context, MVStore.open(null),
                                      new IntCausalClock(new BloomClock(), new AtomicInteger(), new ReentrantLock()));
        }).collect(Collectors.toList());
        ghosties.forEach(e -> e.start(scheduler, gossipDelay));

        int msgs = 1_000_000;
        Map<Digest, Any> stored = new HashMap<>();
        Duration timeout = Duration.ofSeconds(500);
        Executor parallel = Executors.newFixedThreadPool(ghosties.size());

        for (int i = 0; i < msgs; i++) {
            int index = i;
            Semaphore extent = new Semaphore(ghosties.size());
            for (Ghost<?> ghost : ghosties) {
                parallel.execute(() -> {
                    try {
                        extent.acquire();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    try {
                        Any entry = Any.pack(ByteMessage.newBuilder()
                                                        .setContents(copyFromUtf8(format("Member: %s round: %s",
                                                                                         ghost.getMember().getId(),
                                                                                         index)))
                                                        .build());
                        Digest put;
                        try {
                            put = ghost.put(entry, timeout);
                            stored.put(put, entry);
                        } catch (TimeoutException e) {
                            e.printStackTrace();
                        }
                    } finally {
                        extent.release();
                    }
                });
            }
        }
        System.out.println("Finished " + msgs * members.size() + " random puts");
    }
}
