/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.h2.mvstore.MVStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.ghost.proto.Binding;
import com.salesfoce.apollo.ghost.proto.Content;
import com.salesfoce.apollo.messaging.proto.ByteMessage;
import com.salesforce.apollo.causal.BloomClock;
import com.salesforce.apollo.causal.IntCausalClock;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.Signer.SignerImpl;
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
public class GhostTest {

    private static Map<Digest, CertificateWithPrivateKey> certs;
    private static Duration                               gossipDelay     = Duration.ofMillis(10_000);
    private static final int                              testCardinality = 100;

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(0, testCardinality).parallel().mapToObj(i -> Utils.getMember(i))
                         .collect(Collectors.toMap(cert -> Member.getMemberIdentifier(cert.getX509Certificate()),
                                                   cert -> cert));
    }

    private final List<Router> communications = new ArrayList<>();
    private List<Ghost<?>>     ghosties;

    @AfterEach
    public void after() {
        if (ghosties != null) {
            ghosties.forEach(e -> e.stop());
        }
        communications.forEach(e -> e.close());
    }

//    @Test deprecate Ghost
    public void lookupBind() throws Exception {
        List<SigningMember> members = certs.values().stream()
                                           .map(cert -> new SigningMemberImpl(Member.getMemberIdentifier(cert.getX509Certificate()),
                                                                              cert.getX509Certificate(),
                                                                              cert.getPrivateKey(),
                                                                              new SignerImpl(0, cert.getPrivateKey()),
                                                                              cert.getX509Certificate().getPublicKey()))
                                           .collect(Collectors.toList());
        assertEquals(certs.size(), members.size());
        Context<Member> context = new Context<>(DigestAlgorithm.DEFAULT.getOrigin().prefix(1), 0.1, testCardinality);
        members.forEach(e -> context.activate(e));

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(testCardinality);

        ForkJoinPool executor = new ForkJoinPool();

        List<Ghost<Integer>> ghosties = members.stream().map(member -> {
            LocalRouter comms = new LocalRouter(member, ServerConnectionCache.newBuilder().setTarget(30), executor);
            communications.add(comms);
            comms.start();
            return new Ghost<Integer>(member, GhostParameters.newBuilder().build(), comms, context, MVStore.open(null),
                                      new IntCausalClock(new BloomClock(), new AtomicInteger(), new ReentrantLock()));
        }).collect(Collectors.toList());
        ghosties.forEach(e -> e.start(scheduler, gossipDelay));

        int msgs = 10;
        Map<String, Any> stored = new HashMap<>();
        Duration timeout = Duration.ofSeconds(1);
        AtomicInteger count = new AtomicInteger();
        for (int i = 0; i < msgs; i++) {
            int index = i;
            for (Ghost<?> ghost : ghosties) {
                String key = "prefix - " + i + " - " + ghost.getMember().getId();
                Any entry = Any.pack(ByteMessage.newBuilder()
                                                .setContents(ByteString.copyFromUtf8(String.format("Member: %s round: %s",
                                                                                                   ghost.getMember()
                                                                                                        .getId(),
                                                                                                   index)))
                                                .build());
                ghost.bind(key, entry, timeout);
                stored.put(key, entry);
                if (count.incrementAndGet() % 100 == 0) {
                    System.out.print('.');
                    if (count.get() % 8000 == 0) {
                        System.out.println();
                    }
                }
            }
        }
        System.out.println();
        System.out.println("Finished " + msgs * members.size() + " random puts, performing gets");

        count.set(0);
        for (Entry<String, Any> entry : stored.entrySet()) {
            for (Ghost<Integer> ghost : ghosties) {
                Binding found = ghost.lookup(entry.getKey(), timeout).get();
                assertNotNull(found);
                assertEquals(entry.getValue(), found.getValue());
                if (count.incrementAndGet() % 100 == 0) {
                    System.out.print('.');
                    if (count.get() % 8000 == 0) {
                        System.out.println();
                    }
                }
            }
        }

        System.out.println("done");
    }

    @Test
    public void putGet() throws Exception {
        List<SigningMember> members = certs.values().stream()
                                           .map(cert -> new SigningMemberImpl(Member.getMemberIdentifier(cert.getX509Certificate()),
                                                                              cert.getX509Certificate(),
                                                                              cert.getPrivateKey(),
                                                                              new SignerImpl(0, cert.getPrivateKey()),
                                                                              cert.getX509Certificate().getPublicKey()))
                                           .collect(Collectors.toList());
        assertEquals(certs.size(), members.size());
        Context<Member> context = new Context<>(DigestAlgorithm.DEFAULT.getOrigin().prefix(1), 0.1, testCardinality);
        members.forEach(e -> context.activate(e));

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(testCardinality);

        ForkJoinPool executor = new ForkJoinPool();

        List<Ghost<Integer>> ghosties = members.stream().map(member -> {
            LocalRouter comms = new LocalRouter(member, ServerConnectionCache.newBuilder().setTarget(30), executor);
            communications.add(comms);
            comms.start();
            return new Ghost<Integer>(member, GhostParameters.newBuilder().build(), comms, context, MVStore.open(null),
                                      new IntCausalClock(new BloomClock(), new AtomicInteger(), new ReentrantLock()));
        }).collect(Collectors.toList());
        ghosties.forEach(e -> e.start(scheduler, gossipDelay));

        int msgs = 10;
        Map<Digest, Any> stored = new HashMap<>();
        Duration timeout = Duration.ofSeconds(1);
        AtomicInteger count = new AtomicInteger();
        for (int i = 0; i < msgs; i++) {
            int index = i;
            for (Ghost<Integer> ghost : ghosties) {
                Any entry = Any.pack(ByteMessage.newBuilder()
                                                .setContents(ByteString.copyFromUtf8(String.format("Member: %s round: %s",
                                                                                                   ghost.getMember()
                                                                                                        .getId(),
                                                                                                   index)))
                                                .build());
                Digest put = ghost.put(entry, timeout);
                stored.put(put, entry);
                if (count.incrementAndGet() % 100 == 0) {
                    System.out.print('.');
                    if (count.get() % 8000 == 0) {
                        System.out.println();
                    }
                }
            }
        }
        System.out.println();
        System.out.println("Finished " + msgs * members.size() + " random puts, performing gets");

        count.set(0);
        for (Entry<Digest, Any> entry : stored.entrySet()) {
            for (Ghost<Integer> ghost : ghosties) {
                Content found = ghost.get(entry.getKey(), timeout).get();
                assertNotNull(found);
                assertEquals(entry.getValue(), found.getValue());
                if (count.incrementAndGet() % 100 == 0) {
                    System.out.print('.');
                    if (count.get() % 8000 == 0) {
                        System.out.println();
                    }
                }
            }
        }

        System.out.println("done");
    }
}
