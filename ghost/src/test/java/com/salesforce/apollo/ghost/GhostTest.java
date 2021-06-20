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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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

/**
 * @author hal.hildebrand
 * @since 220
 */
public class GhostTest {

    private static Map<Digest, CertificateWithPrivateKey> certs;
    private static Duration                               gossipDelay     = Duration.ofMillis(10);
    private static final int                              testCardinality = 100;

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(0, testCardinality)
                         .parallel()
                         .mapToObj(i -> getMember(i))
                         .collect(Collectors.toMap(cert -> Member.getMemberIdentifier(cert.getX509Certificate()),
                                                   cert -> cert));
    }

    private final List<Router> communications = new ArrayList<>();
    private List<Ghost>        ghosties;

    @AfterEach
    public void after() {
        if (ghosties != null) {
            ghosties.forEach(e -> e.stop());
        }
        communications.forEach(e -> e.close());
    }

    @Test
    public void smoke() throws Exception {
        List<SigningMember> members = certs.values()
                                           .parallelStream()
                                           .map(cert -> new SigningMemberImpl(
                                                   Member.getMemberIdentifier(cert.getX509Certificate()),
                                                   cert.getX509Certificate(), cert.getPrivateKey(),
                                                   new Signer(0, cert.getPrivateKey()),
                                                   cert.getX509Certificate().getPublicKey()))
                                           .collect(Collectors.toList());
        assertEquals(certs.size(), members.size());
        Context<Member> context = new Context<>(DigestAlgorithm.DEFAULT.getOrigin().prefix(1), 0, testCardinality);

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);

        ForkJoinPool executor = new ForkJoinPool();

        List<Ghost> ghosties = members.stream().map(member -> {
            LocalRouter comms = new LocalRouter(member, ServerConnectionCache.newBuilder().setTarget(30), executor);
            communications.add(comms);
            comms.start();
            return new Ghost(member, new GhostParameters(), comms, context, new MemoryStore(DigestAlgorithm.DEFAULT));
        }).collect(Collectors.toList());
        ghosties.forEach(e -> e.start(scheduler, gossipDelay));

        int rounds = 3;
        Map<Digest, Any> stored = new HashMap<>();
        Duration timeout = Duration.ofSeconds(5);
        for (int i = 0; i < rounds; i++) {
            for (Ghost ghost : ghosties) {
                Any entry = Any.pack(ByteMessage.newBuilder()
                                                .setContents(ByteString.copyFromUtf8(String.format("Member: %s round: %s",
                                                                                                   ghost.getMember()
                                                                                                        .getId(),
                                                                                                   i)))
                                                .build());
                stored.put(ghost.put(entry, timeout), entry);
            }
        }

        Thread.sleep(3000);
        for (Entry<Digest, Any> entry : stored.entrySet()) {
            for (Ghost ghost : ghosties) {
                Any found = ghost.get(entry.getKey(), timeout);
                assertNotNull(found);
                assertEquals(entry.getValue(), found);
            }
        }
    }
}
