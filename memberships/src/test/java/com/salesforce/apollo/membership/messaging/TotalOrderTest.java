/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging;

import static com.salesforce.apollo.test.pregen.PregenPopulation.getMember;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.Messenger.MessageChannelHandler.Msg;
import com.salesforce.apollo.membership.messaging.Messenger.Parameters;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.Utils;

import io.github.olivierlemasle.ca.CertificateWithPrivateKey;

/**
 * @author hal.hildebrand
 *
 */
public class TotalOrderTest {

    private static class ToReceiver {
        private final HashKey                 id;
        private final Map<HashKey, List<Msg>> messages = new ConcurrentHashMap<>();
        private final TotalOrder              totalOrder;

        public ToReceiver(HashKey id, Context<? extends Member> ctx) {
            this.id = id;
            BiConsumer<Msg, HashKey> processor = (m, key) -> {
                messages.computeIfAbsent(m.from.getId(), k -> new CopyOnWriteArrayList<>()).add(m);
            };
            totalOrder = new TotalOrder(processor, ctx);
        }

        public boolean validate(int entries, int count) {
            if (messages.size() != entries) {
                return false;
            }
            for (Entry<HashKey, List<Msg>> entry : messages.entrySet()) {
                if (entry.getValue().size() != count) {
                    return false;
                }
            }
            return true;
        }
    }

    private static Map<HashKey, CertificateWithPrivateKey> certs;
    private static final Parameters                        parameters = Parameters.newBuilder()
                                                                                  .setFalsePositiveRate(0.1)
                                                                                  .setBufferSize(500)
                                                                                  .setEntropy(new SecureRandom())
                                                                                  .build();

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(1, 101)
                         .parallel()
                         .mapToObj(i -> getMember(i))
                         .collect(Collectors.toMap(cert -> Member.getMemberId(cert.getX509Certificate()),
                                                   cert -> cert));
    }

    private final List<Router> communications = new ArrayList<>();
    private List<Messenger>    messengers;

    @AfterEach
    public void after() {
        if (messengers != null) {
            messengers.forEach(e -> e.stop());
        }
        communications.forEach(e -> e.close());
    }

    @Test
    public void smoke() {
        List<X509Certificate> seeds = new ArrayList<>();
        List<Member> members = certs.values()
                                    .parallelStream()
                                    .map(cert -> cert.getX509Certificate())
                                    .map(cert -> new Member(Member.getMemberId(cert), cert))
                                    .collect(Collectors.toList());
        assertEquals(certs.size(), members.size());

        Context<Member> context = new Context<Member>(HashKey.ORIGIN, 9);
        members.forEach(m -> context.activate(m));

        while (seeds.size() < 7) {
            CertificateWithPrivateKey cert = certs.get(members.get(parameters.entropy.nextInt(members.size())).getId());
            if (!seeds.contains(cert.getX509Certificate())) {
                seeds.add(cert.getX509Certificate());
            }
        }
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(members.size());

        messengers = members.stream().map(node -> {
            LocalRouter comms = new LocalRouter(node.getId(), ServerConnectionCache.newBuilder().setTarget(30));
            communications.add(comms);
            comms.start();
            return new Messenger(node, () -> forSigning(node), context, comms, parameters);
        }).collect(Collectors.toList());

        messengers.forEach(view -> view.start(Duration.ofMillis(50), scheduler));
        List<ToReceiver> receivers = messengers.stream().map(m -> {
            ToReceiver receiver = new ToReceiver(m.getMember().getId(), context);
            m.register(0, messages -> receiver.totalOrder.process(messages));
            receiver.totalOrder.start();
            return receiver;
        }).collect(Collectors.toList());

        int messageCount = 100;

        for (int i = 0; i < messageCount; i++) {
            messengers.forEach(m -> {
                m.publish("Give me food, or give me slack, or kill me".getBytes());
            });
        }

        boolean complete = Utils.waitForCondition(30_000, 1_000, () -> {
            return receivers.stream()
                            .map(r -> r.validate(messengers.size(), messageCount))
                            .filter(result -> !result)
                            .count() == 0;
        });
        assertTrue(complete, "did not get all messages : "
                + receivers.stream().filter(r -> !r.validate(messengers.size(), messageCount)).map(r -> r.id).count());
    }

    private Signature forSigning(Member member) {
        Signature signature;
        try {
            signature = Signature.getInstance(MessageTest.DEFAULT_SIGNATURE_ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("no such algorithm: " + MessageTest.DEFAULT_SIGNATURE_ALGORITHM, e);
        }
        try {
            signature.initSign(certs.get(member.getId()).getPrivateKey(), parameters.entropy);
        } catch (InvalidKeyException e) {
            throw new IllegalStateException("invalid private key", e);
        }
        return signature;
    }
}
