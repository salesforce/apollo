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
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.proto.ByteMessage;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.Messenger.MessageHandler.Msg;
import com.salesforce.apollo.membership.messaging.Messenger.Parameters;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.utils.Utils;

import io.github.olivierlemasle.ca.CertificateWithPrivateKey;

/**
 * @author hal.hildebrand
 *
 */
public class MemberOrderTest {

    private static class ToReceiver {
        private final HashKey                 id;
        private final Map<HashKey, List<Msg>> messages = new ConcurrentHashMap<>();
        private final MemberOrder             totalOrder;
        private final Messenger               messenger;

        public ToReceiver(HashKey id, Messenger messenger) {
            this.messenger = messenger;
            this.id = id;
            BiConsumer<HashKey, List<Msg>> processor = (cid,
                                                        msgs) -> msgs.forEach(m -> messages.computeIfAbsent(m.from.getId(),
                                                                                                            k -> new CopyOnWriteArrayList<>())
                                                                                           .add(m));
            totalOrder = new MemberOrder(processor, messenger);
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

        public boolean validate(List<ToReceiver> liveRcvrs, int count) {
            for (ToReceiver receiver : liveRcvrs) {
                if (!receiver.id.equals(id)) {
                    List<Msg> msgs = messages.get(receiver.id);
                    if (msgs == null || msgs.size() != count) {
                        return false;
                    }
                }
            }
            return true;
        }
    }

    private static Map<HashKey, CertificateWithPrivateKey> certs;
    private static final Parameters                        parameters = Parameters.newBuilder()
                                                                                  .setFalsePositiveRate(0.25)
                                                                                  .setBufferSize(500)
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
            CertificateWithPrivateKey cert = certs.get(members.get(Utils.bitStreamEntropy().nextInt(members.size())).getId());
            if (!seeds.contains(cert.getX509Certificate())) {
                seeds.add(cert.getX509Certificate());
            }
        }
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(members.size());

        ForkJoinPool executor = ForkJoinPool.commonPool();
        messengers = members.stream().map(node -> {
            LocalRouter comms = new LocalRouter(node, ServerConnectionCache.newBuilder().setTarget(30), executor);
            communications.add(comms);
            comms.start();
            return new Messenger(node, () -> forSigning(node), context, comms, parameters, executor);
        }).collect(Collectors.toList());

        messengers.forEach(view -> view.start(Duration.ofMillis(100), scheduler));
        List<ToReceiver> receivers = messengers.stream().map(m -> {
            ToReceiver receiver = new ToReceiver(m.getMember().getId(), m);
            receiver.totalOrder.start();
            return receiver;
        }).collect(Collectors.toList());

        int messageCount = 100;

        for (int i = 0; i < messageCount; i++) {
            messengers.forEach(m -> {
                m.publish(ByteMessage.newBuilder()
                                     .setContents(ByteString.copyFromUtf8("Give me food, or give me slack, or kill me"))
                                     .build());
            });
        }

        boolean complete = Utils.waitForCondition(30_000, 1_000, () -> {
            return receivers.stream()
                            .map(r -> r.validate(messengers.size() - 1, messageCount))
                            .filter(result -> !result)
                            .count() == 0;
        });
        assertTrue(complete, "did not get all messages : "
                + receivers.stream().filter(r -> !r.validate(messengers.size(), messageCount)).map(r -> r.id).count());
    }

    @Test
    public void testGaps() throws Exception {
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
            CertificateWithPrivateKey cert = certs.get(members.get(Utils.bitStreamEntropy().nextInt(members.size())).getId());
            if (!seeds.contains(cert.getX509Certificate())) {
                seeds.add(cert.getX509Certificate());
            }
        }
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(members.size());

        ForkJoinPool executor = ForkJoinPool.commonPool();
        messengers = members.stream().map(node -> {
            LocalRouter comms = new LocalRouter(node, ServerConnectionCache.newBuilder().setTarget(30),
                    executor);
            communications.add(comms);
            comms.start();
            return new Messenger(node, () -> forSigning(node), context, comms, parameters, executor);
        }).collect(Collectors.toList());

        Duration gossipDuration = Duration.ofMillis(100);
        messengers.forEach(view -> view.start(gossipDuration, scheduler));
        List<ToReceiver> receivers = messengers.stream().map(m -> {
            ToReceiver receiver = new ToReceiver(m.getMember().getId(), m);
            receiver.totalOrder.start();
            return receiver;
        }).collect(Collectors.toList());

        int messageCount = 10;

        for (int i = 0; i < messageCount; i++) {
            messengers.forEach(m -> {
                m.publish(ByteMessage.newBuilder()
                                     .setContents(ByteString.copyFromUtf8("Give me food, or give me slack, or kill me"))
                                     .build());
            });
        }

        boolean complete = Utils.waitForCondition(30_000, 1_000, () -> {
            return receivers.stream()
                            .map(r -> r.validate(messengers.size() - 1, messageCount))
                            .filter(result -> !result)
                            .count() == 0;
        });
        assertTrue(complete,
                   "did not get all messages : "
                           + receivers.stream()
                                      .filter(r -> !r.validate(messengers.size() - 1, messageCount))
                                      .map(r -> r.id)
                                      .count());

        System.out.println("Stoppig half");
        int half = members.size() / 2;
        List<ToReceiver> deadRcvrs = receivers.subList(0, half);
        List<ToReceiver> liveRcvrs = receivers.subList(half, members.size());

        deadRcvrs.stream().peek(r -> context.offline(r.messenger.getMember())).forEach(m -> m.messenger.stop());

        for (int i = 0; i < messageCount; i++) {
            liveRcvrs.forEach(r -> {
                r.messenger.publish(ByteMessage.newBuilder()
                                               .setContents(ByteString.copyFromUtf8("Give me food, or give me slack, or kill me"))
                                               .build());
            });
        }

        complete = Utils.waitForCondition(30_000, 1_000, () -> {
            return liveRcvrs.stream()
                            .map(r -> r.validate(liveRcvrs, messageCount * 2))
                            .filter(result -> !result)
                            .count() == 0;
        });
        assertTrue(complete, "did not get all messages : "
                + liveRcvrs.stream().filter(r -> !r.validate(liveRcvrs, messageCount * 2)).map(r -> r.id).count());

        System.out.println("Restarting half");
        deadRcvrs.stream()
                 .peek(r -> context.activate(r.messenger.getMember()))
                 .forEach(m -> m.messenger.start(gossipDuration, scheduler));

        Thread.sleep(2000);

        for (int i = 0; i < messageCount; i++) {
            receivers.forEach(r -> {
                r.messenger.publish(ByteMessage.newBuilder()
                                               .setContents(ByteString.copyFromUtf8("Give me food, or give me slack, or kill me"))
                                               .build());
            });
        }

        complete = Utils.waitForCondition(30_000, 1_000, () -> {
            return liveRcvrs.stream()
                            .map(r -> r.validate(liveRcvrs, messageCount * 3))
                            .filter(result -> !result)
                            .count() == 0
                    && deadRcvrs.stream()
                                .map(r -> r.validate(deadRcvrs, messageCount))
                                .filter(result -> !result)
                                .count() == 0;
        });
        assertTrue(complete, "did not get all messages : "
                + liveRcvrs.stream().filter(r -> !r.validate(liveRcvrs, messageCount * 3)).map(r -> r.id).count()
                + " : " + deadRcvrs.stream().filter(r -> !r.validate(deadRcvrs, messageCount)).map(r -> r.id).count());

    }

    private Signature forSigning(Member member) {
        Signature signature;
        try {
            signature = Signature.getInstance(MessageTest.DEFAULT_SIGNATURE_ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("no such algorithm: " + MessageTest.DEFAULT_SIGNATURE_ALGORITHM, e);
        }
        try {
            signature.initSign(certs.get(member.getId()).getPrivateKey(), Utils.secureEntropy());
        } catch (InvalidKeyException e) {
            throw new IllegalStateException("invalid private key", e);
        }
        return signature;
    }
}
