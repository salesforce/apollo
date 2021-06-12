/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging;

import static com.salesforce.apollo.test.pregen.PregenPopulation.getMember;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.membership.messaging.Messenger.MessageHandler.Msg;
import com.salesforce.apollo.membership.messaging.Messenger.Parameters;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class MemberOrderTest {

    private static class ToReceiver {
        private final Digest                 id;
        private final Map<Digest, List<Msg>> messages = new ConcurrentHashMap<>();
        private final MemberOrder            totalOrder;
        private final Messenger              messenger;

        public ToReceiver(Digest id, Messenger messenger) {
            this.messenger = messenger;
            this.id = id;
            BiConsumer<Digest, List<Msg>> processor = (cid,
                                                       msgs) -> msgs.forEach(m -> messages.computeIfAbsent(m.from.getId(),
                                                                                                           k -> new CopyOnWriteArrayList<>())
                                                                                          .add(m));
            totalOrder = new MemberOrder(processor, messenger);
        }

        public boolean validate(int entries, int count) {
            if (messages.size() != entries) {
                return false;
            }
            for (Entry<Digest, List<Msg>> entry : messages.entrySet()) {
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

    private static Map<Digest, CertificateWithPrivateKey> certs;
    private static final Parameters                       parameters = Parameters.newBuilder()
                                                                                 .setFalsePositiveRate(0.00125)
                                                                                 .setBufferSize(1000)
                                                                                 .build();

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(1, 101)
                         .parallel()
                         .mapToObj(i -> getMember(i))
                         .collect(Collectors.toMap(cert -> Member.getMemberIdentifier(cert.getX509Certificate()),
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
        List<SigningMember> members = certs.values()
                                           .parallelStream()
                                           .map(cert -> new SigningMemberImpl(
                                                   Member.getMemberIdentifier(cert.getX509Certificate()),
                                                   cert.getX509Certificate(), cert.getPrivateKey(),
                                                   new Signer(0, cert.getPrivateKey()),
                                                   cert.getX509Certificate().getPublicKey()))
                                           .limit(20)
                                           .collect(Collectors.toList());

        Context<Member> context = new Context<Member>(DigestAlgorithm.DEFAULT.getOrigin(), 0.33, members.size());
        members.forEach(m -> context.activate(m));

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(members.size());

        ForkJoinPool executor = ForkJoinPool.commonPool();
        messengers = members.stream().map(node -> {
            LocalRouter comms = new LocalRouter(node, ServerConnectionCache.newBuilder().setTarget(30), executor);
            communications.add(comms);
            comms.start();
            return new Messenger(node, context, comms, parameters, executor);
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
                                     .build(),
                          true);
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                }
            });
        }

        boolean complete = Utils.waitForCondition(30_000, 1_000, () -> {
            return receivers.stream()
                            .map(r -> r.validate(messengers.size(), messageCount))
                            .filter(result -> !result)
                            .count() == 0;
        });
        receivers.forEach(m -> System.out.println(m.totalOrder));
        receivers.forEach(r -> {
            System.out.println(r.messenger.getMember().getId() + ":" + r.messages.size() + " : "
                    + r.messages.values().stream().map(e -> e.size()).sorted().collect(Collectors.toList()));
        });
        assertTrue(complete, "did not get all messages : "
                + receivers.stream().filter(r -> !r.validate(messengers.size(), messageCount)).map(r -> r.id).count());
    }

    @Test
    public void testGaps() throws Exception {
        List<SigningMember> members = certs.values()
                                           .parallelStream()
                                           .map(cert -> new SigningMemberImpl(
                                                   Member.getMemberIdentifier(cert.getX509Certificate()),
                                                   cert.getX509Certificate(), cert.getPrivateKey(),
                                                   new Signer(0, cert.getPrivateKey()),
                                                   cert.getX509Certificate().getPublicKey()))
                                           .limit(30)
                                           .collect(Collectors.toList());

        Context<Member> context = new Context<Member>(DigestAlgorithm.DEFAULT.getOrigin(), 0.33, members.size());
        members.forEach(m -> context.activate(m));
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(members.size());

        ForkJoinPool executor = ForkJoinPool.commonPool();
        messengers = members.stream().map(node -> {
            LocalRouter comms = new LocalRouter(node, ServerConnectionCache.newBuilder().setTarget(30), executor);
            communications.add(comms);
            comms.start();
            return new Messenger(node, context, comms, parameters, executor);
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
                                     .build(),
                          true);
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                }
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

        System.out.println("Stoppig half");
        int half = members.size() / 2;
        List<ToReceiver> deadRcvrs = receivers.subList(0, half);
        List<ToReceiver> liveRcvrs = receivers.subList(half, members.size());

        deadRcvrs.stream().peek(r -> context.offline(r.messenger.getMember())).forEach(m -> m.messenger.stop());
        deadRcvrs.forEach(r -> r.messenger.clearBuffer());

        for (int i = 0; i < messageCount; i++) {
            liveRcvrs.forEach(r -> {
                r.messenger.publish(ByteMessage.newBuilder()
                                               .setContents(ByteString.copyFromUtf8("Give me food, or give me slack, or kill me"))
                                               .build(),
                                    true);
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                }
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

        liveRcvrs.forEach(r -> r.messenger.clearBuffer());

        System.out.println("Restarting half");

        System.out.println("Restarting half");
        deadRcvrs.stream()
                 .peek(r -> context.activate(r.messenger.getMember()))
                 .forEach(m -> m.messenger.start(gossipDuration, scheduler));

        for (int i = 0; i < messageCount; i++) {
            receivers.forEach(r -> {
                r.messenger.publish(ByteMessage.newBuilder()
                                               .setContents(ByteString.copyFromUtf8("Give me food, or give me slack, or kill me"))
                                               .build(),
                                    true);
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                }
            });
        }

        complete = Utils.waitForCondition(15_000, 1_000, () -> {
            return liveRcvrs.stream()
                            .map(r -> r.validate(liveRcvrs, messageCount * 3))
                            .filter(result -> !result)
                            .count() == 0
                    && deadRcvrs.stream()
                                .map(r -> r.validate(deadRcvrs, messageCount))
                                .filter(result -> !result)
                                .count() == 0;
        });
        System.out.println();
        System.out.println("Live Receivers");
        liveRcvrs.forEach(m -> System.out.println(m.totalOrder));
        liveRcvrs.forEach(r -> {
            System.out.println(r.messenger.getMember().getId() + ":" + r.messages.size() + " : "
                    + r.messages.values().stream().map(e -> e.size()).sorted().collect(Collectors.toList()));
        });

        System.out.println();
        System.out.println();
        System.out.println("Dead Receivers");
        deadRcvrs.forEach(m -> System.out.println(m.totalOrder));
        deadRcvrs.forEach(r -> {
            System.out.println(r.messenger.getMember().getId() + ":" + r.messages.size() + " : "
                    + r.messages.values().stream().map(e -> e.size()).sorted().collect(Collectors.toList()));
        });

        assertTrue(complete, "did not get all messages : "
                + liveRcvrs.stream().filter(r -> !r.validate(liveRcvrs, messageCount * 3)).map(r -> r.id).count()
                + " : " + deadRcvrs.stream().filter(r -> !r.validate(deadRcvrs, messageCount)).map(r -> r.id).count());

    }
}
