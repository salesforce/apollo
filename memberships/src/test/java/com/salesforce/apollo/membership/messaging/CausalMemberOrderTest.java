/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.messaging.proto.ByteMessage;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.Signer.SignerImpl;
import com.salesforce.apollo.crypto.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.membership.messaging.causal.CausalBuffer.StampedMessage;
import com.salesforce.apollo.membership.messaging.causal.CausalMessenger;
import com.salesforce.apollo.membership.messaging.causal.Parameters;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class CausalMemberOrderTest {

    private static class ToReceiver {
        private final Digest                            id;
        private final Map<Digest, List<StampedMessage>> messages = new ConcurrentHashMap<>();
        private final CausalMessenger                   messenger;

        public ToReceiver(Digest id, CausalMessenger messenger) {
            this.messenger = messenger;
            this.id = id;
            messenger.registerHandler((cid, msgs) -> msgs.forEach(m -> {
                System.out.println("Rec: " + m.from() + ":" + m.clock().stamp());
                messages.computeIfAbsent(m.from(), k -> new CopyOnWriteArrayList<>()).add(m);
            }));
        }

        public boolean validate(int entries, int count) {
            if (messages.size() != entries) {
                return false;
            }
            for (Entry<Digest, List<StampedMessage>> entry : messages.entrySet()) {
                if (entry.getValue().size() != count) {
                    return false;
                }
            }
            return true;
        }

        public boolean validate(List<ToReceiver> liveRcvrs, int count) {
            for (ToReceiver receiver : liveRcvrs) {
                if (!receiver.id.equals(id)) {
                    List<StampedMessage> msgs = messages.get(receiver.id);
                    if (msgs == null || msgs.size() != count) {
                        return false;
                    }
                }
            }
            return true;
        }
    }

    private static Map<Digest, CertificateWithPrivateKey> certs;
    private static final Parameters.Builder               parameters = Parameters.newBuilder().setMaxMessages(1500)
                                                                                 .setFalsePositiveRate(0.0125)
                                                                                 .setBufferSize(1_500);

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(1, 101).parallel().mapToObj(i -> Utils.getMember(i))
                         .collect(Collectors.toMap(cert -> Member.getMemberIdentifier(cert.getX509Certificate()),
                                                   cert -> cert));
    }

    private final List<Router>    communications = new ArrayList<>();
    private List<CausalMessenger> messengers;

    @AfterEach
    public void after() {
        if (messengers != null) {
            messengers.forEach(e -> e.stop());
        }
        communications.forEach(e -> e.close());
    }

//    @Test
    public void smoke() {
        List<SigningMember> members = certs.values().stream()
                                           .map(cert -> new SigningMemberImpl(Member.getMemberIdentifier(cert.getX509Certificate()),
                                                                              cert.getX509Certificate(),
                                                                              cert.getPrivateKey(),
                                                                              new SignerImpl(0, cert.getPrivateKey()),
                                                                              cert.getX509Certificate().getPublicKey()))
                                           .limit(10).collect(Collectors.toList());

        Context<Member> context = new Context<Member>(DigestAlgorithm.DEFAULT.getOrigin(), 0.1, members.size());
        parameters.setContext(context);
        members.forEach(m -> context.activate(m));

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(members.size());

        ForkJoinPool executor = Router.createFjPool();
        messengers = members.stream().map(node -> {
            LocalRouter comms = new LocalRouter(node, ServerConnectionCache.newBuilder().setTarget(30), executor);
            communications.add(comms);
            comms.start();
            return new CausalMessenger(parameters.setMember(node).setExecutor(executor).build(), comms);
        }).collect(Collectors.toList());

        messengers.forEach(view -> view.start(Duration.ofMillis(10), scheduler));
        List<ToReceiver> receivers = messengers.stream().map(m -> new ToReceiver(m.getMember().getId(), m)).toList();

        int messageCount = 30;

        Executor exec = Executors.newFixedThreadPool(messengers.size());
        Semaphore countDown = new Semaphore(messengers.size());
        for (int i = 0; i < messageCount; i++) {
            messengers.forEach(m -> {
                try {
                    countDown.acquire();
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
                exec.execute(() -> {
                    try {
                        m.publish(ByteMessage.newBuilder()
                                             .setContents(ByteString.copyFromUtf8("Give me food, or give me slack, or kill me"))
                                             .build(),
                                  true);
                    } finally {
                        countDown.release();
                    }
                });
            });
            try {
                Thread.sleep(100); // 10 msgs per second per node
            } catch (InterruptedException e) {
            }
        }

        boolean complete = Utils.waitForCondition(30_000, 1_000, () -> {
            return receivers.stream().map(r -> r.validate(messengers.size(), messageCount)).filter(result -> !result)
                            .count() == 0;
        });
        if (!complete) {
            receivers.forEach(r -> {
                System.out.println(r.messenger.getMember().getId() + ":" + r.messages.size() + " : "
                + r.messages.values().stream().map(e -> e.size()).sorted().collect(Collectors.toList()));
            });
        }
        assertTrue(complete, "did not get all messages : "
        + receivers.stream().filter(r -> !r.validate(messengers.size(), messageCount)).map(r -> r.id).count());
    }

//    @Test
    public void testGaps() throws Exception {
        List<SigningMember> members = certs.values().stream()
                                           .map(cert -> new SigningMemberImpl(Member.getMemberIdentifier(cert.getX509Certificate()),
                                                                              cert.getX509Certificate(),
                                                                              cert.getPrivateKey(),
                                                                              new SignerImpl(0, cert.getPrivateKey()),
                                                                              cert.getX509Certificate().getPublicKey()))
                                           .limit(10).collect(Collectors.toList());

        Context<Member> context = new Context<Member>(DigestAlgorithm.DEFAULT.getOrigin(), 0.33, members.size());
        parameters.setContext(context);
        members.forEach(m -> context.activate(m));
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(members.size());

        ForkJoinPool executor = Router.createFjPool();
        messengers = members.stream().map(node -> {
            LocalRouter comms = new LocalRouter(node, ServerConnectionCache.newBuilder().setTarget(30), executor);
            communications.add(comms);
            comms.start();
            return new CausalMessenger(parameters.setExecutor(executor).setMember(node).build(), comms);
        }).collect(Collectors.toList());

        Duration gossipDuration = Duration.ofMillis(100);
        messengers.forEach(view -> view.start(gossipDuration, scheduler));
        List<ToReceiver> receivers = messengers.stream().map(m -> new ToReceiver(m.getMember().getId(), m))
                                               .collect(Collectors.toList());

        int messageCount = 10;

        for (int i = 0; i < messageCount; i++) {
            messengers.forEach(m -> {
                m.publish(ByteMessage.newBuilder()
                                     .setContents(ByteString.copyFromUtf8("Give me food, or give me slack, or kill me"))
                                     .build(),
                          true);
            });
        }

        boolean complete = Utils.waitForCondition(30_000, 1_000, () -> {
            return receivers.stream().map(r -> r.validate(messengers.size(), messageCount)).filter(result -> !result)
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
            });
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
            }
        }

        complete = Utils.waitForCondition(30_000, 1_000, () -> {
            return liveRcvrs.stream().map(r -> r.validate(liveRcvrs, messageCount * 2)).filter(result -> !result)
                            .count() == 0;
        });
        assertTrue(complete, "did not get all messages : "
        + liveRcvrs.stream().filter(r -> !r.validate(liveRcvrs, messageCount * 2)).map(r -> r.id).count());

        liveRcvrs.forEach(r -> r.messenger.clearBuffer());

        System.out.println("Restarting half");

        System.out.println("Restarting half");
        deadRcvrs.stream().peek(r -> context.activate(r.messenger.getMember()))
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

        complete = Utils.waitForCondition(65_000, 1_000, () -> {
            return liveRcvrs.stream().map(r -> r.validate(liveRcvrs, messageCount * 3)).filter(result -> !result)
                            .count() == 0
            && deadRcvrs.stream().map(r -> r.validate(deadRcvrs, messageCount)).filter(result -> !result).count() == 0;
        });
        if (!complete) {
            System.out.println();
            System.out.println("Live Receivers");
            liveRcvrs.forEach(r -> {
                System.out.println(r.messenger.getMember().getId() + ":" + r.messages.size() + " : "
                + r.messages.values().stream().map(e -> e.size()).sorted().collect(Collectors.toList()));
            });

            System.out.println();
            System.out.println();
            System.out.println("Dead Receivers");
            deadRcvrs.forEach(r -> {
                System.out.println(r.messenger.getMember().getId() + ":" + r.messages.size() + " : "
                + r.messages.values().stream().map(e -> e.size()).sorted().collect(Collectors.toList()));
            });
        }
        assertTrue(complete, "did not get all messages : "
        + liveRcvrs.stream().filter(r -> !r.validate(liveRcvrs, messageCount * 3)).map(r -> r.id).count() + " : "
        + deadRcvrs.stream().filter(r -> !r.validate(deadRcvrs, messageCount)).map(r -> r.id).count());

    }
}
