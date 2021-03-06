/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
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
import com.salesforce.apollo.membership.messaging.Messenger.MessageHandler;
import com.salesforce.apollo.membership.messaging.Messenger.Parameters;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class MessageTest {

    class Receiver implements MessageHandler {
        final Set<Member>       counted = Collections.newSetFromMap(new ConcurrentHashMap<>());
        final AtomicInteger     current;
        volatile CountDownLatch round;

        Receiver(int cardinality, AtomicInteger current) {
            this.current = current;
        }

        @Override
        public void message(Digest context, List<Msg> messages) {
            messages.forEach(message -> {
                assert message.from != null : "null member";
                ByteBuffer buf;
                try {
                    buf = message.content.unpack(ByteMessage.class).getContents().asReadOnlyByteBuffer();
                } catch (InvalidProtocolBufferException e) {
                    throw new IllegalStateException(e);
                }
                assert buf.remaining() > 4 : "buffer: " + buf.remaining();
                if (buf.getInt() == current.get() + 1) {
                    if (counted.add(message.from)) {
                        int totalCount = totalReceived.incrementAndGet();
                        if (totalCount % 1_000 == 0) {
                            System.out.print(".");
                        }
                        if (totalCount % 80_000 == 0) {
                            System.out.println();
                        }
                        if (counted.size() == certs.size() - 1) {
                            round.countDown();
                        }
                    }
                }
            });
        }

        public void setRound(CountDownLatch round) {
            this.round = round;
        }

        void reset() {
            counted.clear();
        }

    }

    public static final String                            DEFAULT_SIGNATURE_ALGORITHM = "SHA256withRSA";
    private static Map<Digest, CertificateWithPrivateKey> certs;

    private static final Parameters parameters = Parameters.newBuilder()
                                                           .setFalsePositiveRate(0.125)
                                                           .setBufferSize(1500)
                                                           .build();

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(1, 101)
                         .parallel()
                         .mapToObj(i -> Utils.getMember(i))
                         .collect(Collectors.toMap(cert -> Member.getMemberIdentifier(cert.getX509Certificate()),
                                                   cert -> cert));
    }

    private final List<Router>  communications = new ArrayList<>();
    private final AtomicInteger totalReceived  = new AtomicInteger(0);
    private List<Messenger>     messengers;

    @AfterEach
    public void after() {
        if (messengers != null) {
            messengers.forEach(e -> e.stop());
        }
        communications.forEach(e -> e.close());
    }

    @Test
    public void broadcast() throws Exception {
        List<SigningMember> members = certs.values()
                                           .stream()
                                           .map(cert -> new SigningMemberImpl(
                                                   Member.getMemberIdentifier(cert.getX509Certificate()),
                                                   cert.getX509Certificate(), cert.getPrivateKey(),
                                                   new SignerImpl(0, cert.getPrivateKey()),
                                                   cert.getX509Certificate().getPublicKey()))
                                           .collect(Collectors.toList());

        Context<Member> context = new Context<Member>(DigestAlgorithm.DEFAULT.getOrigin(), 0.33, members.size());
        members.forEach(m -> context.activate(m));
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(members.size());

        ForkJoinPool executor = Router.createFjPool();
        messengers = members.stream().map(node -> {
            LocalRouter comms = new LocalRouter(node, ServerConnectionCache.newBuilder().setTarget(30), executor);
            communications.add(comms);
            comms.start();
            return new Messenger(node, context, comms, parameters, executor);
        }).collect(Collectors.toList());

        System.out.println("Messaging with " + messengers.size() + " members");
        messengers.forEach(view -> view.start(Duration.ofMillis(100), scheduler));

        Map<Member, Receiver> receivers = new HashMap<>();
        AtomicInteger current = new AtomicInteger(-1);
        for (Messenger view : messengers) {
            Receiver receiver = new Receiver(messengers.size(), current);
            view.registerHandler(receiver);
            receivers.put(view.getMember(), receiver);
        }
        int rounds = 30;
        for (int r = 0; r < rounds; r++) {
            CountDownLatch round = new CountDownLatch(messengers.size());
            for (Receiver receiver : receivers.values()) {
                receiver.setRound(round);
            }
            byte[] rand = new byte[32];
            Utils.secureEntropy().nextBytes(rand);
            ByteBuffer buf = ByteBuffer.wrap(new byte[36]);
            buf.putInt(r);
            buf.put(rand);
            buf.flip();
            assert buf.remaining() > 0;
            messengers.stream().forEach(view -> {
                ByteString packed = ByteString.copyFrom(buf.array());
                assertEquals(36, packed.size());
                view.publish(ByteMessage.newBuilder().setContents(packed).build(), true);
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                }
            });
            boolean success = round.await(20, TimeUnit.SECONDS);
            assertTrue(success, "Did not complete round: " + r + " waiting for: " + round.getCount());

            round = new CountDownLatch(messengers.size());
            current.incrementAndGet();
            for (Receiver receiver : receivers.values()) {
                receiver.reset();
            }
        }
        System.out.println();
    }
}
