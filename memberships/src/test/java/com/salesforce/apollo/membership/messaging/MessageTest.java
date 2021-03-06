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

import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.security.cert.X509Certificate;
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
import com.salesfoce.apollo.proto.ByteMessage;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.Messenger.MessageHandler;
import com.salesforce.apollo.membership.messaging.Messenger.Parameters;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.Utils;

import io.github.olivierlemasle.ca.CertificateWithPrivateKey;

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
        public void message(HashKey context, List<Msg> messages) {
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

    public static final String                             DEFAULT_SIGNATURE_ALGORITHM = "SHA256withRSA";
    private static Map<HashKey, CertificateWithPrivateKey> certs;

    private static final Parameters parameters = Parameters.newBuilder().setBufferSize(100).build();

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(1, 101)
                         .parallel()
                         .mapToObj(i -> getMember(i))
                         .collect(Collectors.toMap(cert -> Member.getMemberId(cert.getX509Certificate()),
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
            messengers.parallelStream().forEach(view -> {
                ByteString packed = ByteString.copyFrom(buf.array());
                assertEquals(36, packed.size());
                view.publish(ByteMessage.newBuilder().setContents(packed).build());
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

    private Signature forSigning(Member member) {
        Signature signature;
        try {
            signature = Signature.getInstance(DEFAULT_SIGNATURE_ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("no such algorithm: " + DEFAULT_SIGNATURE_ALGORITHM, e);
        }
        try {
            signature.initSign(certs.get(member.getId()).getPrivateKey(), Utils.secureEntropy());
        } catch (InvalidKeyException e) {
            throw new IllegalStateException("invalid private key", e);
        }
        return signature;
    }
}
