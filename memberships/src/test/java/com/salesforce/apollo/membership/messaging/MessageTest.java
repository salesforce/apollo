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
import java.security.SecureRandom;
import java.security.Signature;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.proto.Message;
import com.salesforce.apollo.comm.Communications;
import com.salesforce.apollo.comm.LocalCommSimm;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.Messenger.MessageChannelHandler;
import com.salesforce.apollo.membership.messaging.Messenger.Parameters;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;

import io.github.olivierlemasle.ca.CertificateWithPrivateKey;

/**
 * @author hal.hildebrand
 *
 */
public class MessageTest {

    class Receiver implements MessageChannelHandler {
        final Set<Member>       counted = Collections.newSetFromMap(new ConcurrentHashMap<>());
        final AtomicInteger     current;
        final AtomicInteger     dups    = new AtomicInteger(0);
        volatile CountDownLatch round;

        Receiver(int cardinality, AtomicInteger current) {
            this.current = current;
        }

        @Override
        public void message(List<Msg> messages) {
            messages.forEach(message -> {
                assert message.from != null : "null member";
                ByteBuffer buf = ByteBuffer.wrap(message.content);
                if (buf.getInt() == current.get() + 1) {
                    if (counted.add(message.from)) {
                        if (totalReceived.incrementAndGet() % 1_000 == 0) {
                            System.out.print(".");
                        }
                        if (counted.size() == certs.size() - 1) {
                            round.countDown();
                        }
                    } else {
                        dups.incrementAndGet();
                        System.out.print("!");
                    }
                }
            });
        }

        public void setRound(CountDownLatch round) {
            this.round = round;
        }

        void reset() {
            dups.set(0);
            counted.clear();
        }

    }

    private static Map<HashKey, CertificateWithPrivateKey> certs;
    private static final Parameters                        parameters = Parameters.newBuilder()
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

    private final List<Communications> communications = new ArrayList<>();

    private final AtomicInteger totalReceived = new AtomicInteger(0);

    @AfterEach
    public void after() {
        communications.forEach(e -> e.close());
    }

    @Test
    public void testSigning() {
        CertificateWithPrivateKey certificateWithPrivateKey = certs.values().stream().findFirst().get();
        Member from = new Member(Member.getMemberId(certificateWithPrivateKey.getX509Certificate()),
                certificateWithPrivateKey.getX509Certificate());
        Signature signature = forSigning(from);

        byte[] content = new byte[] { 1, 2, 3, 4 };
        long ts = 400;
        int channel = 1;
        HashKey id = HashKey.ORIGIN;
        ByteBuffer header = MessageBuffer.headerBuffer(ts, channel);

        Message message = Message.newBuilder()
                                 .setAge(1)
                                 .setChannel(channel)
                                 .setContent(ByteString.copyFrom(content))
                                 .setSource(id.toID())
                                 .setSignature(ByteString.copyFrom(MessageBuffer.sign(from, signature, header,
                                                                                      content)))
                                 .build();

        MessageBuffer.validate(message, from.forVerification(Conversion.DEFAULT_SIGNATURE_ALGORITHM));
    }

    @Test
    public void testMessageBuffer() {
        CertificateWithPrivateKey certificateWithPrivateKey = certs.values().stream().findFirst().get();
        Member from = new Member(Member.getMemberId(certificateWithPrivateKey.getX509Certificate()),
                certificateWithPrivateKey.getX509Certificate());

        MessageBuffer buff1 = new MessageBuffer(10, 10);
        byte[] bytes1 = new byte[] { 1, 2, 3, 4, 5 };
        Message message = buff1.put(1, bytes1, from, forSigning(from), 1);

        MessageBuffer buff2 = new MessageBuffer(10, 10);
        List<Message> merged = buff2.merge(Arrays.asList(message),
                                           m -> MessageBuffer.validate(message,
                                                                       from.forVerification(Conversion.DEFAULT_SIGNATURE_ALGORITHM)));

        assertEquals(1, merged.size());
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
            CertificateWithPrivateKey cert = certs.get(members.get(parameters.entropy.nextInt(members.size())).getId());
            if (!seeds.contains(cert.getX509Certificate())) {
                seeds.add(cert.getX509Certificate());
            }
        }
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(members.size());

        List<Messenger> messengers = members.stream().map(node -> {
            Communications comms = new LocalCommSimm(ServerConnectionCache.newBuilder().setTarget(30), node.getId());
            communications.add(comms);
            return new Messenger(node, () -> forSigning(node), context, comms, parameters);
        }).collect(Collectors.toList());

        messengers.forEach(view -> view.start(Duration.ofMillis(100), scheduler));

        try {
            Map<Member, Receiver> receivers = new HashMap<>();
            AtomicInteger current = new AtomicInteger(-1);
            for (Messenger view : messengers) {
                Receiver receiver = new Receiver(messengers.size(), current);
                view.register(0, receiver);
                receivers.put(view.getMember(), receiver);
            }
            int rounds = 5;
            for (int r = 0; r < rounds; r++) {
                CountDownLatch round = new CountDownLatch(messengers.size());
                for (Receiver receiver : receivers.values()) {
                    receiver.setRound(round);
                }
                ByteBuffer buf = ByteBuffer.wrap(new byte[4]);
                buf.putInt(r);
                messengers.parallelStream().forEach(view -> view.publish(0, buf.array()));
                boolean success = round.await(10, TimeUnit.SECONDS);
                assertTrue(success, "Did not complete round: " + r + " waiting for: " + round.getCount());

                round = new CountDownLatch(messengers.size());
                current.incrementAndGet();
                for (Receiver receiver : receivers.values()) {
                    assertEquals(0, receiver.dups.get());
                    receiver.reset();
                }
            }
            System.out.println();
        } finally {
            messengers.forEach(e -> e.stop());
        }
    }

    public static final String DEFAULT_SIGNATURE_ALGORITHM = "SHA256withRSA";

    private Signature forSigning(Member member) {
        Signature signature;
        try {
            signature = Signature.getInstance(DEFAULT_SIGNATURE_ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("no such algorithm: " + DEFAULT_SIGNATURE_ALGORITHM, e);
        }
        try {
            signature.initSign(certs.get(member.getId()).getPrivateKey(), parameters.entropy);
        } catch (InvalidKeyException e) {
            throw new IllegalStateException("invalid private key", e);
        }
        return signature;
    }
}
