/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.google.protobuf.ByteString;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.comm.ServerConnectionCacheMetricsImpl;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.Signer.SignerImpl;
import com.salesforce.apollo.crypto.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.membership.messaging.rbc.RbcMetrics;
import com.salesforce.apollo.membership.messaging.rbc.RbcMetricsImpl;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster.MessageHandler;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster.Msg;
import com.salesforce.apollo.membership.messaging.rbc.ReliableBroadcaster.Parameters;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class RbcTest {

    class Receiver implements MessageHandler {
        final Set<Digest>       counted = Collections.newSetFromMap(new ConcurrentHashMap<>());
        final AtomicInteger     current;
        volatile CountDownLatch round;

        Receiver(int cardinality, AtomicInteger current) {
            this.current = current;
        }

        @Override
        public void message(Digest context, List<Msg> messages) {
            messages.forEach(m -> {
                assert m.source() != null : "null member";
                ByteBuffer buf = m.content().asReadOnlyByteBuffer();
                assert buf.remaining() > 4 : "buffer: " + buf.remaining();
                if (buf.getInt() == current.get() + 1) {
                    if (counted.add(m.source())) {
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

    private static Map<Digest, CertificateWithPrivateKey> certs;
    private static final Parameters.Builder               parameters = Parameters.newBuilder()
                                                                                 .setMaxMessages(100)
                                                                                 .setFalsePositiveRate(0.0125)
                                                                                 .setBufferSize(500);

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(1, 101)
                         .parallel()
                         .mapToObj(i -> Utils.getMember(i))
                         .collect(Collectors.toMap(cert -> Member.getMemberIdentifier(cert.getX509Certificate()),
                                                   cert -> cert));
    }

    private final List<Router>        communications = new ArrayList<>();
    private final AtomicInteger       totalReceived  = new AtomicInteger(0);
    private List<ReliableBroadcaster> messengers;

    @AfterEach
    public void after() {
        if (messengers != null) {
            messengers.forEach(e -> e.stop());
        }
        communications.forEach(e -> e.close());
    }

    @Test
    public void broadcast() throws Exception {
        MetricRegistry registry = new MetricRegistry();

        List<SigningMember> members = certs.values()
                                           .stream()
                                           .map(cert -> new SigningMemberImpl(Member.getMemberIdentifier(cert.getX509Certificate()),
                                                                              cert.getX509Certificate(),
                                                                              cert.getPrivateKey(),
                                                                              new SignerImpl(cert.getPrivateKey()),
                                                                              cert.getX509Certificate().getPublicKey()))
                                           .collect(Collectors.toList());

        Context<Member> context = Context.newBuilder().setCardinality(members.size()).build();
        RbcMetrics metrics = new RbcMetricsImpl(context.getId(), "test", registry);
        members.forEach(m -> context.activate(m));

        final var prefix = UUID.randomUUID().toString();
        final var exec = Executors.newFixedThreadPool(100);
        messengers = members.stream().map(node -> {
            var comms = new LocalRouter(prefix,
                                        ServerConnectionCache.newBuilder()
                                                             .setTarget(30)
                                                             .setMetrics(new ServerConnectionCacheMetricsImpl(registry)),
                                        Executors.newFixedThreadPool(1), metrics.limitsMetrics());
            communications.add(comms);
            comms.setMember(node);
            comms.start();
            return new ReliableBroadcaster(context, node, parameters.build(), exec, comms, metrics);
        }).collect(Collectors.toList());

        System.out.println("Messaging with " + messengers.size() + " members");
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(50);
        messengers.forEach(view -> view.start(Duration.ofMillis(10), scheduler));

        Map<Member, Receiver> receivers = new HashMap<>();
        AtomicInteger current = new AtomicInteger(-1);
        for (ReliableBroadcaster view : messengers) {
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
            var rnd = r;
            messengers.stream().forEach(view -> {
                byte[] rand = new byte[32];
                Utils.secureEntropy().nextBytes(rand);
                ByteBuffer buf = ByteBuffer.wrap(new byte[36]);
                buf.putInt(rnd);
                buf.put(rand);
                buf.flip();
                view.publish(ByteString.copyFrom(buf), true);
            });
            boolean success = round.await(20, TimeUnit.SECONDS);
            assertTrue(success, "Did not complete round: " + r + " waiting for: " + round.getCount());

            current.incrementAndGet();
            for (Receiver receiver : receivers.values()) {
                receiver.reset();
            }
        }
        communications.forEach(e -> e.close());

        System.out.println();

        ConsoleReporter.forRegistry(registry)
                       .convertRatesTo(TimeUnit.SECONDS)
                       .convertDurationsTo(TimeUnit.MILLISECONDS)
                       .build()
                       .report();
    }
}
