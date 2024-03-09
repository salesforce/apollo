/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.ethereal;

import com.codahale.metrics.MetricRegistry;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesforce.apollo.archipelago.LocalServer;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.ServerConnectionCache;
import com.salesforce.apollo.context.StaticContext;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.cryptography.Signer;
import com.salesforce.apollo.ethereal.memberships.ChRbcGossip;
import com.salesforce.apollo.ethereal.memberships.comm.EtherealMetricsImpl;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.messaging.proto.ByteMessage;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.utils.Entropy;
import org.junit.jupiter.api.Test;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author hal.hildebrand
 */
public class EtherealTest {

    private final static long    DELAY_MS;
    private static final int     EPOCH_LENGTH = 30;
    private static final boolean LARGE_TESTS;
    private static final int     NPROC;
    private static final int     NUM_EPOCHS   = 3;

    static {
        LARGE_TESTS = Boolean.getBoolean("large_tests");
        DELAY_MS = 5;
        NPROC = LARGE_TESTS ? 7 : 4;
    }

    @Test
    public void context() throws Exception {
        one(0);
    }

    @Test
    public void lots() throws Exception {
        if (!LARGE_TESTS) {
            return;
        }
        for (int i = 0; i < 10; i++) {
            System.out.println("Iteration: " + i);
            one(i);
            System.out.println();
        }
    }

    @Test
    public void unbounded() throws NoSuchAlgorithmException, InterruptedException, InvalidProtocolBufferException {
        final var gossipPeriod = Duration.ofMillis(5);

        var registry = new MetricRegistry();

        CountDownLatch finished = new CountDownLatch((short) NPROC);

        List<Ethereal> controllers = new ArrayList<>();
        List<DataSource> dataSources = new ArrayList<>();
        List<ChRbcGossip> gossipers = new ArrayList<>();
        List<Router> comms = new ArrayList<>();

        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);

        List<Member> members = IntStream.range(0, (short) NPROC)
                                        .mapToObj(i -> stereotomy.newIdentifier())
                                        .map(ControlledIdentifierMember::new)
                                        .map(e -> (Member) e)
                                        .toList();

        StaticContext<Member> context = new StaticContext<>(DigestAlgorithm.DEFAULT.getOrigin(), 0.1, members, 3);
        var metrics = new EtherealMetricsImpl(context.getId(), "test", registry);
        var builder = Config.newBuilder().setnProc((short) NPROC).setNumberOfEpochs(-1).setEpochLength(EPOCH_LENGTH);

        List<List<List<ByteString>>> produced = new ArrayList<>();
        for (int i = 0; i < (short) NPROC; i++) {
            produced.add(new CopyOnWriteArrayList<>());
        }

        final var prefix = UUID.randomUUID().toString();
        int maxSize = 1024 * 1024;
        var expectedExpochs = NUM_EPOCHS * 2;
        var epochCountDown = new CountDownLatch(NPROC * expectedExpochs);
        for (short i = 0; i < (short) NPROC; i++) {
            var level = new AtomicInteger();
            var ds = new SimpleDataSource();
            final short pid = i;
            List<List<ByteString>> output = produced.get(pid);
            final var member = members.get(i);
            var com = new LocalServer(prefix, member).router(ServerConnectionCache.newBuilder());
            comms.add(com);
            var controller = new Ethereal(builder.setSigner((Signer) members.get(i)).setPid(pid).build(), maxSize, ds,
                                          (pb, last) -> {
                                              System.out.println("block: " + level.incrementAndGet() + " pid: " + pid);
                                              output.add(pb);
                                              if (last) {
                                                  finished.countDown();
                                              }
                                          }, ep -> {
                epochCountDown.countDown();
                if (pid == 0) {
                    System.out.println("new epoch: " + ep);
                }
            }, "Test: " + i);

            var gossiper = new ChRbcGossip(context, (SigningMember) member, controller.processor(), com, metrics);
            gossipers.add(gossiper);
            dataSources.add(ds);
            controllers.add(controller);
            for (int d = 0; d < 5000; d++) {
                ds.dataStack.add(ByteMessage.newBuilder()
                                            .setContents(ByteString.copyFromUtf8("pid: " + pid + " data: " + d))
                                            .build()
                                            .toByteString());
            }
        }
        try {
            controllers.forEach(Ethereal::start);
            comms.forEach(Router::start);
            gossipers.forEach(e -> {
                e.start(gossipPeriod);
            });
            epochCountDown.await(LARGE_TESTS ? 90 : 10, TimeUnit.SECONDS);
            controllers.forEach(Ethereal::completeIt);
            finished.await(5, TimeUnit.SECONDS);
        } finally {
            controllers.forEach(Ethereal::stop);
            gossipers.forEach(ChRbcGossip::stop);
            comms.forEach(e -> e.close(Duration.ofSeconds(1)));
        }

        final var expected = expectedExpochs * (EPOCH_LENGTH - 1);
        final var first = produced.stream().filter(l -> l.size() == expected).findFirst();
        assertFalse(first.isEmpty(),
                    "no process produced " + expected + " blocks: " + produced.stream().map(l -> l.size()).toList());
        List<List<ByteString>> preblocks = first.get();
        List<String> outputOrder = new ArrayList<>();
        Set<Short> failed = new HashSet<>();
        for (short i = 0; i < NPROC; i++) {
            final List<List<ByteString>> output = produced.get(i);
            if (output.size() != expected) {
                System.out.println("did not get all expected blocks on: " + i + " blocks received: " + output.size());
            } else {
                for (int j = 0; j < preblocks.size(); j++) {
                    var a = preblocks.get(j);
                    var b = output.get(j);
                    if (a.size() != b.size()) {
                        failed.add(i);
                        System.out.println(
                        "mismatch at block: " + j + " process: " + i + " data size: " + a.size() + " != " + b.size());
                    } else {
                        for (int k = 0; k < a.size(); k++) {
                            if (!a.get(k).equals(b.get(k))) {
                                failed.add(i);
                                System.out.println(
                                "mismatch at block: " + j + " unit: " + k + " process: " + i + " expected: " + a.get(k)
                                + " received: " + b.get(k));
                            }
                            outputOrder.add(new String(ByteMessage.parseFrom(a.get(k)).getContents().toByteArray()));
                        }
                    }
                }
            }
        }
        assertTrue((NPROC - failed.size()) >= context.majority(), "Failed");
        assertTrue(produced.stream().map(List::size).filter(count -> count == expected).count() >= context.majority(),
                   "Failed to obtain majority agreement on output count");
    }

    private void one(int iteration)
    throws NoSuchAlgorithmException, InterruptedException, InvalidProtocolBufferException {
        final var gossipPeriod = Duration.ofMillis(5);

        var registry = new MetricRegistry();

        CountDownLatch finished = new CountDownLatch((short) NPROC);

        List<Ethereal> controllers = new ArrayList<>();
        List<DataSource> dataSources = new ArrayList<>();
        List<ChRbcGossip> gossipers = new ArrayList<>();
        List<Router> comms = new ArrayList<>();

        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);

        List<Member> members = IntStream.range(0, (short) NPROC)
                                        .mapToObj(i -> stereotomy.newIdentifier())
                                        .map(ControlledIdentifierMember::new)
                                        .map(e -> (Member) e)
                                        .toList();

        StaticContext<Member> context = new StaticContext<>(DigestAlgorithm.DEFAULT.getOrigin(), 0.1, members, 3);
        var metrics = new EtherealMetricsImpl(context.getId(), "test", registry);
        var builder = Config.newBuilder()
                            .setnProc((short) NPROC)
                            .setNumberOfEpochs(NUM_EPOCHS)
                            .setEpochLength(EPOCH_LENGTH);

        List<List<List<ByteString>>> produced = new ArrayList<>();
        for (int i = 0; i < (short) NPROC; i++) {
            produced.add(new CopyOnWriteArrayList<>());
        }

        final var prefix = UUID.randomUUID().toString();
        int maxSize = 1024 * 1024;
        for (short i = 0; i < (short) NPROC; i++) {
            var level = new AtomicInteger();
            var ds = new SimpleDataSource();
            final short pid = i;
            List<List<ByteString>> output = produced.get(pid);
            final var member = members.get(i);
            var com = new LocalServer(prefix, member).router(ServerConnectionCache.newBuilder());
            comms.add(com);
            var controller = new Ethereal(builder.setSigner((Signer) members.get(i)).setPid(pid).build(), maxSize, ds,
                                          (pb, last) -> {
                                              System.out.println("block: " + level.incrementAndGet() + " pid: " + pid);
                                              output.add(pb);
                                              if (last) {
                                                  finished.countDown();
                                              }
                                          }, ep -> {
                if (pid == 0) {
                    System.out.println("new epoch: " + ep);
                }
            }, "Test: " + i);

            var gossiper = new ChRbcGossip(context, (SigningMember) member, controller.processor(), com, metrics);
            gossipers.add(gossiper);
            dataSources.add(ds);
            controllers.add(controller);
            for (int d = 0; d < 5000; d++) {
                ds.dataStack.add(ByteMessage.newBuilder()
                                            .setContents(ByteString.copyFromUtf8("pid: " + pid + " data: " + d))
                                            .build()
                                            .toByteString());
            }
        }
        try {
            controllers.forEach(Ethereal::start);
            comms.forEach(Router::start);
            gossipers.forEach(e -> {
                e.start(gossipPeriod);
            });
            finished.await(LARGE_TESTS ? 90 : 10, TimeUnit.SECONDS);
        } finally {
            controllers.forEach(c -> System.out.println(c.dump()));
            controllers.forEach(Ethereal::stop);
            gossipers.forEach(ChRbcGossip::stop);
            comms.forEach(e -> e.close(Duration.ofSeconds(1)));
        }

        final var expected = NUM_EPOCHS * (EPOCH_LENGTH - 1);
        final var first = produced.stream().filter(l -> l.size() == expected).findFirst();
        assertFalse(first.isEmpty(),
                    "Iteration: " + iteration + ", no process produced " + expected + " blocks: " + produced.stream()
                                                                                                            .map(
                                                                                                            l -> l.size())
                                                                                                            .toList());
        List<List<ByteString>> preblocks = first.get();
        List<String> outputOrder = new ArrayList<>();
        Set<Short> failed = new HashSet<>();
        for (short i = 0; i < NPROC; i++) {
            final List<List<ByteString>> output = produced.get(i);
            if (output.size() != expected) {
                System.out.println(
                "Iteration: " + iteration + ", did not get all expected blocks on: " + i + " blocks received: "
                + output.size());
            } else {
                for (int j = 0; j < preblocks.size(); j++) {
                    var a = preblocks.get(j);
                    var b = output.get(j);
                    if (a.size() != b.size()) {
                        failed.add(i);
                        System.out.println(
                        "Iteration: " + iteration + ", mismatch at block: " + j + " process: " + i + " data size: "
                        + a.size() + " != " + b.size());
                    } else {
                        for (int k = 0; k < a.size(); k++) {
                            if (!a.get(k).equals(b.get(k))) {
                                failed.add(i);
                                System.out.println(
                                "Iteration: " + iteration + ", mismatch at block: " + j + " unit: " + k + " process: "
                                + i + " expected: " + a.get(k) + " received: " + b.get(k));
                            }
                            outputOrder.add(new String(ByteMessage.parseFrom(a.get(k)).getContents().toByteArray()));
                        }
                    }
                }
            }
        }
        assertTrue((NPROC - failed.size()) >= context.majority(), "Failed iteration: " + iteration);
        assertTrue(produced.stream().map(List::size).filter(count -> count == expected).count() >= context.majority(),
                   "Failed iteration: " + iteration + ", failed to obtain majority agreement on output count");
    }

    private static class SimpleDataSource implements DataSource {
        private final Deque<ByteString> dataStack = new ArrayDeque<>();

        @Override
        public ByteString getData() {
            try {
                Thread.sleep(Entropy.nextBitsStreamLong(DELAY_MS * 2));
            } catch (InterruptedException e) {
            }
            return dataStack.pollFirst();
        }
    }
}
