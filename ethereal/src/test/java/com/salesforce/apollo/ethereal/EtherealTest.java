/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.ethereal;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.messaging.proto.ByteMessage;
import com.salesforce.apollo.archipelago.LocalServer;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.ServerConnectionCache;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.ethereal.Ethereal.PreBlock;
import com.salesforce.apollo.ethereal.memberships.ChRbcGossip;
import com.salesforce.apollo.ethereal.memberships.comm.EtherealMetricsImpl;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.ContextImpl;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.utils.Entropy;

/**
 * 
 * @author hal.hildebrand
 *
 */
public class EtherealTest {

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

    private final static long    DELAY_MS;
    private static final int     EPOCH_LENGTH = 30;
    private static final boolean LARGE_TESTS;
    private static final int     NPROC;
    private static final int     NUM_EPOCHS   = 3;

    static {
        LARGE_TESTS = Boolean.getBoolean("large_tests");
        DELAY_MS = LARGE_TESTS ? 5 : 5;
        NPROC = LARGE_TESTS ? 7 : 4;
    }

    @Test
    public void context() throws Exception {
        var consumers = new ArrayList<ThreadPoolExecutor>();
        for (var i = 0; i < NPROC; i++) {
            consumers.add(Ethereal.consumer(Integer.toString(i)));
        }
        one(0, consumers);
    }

    @Test
    public void lots() throws Exception {
        if (!LARGE_TESTS) {
            return;
        }
        var consumers = new ArrayList<ThreadPoolExecutor>();
        for (var i = 0; i < NPROC; i++) {
            consumers.add(Ethereal.consumer(Integer.toString(i)));
        }
        for (int i = 0; i < 10; i++) {
            System.out.println("Iteration: " + i);
            one(i, consumers);
            System.out.println();
        }
    }

    private void one(int iteration, List<ThreadPoolExecutor> consumers) throws NoSuchAlgorithmException,
                                                                        InterruptedException,
                                                                        InvalidProtocolBufferException {
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

        List<SigningMember> members = IntStream.range(0, (short) NPROC).mapToObj(i -> {
            try {
                return stereotomy.newIdentifier().get();
            } catch (InterruptedException | ExecutionException e) {
                throw new IllegalStateException(e);
            }
        }).map(cpk -> new ControlledIdentifierMember(cpk)).map(e -> (SigningMember) e).toList();

        Context<Member> context = new ContextImpl<>(DigestAlgorithm.DEFAULT.getOrigin(), members.size(), 0.1, 3);
        var metrics = new EtherealMetricsImpl(context.getId(), "test", registry);
        for (Member m : members) {
            context.activate(m);
        }
        var builder = Config.newBuilder()
                            .setnProc((short) NPROC)
                            .setNumberOfEpochs(NUM_EPOCHS)
                            .setEpochLength(EPOCH_LENGTH)
                            .setVerifiers(members.toArray(new Verifier[members.size()]));

        List<List<PreBlock>> produced = new ArrayList<>();
        for (int i = 0; i < (short) NPROC; i++) {
            produced.add(new CopyOnWriteArrayList<>());
        }

        List<ExecutorService> executors = new ArrayList<>();
        final var prefix = UUID.randomUUID().toString();
        int maxSize = 1024 * 1024;
        for (short i = 0; i < (short) NPROC; i++) {
            var level = new AtomicInteger();
            var ds = new SimpleDataSource();
            final short pid = i;
            List<PreBlock> output = produced.get(pid);
            final var exec = Executors.newFixedThreadPool(2, Thread.ofVirtual().factory());
            executors.add(exec);
            final var member = members.get(i);
            var com = new LocalServer(prefix, member, exec).router(ServerConnectionCache.newBuilder(), exec);
            comms.add(com);
            var controller = new Ethereal(builder.setSigner(members.get(i)).setPid(pid).build(), maxSize, ds,
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
                                          }, consumers.get(i));

            var e = Executors.newFixedThreadPool(3, Thread.ofVirtual().factory());
            executors.add(e);
            var gossiper = new ChRbcGossip(context, member, controller.processor(), com, e, metrics);
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
            controllers.forEach(e -> e.start());
            comms.forEach(e -> e.start());
            gossipers.forEach(e -> {
                final var sched = Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().factory());
                executors.add(sched);
                e.start(gossipPeriod, sched);
            });
            finished.await(LARGE_TESTS ? 90 : 10, TimeUnit.SECONDS);
        } finally {
            controllers.forEach(c -> System.out.println(c.dump()));
            controllers.forEach(e -> e.stop());
            gossipers.forEach(e -> e.stop());
            comms.forEach(e -> e.close(Duration.ofSeconds(1)));
            executors.forEach(executor -> {
                executor.shutdown();
                try {
                    executor.awaitTermination(1, TimeUnit.SECONDS);
                } catch (InterruptedException e1) {
                }
            });
        }

        final var expected = NUM_EPOCHS * (EPOCH_LENGTH - 1);
        final var first = produced.stream().filter(l -> l.size() == expected).findFirst();
        assertFalse(first.isEmpty(), "Iteration: " + iteration + ", no process produced " + expected + " blocks: "
        + produced.stream().map(l -> l.size()).toList());
        List<PreBlock> preblocks = first.get();
        List<String> outputOrder = new ArrayList<>();
        Set<Short> failed = new HashSet<>();
        for (short i = 0; i < NPROC; i++) {
            final List<PreBlock> output = produced.get(i);
            if (output.size() != expected) {
                System.out.println("Iteration: " + iteration + ", did not get all expected blocks on: " + i
                + " blocks received: " + output.size());
            } else {
                for (int j = 0; j < preblocks.size(); j++) {
                    var a = preblocks.get(j);
                    var b = output.get(j);
                    if (a.data().size() != b.data().size()) {
                        failed.add(i);
                        System.out.println("Iteration: " + iteration + ", mismatch at block: " + j + " process: " + i
                        + " data size: " + a.data().size() + " != " + b.data().size());
                    } else {
                        for (int k = 0; k < a.data().size(); k++) {
                            if (!a.data().get(k).equals(b.data().get(k))) {
                                failed.add(i);
                                System.out.println("Iteration: " + iteration + ", mismatch at block: " + j + " unit: "
                                + k + " process: " + i + " expected: " + a.data().get(k) + " received: "
                                + b.data().get(k));
                            }
                            outputOrder.add(new String(ByteMessage.parseFrom(a.data().get(k))
                                                                  .getContents()
                                                                  .toByteArray()));
                        }
                    }
                }
            }
        }
        assertTrue((NPROC - failed.size()) >= context.majority(), "Failed iteration: " + iteration);
        assertTrue(produced.stream()
                           .map(pbs -> pbs.size())
                           .filter(count -> count == expected)
                           .count() >= context.majority(),
                   "Failed iteration: " + iteration + ", failed to obtain majority agreement on output count");
    }
}
