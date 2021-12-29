/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.h2.mvstore.MVStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.ethereal.proto.ByteMessage;
import com.salesforce.apollo.choam.CHOAM.TransactionExecutor;
import com.salesforce.apollo.choam.Parameters.BootstrapParameters;
import com.salesforce.apollo.choam.Parameters.ProducerParameters;
import com.salesforce.apollo.choam.support.InvalidTransaction;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class MembershipTests {
    private class Transactioneer {
        private final static Random entropy = new Random();

        private final AtomicInteger            completed = new AtomicInteger();
        private final CountDownLatch           countdown;
        private final AtomicInteger            failed    = new AtomicInteger();
        private final int                      max;
        private final AtomicBoolean            proceed;
        private final ScheduledExecutorService scheduler;
        private final Session                  session;
        private final Duration                 timeout;
        private final ByteMessage              tx        = ByteMessage.newBuilder()
                                                                      .setContents(ByteString.copyFromUtf8("Give me food or give me slack or kill me"))
                                                                      .build();

        Transactioneer(Session session, Duration timeout, AtomicBoolean proceed, int max,
                       ScheduledExecutorService scheduler, CountDownLatch countdown) {
            this.proceed = proceed;
            this.session = session;
            this.timeout = timeout;
            this.max = max;
            this.scheduler = scheduler;
            this.countdown = countdown;
        }

        void decorate(CompletableFuture<?> fs) {
            fs.whenCompleteAsync((o, t) -> {
                if (!proceed.get()) {
                    return;
                }

                if (t != null) {
                    failed.incrementAndGet();
                    scheduler.schedule(() -> {
                        try {
                            decorate(session.submit(ForkJoinPool.commonPool(), tx, timeout, scheduler));
                        } catch (InvalidTransaction e) {
                            e.printStackTrace();
                        }
                    }, entropy.nextInt(10), TimeUnit.MILLISECONDS);
                } else {
                    if (completed.incrementAndGet() == max) {
                        countdown.countDown();
                    } else {
                        try {
                            decorate(session.submit(ForkJoinPool.commonPool(), tx, timeout, scheduler));
                        } catch (InvalidTransaction e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }

        void start() {
            scheduler.schedule(() -> {
                try {
                    decorate(session.submit(ForkJoinPool.commonPool(), tx, timeout, scheduler));
                } catch (InvalidTransaction e) {
                    throw new IllegalStateException(e);
                }
            }, entropy.nextInt(2000), TimeUnit.MILLISECONDS);
        }
    }

    private Map<Digest, List<Digest>>      blocks;
    private Map<Digest, CHOAM>             choams;
    private List<SigningMember>            members;
    private Map<Digest, Router>            routers;
    private int                            toleranceLevel;
    private Map<Digest, List<Transaction>> transactions;

    @AfterEach
    public void after() throws Exception {
        shutdown();
        members = null;
        transactions = null;
        blocks = null;
    }

    @Test
    public void genesisBootstrap() throws Exception {
        SigningMember testSubject = initialize(2000, 5);
        System.out.println("Test subject: " + testSubject);
        routers.entrySet()
               .stream()
               .filter(e -> !e.getKey().equals(testSubject.getId()))
               .forEach(r -> r.getValue().start());
        choams.entrySet()
              .stream()
              .filter(e -> !e.getKey().equals(testSubject.getId()))
              .forEach(ch -> ch.getValue().start());
        final int expected = 3;

        Utils.waitForCondition(30_000, 1000,
                               () -> blocks.entrySet()
                                           .stream()
                                           .filter(e -> !e.getKey().equals(testSubject.getId()))
                                           .map(e -> e.getValue())
                                           .mapToInt(l -> l.size())
                                           .filter(s -> s >= expected)
                                           .count() == choams.size() - 1);
        assertTrue(blocks.entrySet()
                         .stream()
                         .filter(e -> !e.getKey().equals(testSubject.getId()))
                         .map(e -> e.getValue())
                         .mapToInt(l -> l.size())
                         .filter(s -> s >= expected)
                         .count() > toleranceLevel,
                   "Failed: " + blocks.entrySet()
                                      .stream()
                                      .filter(e -> !e.getKey().equals(testSubject.getId()))
                                      .map(e -> e.getValue())
                                      .map(l -> l.size())
                                      .filter(s -> s >= expected)
                                      .toList());

        final Duration timeout = Duration.ofSeconds(2);
        final var scheduler = Executors.newScheduledThreadPool(20);

        AtomicBoolean proceed = new AtomicBoolean(true);
        var transactioneers = new ArrayList<Transactioneer>();
        final int clientCount = 1;
        final int max = 1;
        final var countdown = new CountDownLatch(clientCount);
        for (int i = 0; i < clientCount; i++) {
            choams.entrySet()
                  .stream()
                  .filter(e -> !e.getKey().equals(testSubject.getId()))
                  .map(e -> e.getValue())
                  .map(c -> new Transactioneer(c.getSession(), timeout, proceed, max, scheduler, countdown))
                  .forEach(e -> transactioneers.add(e));
        }

        transactioneers.forEach(e -> e.start());
        boolean success;
        try {
            success = countdown.await(30, TimeUnit.SECONDS);
        } finally {
            proceed.set(false);
        }
        assertTrue(success,
                   "Only completed: " + transactioneers.stream().filter(e -> e.completed.get() >= max).count());
        var target = blocks.values().stream().mapToInt(l -> l.size()).max().getAsInt() + 1;

        routers.get(testSubject.getId()).start();
        choams.get(testSubject.getId()).start();
        success = Utils.waitForCondition(60_000, () -> blocks.get(testSubject.getId()).size() >= target);
        assertTrue(success, "Test subject completed: " + blocks.get(testSubject.getId()).size() + " expected >= "
        + blocks.get(members.get(0).getId()).size());

    }

    public SigningMember initialize(int checkpointBlockSize, int cardinality) {
        transactions = new ConcurrentHashMap<>();
        blocks = new ConcurrentHashMap<>();
        Random entropy = new Random();
        var context = new Context<>(DigestAlgorithm.DEFAULT.getOrigin(), 0.2, cardinality, 3);
        toleranceLevel = context.toleranceLevel();
        var scheduler = Executors.newScheduledThreadPool(cardinality * 5);

        var params = Parameters.newBuilder()
                               .setContext(context)
                               .setSynchronizeTimeout(Duration.ofSeconds(1))
                               .setExec(Router.createFjPool())
                               .setBootstrap(BootstrapParameters.newBuilder()
                                                                .setGossipDuration(Duration.ofMillis(10))
                                                                .setMaxSyncBlocks(1000)
                                                                .setMaxViewBlocks(1000)
                                                                .build())
                               .setGenesisViewId(DigestAlgorithm.DEFAULT.getOrigin().prefix(entropy.nextLong()))
                               .setGossipDuration(Duration.ofMillis(5))
                               .setScheduler(scheduler)
                               .setProducer(ProducerParameters.newBuilder()
                                                              .setGossipDuration(Duration.ofMillis(10))
                                                              .setBatchInterval(Duration.ofMillis(150))
                                                              .setMaxBatchByteSize(1024 * 1024)
                                                              .setMaxBatchCount(10000)
                                                              .build())
                               .setTxnPermits(10_000)
                               .setCheckpointBlockSize(checkpointBlockSize);

        members = IntStream.range(0, cardinality)
                           .mapToObj(i -> Utils.getMember(i))
                           .map(cpk -> new SigningMemberImpl(cpk))
                           .map(e -> (SigningMember) e)
                           .peek(m -> context.activate(m))
                           .toList();
        SigningMember testSubject = members.get(cardinality - 1);
        final var prefix = UUID.randomUUID().toString();
        routers = members.stream().collect(Collectors.toMap(m -> m.getId(), m -> {
            AtomicInteger exec = new AtomicInteger();
            var localRouter = new LocalRouter(prefix, m,
                                              ServerConnectionCache.newBuilder()
                                                                   .setTarget(cardinality)
                                                                   .setMetrics(params.getMetrics()),
                                              Executors.newFixedThreadPool(2, r -> {
                                                  Thread thread = new Thread(r, "Router exec" + m.getId() + "["
                                                  + exec.getAndIncrement() + "]");
                                                  thread.setDaemon(true);
                                                  return thread;
                                              }));
            return localRouter;
        }));
        Executor clients = Executors.newCachedThreadPool();
        choams = members.stream().collect(Collectors.toMap(m -> m.getId(), m -> {
            blocks.put(m.getId(), new CopyOnWriteArrayList<>());
            final TransactionExecutor processor = new TransactionExecutor() {

                @Override
                public void beginBlock(long height, Digest hash) {
                    blocks.get(m.getId()).add(hash);
                }

                @SuppressWarnings({ "unchecked", "rawtypes" })
                @Override
                public void execute(int index, Digest hash, Transaction t, CompletableFuture f) {
                    transactions.computeIfAbsent(m.getId(), d -> new ArrayList<>()).add(t);
                    if (f != null) {
                        f.completeAsync(() -> new Object(), clients);
                    }
                }
            };
            params.getProducer().ethereal().setSigner(m);
            if (m.equals(testSubject)) {
                params.setSynchronizationCycles(100);
            } else {
                params.setSynchronizationCycles(3);
            }
            return new CHOAM(params.setMember(m)
                                   .setCommunications(routers.get(m.getId()))
                                   .setProcessor(processor)
                                   .build(),
                             MVStore.open(null));
        }));
        return testSubject;
    }

    private void shutdown() {
        if (choams != null) {
            choams.values().forEach(e -> e.stop());
            choams = null;
        }
        if (routers != null) {
            routers.values().forEach(e -> e.close());
            routers = null;
        }
    }
}
