/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.joou.ULong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.ethereal.proto.ByteMessage;
import com.salesforce.apollo.choam.CHOAM.TransactionExecutor;
import com.salesforce.apollo.choam.Parameters.ProducerParameters;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
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
public class TestCHOAM {
    private class Transactioneer {
        private final static Random entropy = new Random();

        private final AtomicInteger            completed = new AtomicInteger();
        private final CountDownLatch           countdown;
        private final AtomicInteger            failed    = new AtomicInteger();
        private final Timer                    latency;
        private final AtomicInteger            lineTotal;
        private final int                      max;
        private final AtomicBoolean            proceed;
        private final ScheduledExecutorService scheduler;
        private final Session                  session;
        private final Duration                 timeout;
        private final Counter                  timeouts;
        private final ByteMessage              tx        = ByteMessage.newBuilder()
                                                                      .setContents(ByteString.copyFromUtf8("Give me food or give me slack or kill me"))
                                                                      .build();

        Transactioneer(Session session, Duration timeout, Counter timeouts, Timer latency, AtomicBoolean proceed,
                       AtomicInteger lineTotal, int max, CountDownLatch countdown,
                       ScheduledExecutorService txScheduler) {
            this.latency = latency;
            this.proceed = proceed;
            this.session = session;
            this.timeout = timeout;
            this.lineTotal = lineTotal;
            this.timeouts = timeouts;
            this.max = max;
            this.countdown = countdown;
            this.scheduler = txScheduler;
        }

        void decorate(CompletableFuture<?> fs, Timer.Context time) {
            fs.whenCompleteAsync((o, t) -> {
                if (!proceed.get()) {
                    return;
                }

                if (t != null) {
                    timeouts.inc();
                    var tc = latency.time();
                    failed.incrementAndGet();

                    if (completed.get() < max) {
                        scheduler.schedule(() -> {
                            try {
                                decorate(session.submit(ForkJoinPool.commonPool(), tx, timeout, scheduler), tc);
                            } catch (InvalidTransaction e) {
                                e.printStackTrace();
                            }
                        }, entropy.nextInt(250), TimeUnit.MILLISECONDS);
                    }
                } else {
                    time.close();
                    final int tot = lineTotal.incrementAndGet();
                    if (tot % 100 == 0 && tot % (100 * 100) == 0) {
                        System.out.println(".");
                    } else if (tot % 100 == 0) {
                        System.out.print(".");
                    }
                    var tc = latency.time();
                    final var complete = completed.incrementAndGet();
                    if (complete < max) {
                        scheduler.schedule(() -> {
                            try {
                                decorate(session.submit(ForkJoinPool.commonPool(), tx, timeout, scheduler), tc);
                            } catch (InvalidTransaction e) {
                                e.printStackTrace();
                            }
                        }, entropy.nextInt(100), TimeUnit.MILLISECONDS);
                    } else if (complete == max) {
                        countdown.countDown();
                    }
                }
            });
        }

        void start() {
            scheduler.schedule(() -> {
                Timer.Context time = latency.time();
                try {
                    decorate(session.submit(ForkJoinPool.commonPool(), tx, timeout, scheduler), time);
                } catch (InvalidTransaction e) {
                    throw new IllegalStateException(e);
                }
            }, entropy.nextInt(2000), TimeUnit.MILLISECONDS);
        }
    }

    private static final int CARDINALITY = 5;

    private Map<Digest, AtomicInteger> blocks;
    private Map<Digest, CHOAM>         choams;
    private List<SigningMember>        members;
    private Map<Digest, Router>        routers;

    private Map<Digest, List<Transaction>> transactions;

    @AfterEach
    public void after() throws Exception {
        if (routers != null) {
            routers.values().forEach(e -> e.close());
            routers = null;
        }
        if (choams != null) {
            choams.values().forEach(e -> e.stop());
            choams = null;
        }
        members = null;
    }

    @BeforeEach
    public void before() {
        transactions = new ConcurrentHashMap<>();
        blocks = new ConcurrentHashMap<>();
        Random entropy = new Random();
        var context = new Context<>(DigestAlgorithm.DEFAULT.getOrigin(), 0.2, CARDINALITY, 3);
        var scheduler = Executors.newScheduledThreadPool(CARDINALITY);

        var exec = Router.createFjPool();
        var params = Parameters.newBuilder()
                               .setSynchronizationCycles(1)
                               .setSynchronizeTimeout(Duration.ofSeconds(1))
                               .setGenesisViewId(DigestAlgorithm.DEFAULT.getOrigin().prefix(entropy.nextLong()))
                               .setGossipDuration(Duration.ofMillis(10))
                               .setProducer(ProducerParameters.newBuilder()
                                                              .setGossipDuration(Duration.ofMillis(10))
                                                              .setBatchInterval(Duration.ofMillis(100))
                                                              .build())
                               .setTxnPermits(100)
                               .setCheckpointBlockSize(1);
        params.getClientBackoff()
              .setBase(20)
              .setCap(150)
              .setInfiniteAttempts()
              .setJitter()
              .setExceptionHandler(t -> System.out.println(t.getClass().getSimpleName()));

        params.getProducer().ethereal().setNumberOfEpochs(5).setFpr(0.000125);

        members = IntStream.range(0, CARDINALITY)
                           .mapToObj(i -> Utils.getMember(i))
                           .map(cpk -> new SigningMemberImpl(cpk))
                           .map(e -> (SigningMember) e)
                           .peek(m -> context.activate(m))
                           .toList();
        final var prefix = UUID.randomUUID().toString();
        routers = members.stream().collect(Collectors.toMap(m -> m.getId(), m -> {
            AtomicInteger execC = new AtomicInteger();
            var localRouter = new LocalRouter(prefix, m, ServerConnectionCache.newBuilder().setTarget(CARDINALITY),
                                              Executors.newFixedThreadPool(2, r -> {
                                                  Thread thread = new Thread(r, "Router exec" + m.getId() + "["
                                                  + execC.getAndIncrement() + "]");
                                                  thread.setDaemon(true);
                                                  return thread;
                                              }));
            return localRouter;
        }));
        choams = members.stream().collect(Collectors.toMap(m -> m.getId(), m -> {
            var recording = new AtomicInteger();
            blocks.put(m.getId(), recording);
            final TransactionExecutor processor = new TransactionExecutor() {

                @Override
                public void beginBlock(ULong height, Digest hash) {
                    recording.incrementAndGet();
                }

                @SuppressWarnings({ "unchecked", "rawtypes" })
                @Override
                public void execute(int index, Digest hash, Transaction t, CompletableFuture f) {
                    transactions.computeIfAbsent(m.getId(), d -> new ArrayList<>()).add(t);
                    if (f != null) {
                        f.complete(new Object());
                    }
                }
            };
            params.getProducer().ethereal().setSigner(m);
            return new CHOAM(params.build(RuntimeParameters.newBuilder()
                                                           .setMember(m)
                                                           .setCommunications(routers.get(m.getId()))
                                                           .setProcessor(processor)
                                                           .setContext(context)
                                                           .setExec(exec)
                                                           .setScheduler(scheduler)
                                                           .build()));
        }));
    }

    @Test
    public void regenerateGenesis() throws Exception {
        routers.values().forEach(r -> r.start());
        choams.values().forEach(ch -> ch.start());
        final int expected = 88 + (30 * 2);
        Utils.waitForCondition(300_000, 1_000,
                               () -> blocks.values()
                                           .stream()
                                           .mapToInt(l -> l.get())
                                           .filter(s -> s >= expected)
                                           .count() == choams.size());
        assertEquals(choams.size(), blocks.values().stream().mapToInt(l -> l.get()).filter(s -> s >= expected).count(),
                     "Failed: " + blocks.get(members.get(0).getId()).get());
    }

    @Test
    public void submitMultiplTxn() throws Exception {
        routers.values().forEach(r -> r.start());
        choams.values().forEach(ch -> ch.start());
        final int expected = 10;

        Utils.waitForCondition(30_000, 1_000,
                               () -> blocks.values()
                                           .stream()
                                           .mapToInt(l -> l.get())
                                           .filter(s -> s >= expected)
                                           .count() == choams.size());
        assertEquals(choams.size(), blocks.values().stream().mapToInt(l -> l.get()).filter(s -> s >= expected).count(),
                     "Failed: " + blocks.get(members.get(0).getId()).get());

        final Duration timeout = Duration.ofSeconds(3);

        AtomicBoolean proceed = new AtomicBoolean(true);
        MetricRegistry reg = new MetricRegistry();
        Timer latency = reg.timer("Transaction latency");
        Counter timeouts = reg.counter("Transaction timeouts");
        AtomicInteger lineTotal = new AtomicInteger();
        var transactioneers = new ArrayList<Transactioneer>();
        final int clientCount = 5_000;
        final int max = 10;
        final CountDownLatch countdown = new CountDownLatch(clientCount * choams.size());
        final ScheduledExecutorService txScheduler = Executors.newScheduledThreadPool(CARDINALITY);

        for (int i = 0; i < clientCount; i++) {
            choams.values()
                  .stream()
                  .map(c -> new Transactioneer(c.getSession(), timeout, timeouts, latency, proceed, lineTotal, max,
                                               countdown, txScheduler))
                  .forEach(e -> transactioneers.add(e));
        }

        transactioneers.stream().forEach(e -> e.start());
        try {
            countdown.await(300, TimeUnit.SECONDS);
        } finally {
            proceed.set(false);
            after();
            System.out.println();
            System.out.println();
            System.out.println("# of clients: " + transactioneers.size());
            ConsoleReporter.forRegistry(reg)
                           .convertRatesTo(TimeUnit.SECONDS)
                           .convertDurationsTo(TimeUnit.MILLISECONDS)
                           .build()
                           .report();
        }
    }

    @Test
    public void submitTxn() throws Exception {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(CARDINALITY);
        routers.values().forEach(r -> r.start());
        choams.values().forEach(ch -> ch.start());
        final int expected = 3;
        var session = choams.get(members.get(0).getId()).getSession();

        Utils.waitForCondition(30_000, 1_000,
                               () -> blocks.values()
                                           .stream()
                                           .mapToInt(l -> l.get())
                                           .filter(s -> s >= expected)
                                           .count() == choams.size());
        assertEquals(choams.size(), blocks.values().stream().mapToInt(l -> l.get()).filter(s -> s >= expected).count(),
                     "Failed: " + blocks.get(members.get(0).getId()).get());
        final ByteMessage tx = ByteMessage.newBuilder()
                                          .setContents(ByteString.copyFromUtf8("Give me food or give me slack or kill me"))
                                          .build();
        CompletableFuture<?> result = session.submit(ForkJoinPool.commonPool(), tx, Duration.ofSeconds(30), scheduler);
        result.get(30, TimeUnit.SECONDS);
    }
}
