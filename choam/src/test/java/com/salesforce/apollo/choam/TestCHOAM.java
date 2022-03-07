/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.joou.ULong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.ethereal.proto.ByteMessage;
import com.salesforce.apollo.choam.CHOAM.TransactionExecutor;
import com.salesforce.apollo.choam.Parameters.ProducerParameters;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.choam.support.ChoamMetricsImpl;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.comm.ServerConnectionCacheMetricsImpl;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.ContextImpl;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class TestCHOAM {
    private static final int     CARDINALITY = 5;
    private static final boolean LARGE_TESTS = Boolean.getBoolean("large_tests");

    static {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
            LoggerFactory.getLogger(TestCHOAM.class).error("Error on thread: {}", t.getName(), e);
        });
    }

    protected CompletableFuture<Boolean>   checkpointOccurred;
    private Map<Digest, AtomicInteger>     blocks;
    private Map<Digest, CHOAM>             choams;
    private List<SigningMember>            members;
    private MetricRegistry                 registry;
    private Map<Digest, Router>            routers;
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
        registry = null;
    }

    @BeforeEach
    public void before() {
        var context = new ContextImpl<>(DigestAlgorithm.DEFAULT.getOrigin(), 0.2, CARDINALITY, 3);
        registry = new MetricRegistry();
        var metrics = new ChoamMetricsImpl(context.getId(), registry);
        transactions = new ConcurrentHashMap<>();
        blocks = new ConcurrentHashMap<>();
        Random entropy = new Random();
        var scheduler = Executors.newScheduledThreadPool(CARDINALITY);

        var exec = Router.createFjPool();
        var params = Parameters.newBuilder()
                               .setSynchronizationCycles(1)
                               .setSynchronizeTimeout(Duration.ofSeconds(1))
                               .setGenesisViewId(DigestAlgorithm.DEFAULT.getOrigin().prefix(entropy.nextLong()))
                               .setGossipDuration(Duration.ofMillis(10))
                               .setProducer(ProducerParameters.newBuilder()
                                                              .setMaxBatchCount(10_000)
                                                              .setMaxBatchByteSize(1024 * 1024)
                                                              .setGossipDuration(Duration.ofMillis(10))
                                                              .setBatchInterval(Duration.ofMillis(50))
                                                              .build())
                               .setTxnPermits(100)
                               .setCheckpointBlockSize(1);
        params.getCombineParams().setMetrics(metrics.getCombineMetrics());
        params.getClientBackoff()
              .setBase(10)
              .setCap(100)
              .setInfiniteAttempts()
              .setJitter()
              .setExceptionHandler(t -> System.out.println(t.getClass().getSimpleName()));
        params.getProducer().ethereal().setNumberOfEpochs(5).setFpr(0.000125);

        checkpointOccurred = new CompletableFuture<>();
        members = IntStream.range(0, CARDINALITY)
                           .mapToObj(i -> Utils.getMember(i))
                           .map(cpk -> new SigningMemberImpl(cpk))
                           .map(e -> (SigningMember) e)
                           .peek(m -> context.activate(m))
                           .toList();
        final var prefix = UUID.randomUUID().toString();
        routers = members.stream().collect(Collectors.toMap(m -> m.getId(), m -> {
            AtomicInteger execC = new AtomicInteger();
            var localRouter = new LocalRouter(prefix, m,
                                              ServerConnectionCache.newBuilder()
                                                                   .setMetrics(new ServerConnectionCacheMetricsImpl(registry))
                                                                   .setTarget(CARDINALITY),
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
                public void endBlock(ULong height, Digest hash) {
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
            var runtime = RuntimeParameters.newBuilder();
            return new CHOAM(params.build(runtime.setMember(m)
                                                 .setMetrics(metrics)
                                                 .setCommunications(routers.get(m.getId()))
                                                 .setProcessor(processor)
                                                 .setCheckpointer(wrap(runtime.getCheckpointer()))
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
        Thread.sleep(2_000);
        assertTrue(checkpointOccurred.get(120, TimeUnit.SECONDS));
    }

    @Test
    public void submitMultiplTxn() throws Exception {
        routers.values().forEach(r -> r.start());
        choams.values().forEach(ch -> ch.start());

        final Duration timeout = Duration.ofSeconds(2);

        var transactioneers = new ArrayList<Transactioneer>();
        final int clientCount = LARGE_TESTS ? 5_000 : 50;
        final int max = LARGE_TESTS ? 100 : 10;
        final CountDownLatch countdown = new CountDownLatch(clientCount * choams.size());
        final ScheduledExecutorService txScheduler = Executors.newScheduledThreadPool(CARDINALITY);

        for (int i = 0; i < clientCount; i++) {
            choams.values()
                  .stream()
                  .map(c -> new Transactioneer(c.getSession(), timeout, max, txScheduler, countdown))
                  .forEach(e -> transactioneers.add(e));
        }

        Thread.sleep(2_000);

        transactioneers.stream().forEach(e -> e.start());
        try {
            countdown.await(60, TimeUnit.SECONDS);
        } finally {
            routers.values().forEach(e -> e.close());
            choams.values().forEach(e -> e.stop());
            System.out.println();

            ConsoleReporter.forRegistry(registry)
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
        var session = choams.get(members.get(0).getId()).getSession();
        Thread.sleep(2_000);
        final ByteMessage tx = ByteMessage.newBuilder()
                                          .setContents(ByteString.copyFromUtf8("Give me food or give me slack or kill me"))
                                          .build();
        CompletableFuture<?> result = session.submit(ForkJoinPool.commonPool(), tx, Duration.ofSeconds(3), scheduler);
        result.get(60, TimeUnit.SECONDS);
    }

    private Function<ULong, File> wrap(Function<ULong, File> checkpointer) {
        return ul -> {
            checkpointOccurred.complete(true);
            return checkpointer.apply(ul);
        };
    }
}
