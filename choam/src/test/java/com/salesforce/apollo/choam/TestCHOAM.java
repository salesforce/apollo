/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
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
import com.salesfoce.apollo.choam.proto.Transaction;
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
    private static final int     CARDINALITY = 10;
    private static final boolean LARGE_TESTS = Boolean.getBoolean("large_tests");
    static {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
            LoggerFactory.getLogger(TestCHOAM.class).error("Error on thread: {}", t.getName(), e);
        });
    }
    protected CompletableFuture<Boolean> checkpointOccurred;
    private Map<Digest, AtomicInteger>   blocks;
    private Map<Digest, CHOAM>           choams;
    private List<SigningMember>          members;
    private MetricRegistry               registry;
    private Map<Digest, Router>          routers;

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
        var context = new ContextImpl<>(DigestAlgorithm.DEFAULT.getOrigin(), CARDINALITY, 0.2, 3);
        registry = new MetricRegistry();
        var metrics = new ChoamMetricsImpl(context.getId(), registry);
        blocks = new ConcurrentHashMap<>();
        Random entropy = new Random();

        var params = Parameters.newBuilder()
                               .setSynchronizationCycles(1)
                               .setSynchronizeTimeout(Duration.ofSeconds(1))
                               .setGenesisViewId(DigestAlgorithm.DEFAULT.getOrigin().prefix(entropy.nextLong()))
                               .setGossipDuration(Duration.ofMillis(200))
                               .setProducer(ProducerParameters.newBuilder()
                                                              .setMaxBatchCount(15_000)
                                                              .setMaxBatchByteSize(200 * 1024 * 1024)
                                                              .setGossipDuration(Duration.ofMillis(10))
                                                              .setBatchInterval(Duration.ofMillis(50))
                                                              .build())
                               .setCheckpointBlockSize(1);
        params.getProducer().ethereal().setNumberOfEpochs(5).setFpr(0.0125);

        checkpointOccurred = new CompletableFuture<>();
        members = IntStream.range(0, CARDINALITY)
                           .mapToObj(i -> Utils.getMember(i))
                           .map(cpk -> new SigningMemberImpl(cpk))
                           .map(e -> (SigningMember) e)
                           .peek(m -> context.activate(m))
                           .toList();
        final var prefix = UUID.randomUUID().toString();
        routers = members.stream().collect(Collectors.toMap(m -> m.getId(), m -> {
            var localRouter = new LocalRouter(prefix,
                                              ServerConnectionCache.newBuilder()
                                                                   .setMetrics(new ServerConnectionCacheMetricsImpl(registry))
                                                                   .setTarget(CARDINALITY),
                                              Executors.newFixedThreadPool(1,
                                                                           r -> new Thread(r,
                                                                                           "Comm Exec[" + m.getId()
                                                                                           + "]")),
                                              metrics.limitsMetrics());
            localRouter.setMember(m);
            return localRouter;
        }));
        final var si = new AtomicInteger();
        final var scheduler = Executors.newScheduledThreadPool(5, r -> new Thread(r, "Scheduler[" + si.incrementAndGet()
        + "]"));
        choams = members.stream().collect(Collectors.toMap(m -> m.getId(), m -> {
            var recording = new AtomicInteger();
            blocks.put(m.getId(), recording);
            final TransactionExecutor processor = new TransactionExecutor() {

                @SuppressWarnings({ "unchecked", "rawtypes" })
                @Override
                public void execute(int index, Digest hash, Transaction t, CompletableFuture f) {
                    if (f != null) {
                        f.complete(new Object());
                    }
                }
            };
            params.getProducer().ethereal().setSigner(m);
            var runtime = RuntimeParameters.newBuilder();
            File fn = null;
            try {
                fn = File.createTempFile("tst-", ".tstData");
                fn.deleteOnExit();
            } catch (IOException e1) {
                fail(e1);
            }
//            params.getMvBuilder().setFileName(fn);
            var nExec = new AtomicInteger();
            return new CHOAM(params.build(runtime.setMember(m)
                                                 .setMetrics(metrics)
                                                 .setCommunications(routers.get(m.getId()))
                                                 .setProcessor(processor)
                                                 .setCheckpointer(wrap(runtime.getCheckpointer()))
                                                 .setContext(context)
                                                 .setExec(Executors.newFixedThreadPool(2, r -> new Thread(r, "Exec["
                                                 + nExec.incrementAndGet() + ":" + m.getId() + "]")))
                                                 .setScheduler(scheduler)
                                                 .build()));
        }));
    }

    @Test
    public void submitMultiplTxn() throws Exception {
        routers.values().forEach(r -> r.start());
        choams.values().forEach(ch -> ch.start());

        final var timeout = Duration.ofSeconds(3);

        final var transactioneers = new ArrayList<Transactioneer>();
        final var clientCount = LARGE_TESTS ? 1_000 : 50;
        final var max = LARGE_TESTS ? 1_000 : 10;
        final var countdown = new CountDownLatch(clientCount * choams.size());

        var cnt = new AtomicInteger();
        choams.values().forEach(c -> {
            final var txnCompletion = Executors.newFixedThreadPool(2, r -> new Thread(r, "Completion["
            + cnt.incrementAndGet() + "]"));
            final var txnExecutor = Executors.newFixedThreadPool(1, r -> new Thread(r, "txn exec"));
            final var txScheduler = Executors.newScheduledThreadPool(1, r -> new Thread(r, "txn sched"));
            for (int i = 0; i < clientCount; i++) {
                transactioneers.add(new Transactioneer(c.getSession(), txnCompletion, timeout, max, txScheduler,
                                                       countdown, txnExecutor));
            }
        });

        assertTrue(Utils.waitForCondition(30_000, () -> choams.values().stream().filter(c -> !c.active()).count() == 0),
                   "System did not become active");

        transactioneers.stream().forEach(e -> e.start());
        try {
            final var complete = countdown.await(LARGE_TESTS ? 1200 : 60, TimeUnit.SECONDS);
            assertTrue(complete, "All clients did not complete: "
            + transactioneers.stream().map(t -> t.getCompleted()).filter(i -> i < max).count());
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
        assertTrue(checkpointOccurred.get());
    }

    private Function<ULong, File> wrap(Function<ULong, File> checkpointer) {
        return ul -> {
            var file = checkpointer.apply(ul);
            checkpointOccurred.complete(true);
            return file;
        };
    }
}
