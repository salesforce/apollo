/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesforce.apollo.archipelago.LocalServer;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.ServerConnectionCache;
import com.salesforce.apollo.archipelago.ServerConnectionCacheMetricsImpl;
import com.salesforce.apollo.choam.CHOAM.TransactionExecutor;
import com.salesforce.apollo.choam.Parameters.ProducerParameters;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.choam.support.ChoamMetricsImpl;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.ContextImpl;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.utils.Utils;
import org.joou.ULong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author hal.hildebrand
 */
public class TestCHOAM {
    private static final int     CARDINALITY;
    private static final boolean LARGE_TESTS = Boolean.getBoolean("large_tests");

    static {
        CARDINALITY = LARGE_TESTS ? 10 : 5;
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
            LoggerFactory.getLogger(TestCHOAM.class).error("Error on thread: {}", t.getName(), e);
        });
    }

    protected CompletableFuture<Boolean> checkpointOccurred;
    private   Map<Digest, AtomicInteger> blocks;
    private   Map<Digest, CHOAM>         choams;
    private   Executor                   exec = Executors.newVirtualThreadPerTaskExecutor();
    private   List<SigningMember>        members;
    private   MetricRegistry             registry;
    private   Map<Digest, Router>        routers;

    @AfterEach
    public void after() throws Exception {
        if (routers != null) {
            routers.values().forEach(e -> e.close(Duration.ofSeconds(1)));
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
    public void before() throws Exception {
        var context = new ContextImpl<>(DigestAlgorithm.DEFAULT.getOrigin(), CARDINALITY, 0.2, 3);
        registry = new MetricRegistry();
        var metrics = new ChoamMetricsImpl(context.getId(), registry);
        blocks = new ConcurrentHashMap<>();
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });

        var params = Parameters.newBuilder()
                               .setGenesisViewId(DigestAlgorithm.DEFAULT.getOrigin().prefix(entropy.nextLong()))
                               .setGossipDuration(Duration.ofMillis(20))
                               .setProducer(ProducerParameters.newBuilder()
                                                              .setMaxBatchCount(15_000)
                                                              .setMaxBatchByteSize(200 * 1024 * 1024)
                                                              .setGossipDuration(Duration.ofMillis(10))
                                                              .setBatchInterval(Duration.ofMillis(50))
                                                              .build())
                               .setCheckpointBlockDelta(1);
        if (LARGE_TESTS) {
            params.getProducer().ethereal().setNumberOfEpochs(5).setEpochLength(60);
        }

        checkpointOccurred = new CompletableFuture<>();
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);

        members = IntStream.range(0, CARDINALITY)
                           .mapToObj(i -> stereotomy.newIdentifier())
                           .map(cpk -> new ControlledIdentifierMember(cpk))
                           .map(e -> (SigningMember) e)
                           .toList();
        members.forEach(m -> context.activate(m));
        final var prefix = UUID.randomUUID().toString();
        routers = members.stream()
                         .collect(Collectors.toMap(m -> m.getId(), m -> new LocalServer(prefix, m, exec).router(
                         ServerConnectionCache.newBuilder()
                                              .setMetrics(new ServerConnectionCacheMetricsImpl(registry))
                                              .setTarget(CARDINALITY), exec)));
        choams = members.stream().collect(Collectors.toMap(m -> m.getId(), m -> {
            var recording = new AtomicInteger();
            blocks.put(m.getId(), recording);
            final TransactionExecutor processor = new TransactionExecutor() {

                @SuppressWarnings({ "unchecked", "rawtypes" })
                @Override
                public void execute(int index, Digest hash, Transaction t, CompletableFuture f, Executor executor) {
                    if (f != null) {
                        f.completeAsync(() -> new Object(), executor);
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
            return new CHOAM(params.build(runtime.setMember(m)
                                                 .setMetrics(metrics)
                                                 .setCommunications(routers.get(m.getId()))
                                                 .setProcessor(processor)
                                                 .setCheckpointer(wrap(runtime.getCheckpointer()))
                                                 .setContext(context)
                                                 .build()));
        }));
    }

    @Test
    public void submitMultiplTxn() throws Exception {
        routers.values().forEach(r -> r.start());
        choams.values().forEach(ch -> ch.start());

        final var timeout = Duration.ofSeconds(15);

        final var transactioneers = new ArrayList<Transactioneer>();
        final var clientCount = LARGE_TESTS ? 1_500 : 50;
        final var max = LARGE_TESTS ? 100 : 10;
        final var countdown = new CountDownLatch(clientCount * choams.size());

        var txExec = Executors.newVirtualThreadPerTaskExecutor();
        choams.values().forEach(c -> {
            for (int i = 0; i < clientCount; i++) {
                transactioneers.add(new Transactioneer(c.getSession(), timeout, max, Executors.newScheduledThreadPool(5,
                                                                                                                      Thread.ofVirtual()
                                                                                                                            .factory()),
                                                       countdown, txExec));
            }
        });

        boolean activated = Utils.waitForCondition(30_000, 1_000,
                                                   () -> choams.values().stream().filter(c -> !c.active()).count()
                                                   == 0);
        assertTrue(activated, "System did not become active: " + choams.entrySet()
                                                                       .stream()
                                                                       .map(e -> e.getValue())
                                                                       .filter(c -> !c.active())
                                                                       .map(c -> c.logState())
                                                                       .toList());

        transactioneers.stream().forEach(e -> e.start());
        try {
            final var complete = countdown.await(LARGE_TESTS ? 3200 : 60, TimeUnit.SECONDS);
            assertTrue(complete, "All clients did not complete: " + transactioneers.stream()
                                                                                   .map(t -> t.getCompleted())
                                                                                   .filter(i -> i < max)
                                                                                   .count());
        } finally {
            routers.values().forEach(e -> e.close(Duration.ofSeconds(1)));
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
