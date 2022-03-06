/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.joou.ULong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesforce.apollo.choam.CHOAM.TransactionExecutor;
import com.salesforce.apollo.choam.Parameters.BootstrapParameters;
import com.salesforce.apollo.choam.Parameters.ProducerParameters;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
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
public class MembershipTests {
    static {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
            LoggerFactory.getLogger(MembershipTests.class).error("Error on thread: {}", t.getName(), e);
        });
    }

    private Map<Digest, AtomicInteger>     blocks;
    private Map<Digest, CHOAM>             choams;
    private List<SigningMember>            members;
    private Map<Digest, Router>            routers;
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

        final Duration timeout = Duration.ofSeconds(5);
        final var scheduler = Executors.newScheduledThreadPool(2);

        var txneer = choams.entrySet().stream().filter(e -> !e.getKey().equals(testSubject.getId())).findFirst().get();

        final var countdown = new CountDownLatch(1);
        var transactioneer = new Transactioneer(txneer.getValue().getSession(), timeout, 1, scheduler, countdown);

        transactioneer.start();
        System.out.println("completed: " + countdown.await(60, TimeUnit.SECONDS));
        assertEquals(0, countdown.getCount(), "Did not complete: " + countdown.getCount());
        var target = blocks.values().stream().mapToInt(l -> l.get()).max().getAsInt();

        routers.get(testSubject.getId()).start();
        choams.get(testSubject.getId()).start();
        var success = Utils.waitForCondition(10_000, () -> blocks.get(testSubject.getId()).get() >= target);
        assertTrue(success, "Expecting: " + target + "completed: " + blocks);

    }

    public SigningMember initialize(int checkpointBlockSize, int cardinality) {
        transactions = new ConcurrentHashMap<>();
        blocks = new ConcurrentHashMap<>();
        Random entropy = new Random();
        var context = new ContextImpl<>(DigestAlgorithm.DEFAULT.getOrigin(), 0.2, cardinality, 3);
        var scheduler = Executors.newScheduledThreadPool(cardinality * 5);

        var exec = Router.createFjPool();
        var params = Parameters.newBuilder()
                               .setSynchronizeTimeout(Duration.ofSeconds(1))
                               .setBootstrap(BootstrapParameters.newBuilder()
                                                                .setGossipDuration(Duration.ofMillis(10))
                                                                .setMaxSyncBlocks(1000)
                                                                .setMaxViewBlocks(1000)
                                                                .build())
                               .setGenesisViewId(DigestAlgorithm.DEFAULT.getOrigin().prefix(entropy.nextLong()))
                               .setGossipDuration(Duration.ofMillis(5))
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
            AtomicInteger execC = new AtomicInteger();
            var localRouter = new LocalRouter(prefix, m, ServerConnectionCache.newBuilder().setTarget(cardinality),
                                              Executors.newFixedThreadPool(2, r -> {
                                                  Thread thread = new Thread(r, "Router exec" + m.getId() + "["
                                                  + execC.getAndIncrement() + "]");
                                                  thread.setDaemon(true);
                                                  return thread;
                                              }));
            return localRouter;
        }));
        Executor clients = Executors.newCachedThreadPool();
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
            return new CHOAM(params.build(RuntimeParameters.newBuilder()
                                                           .setMember(m)
                                                           .setCommunications(routers.get(m.getId()))
                                                           .setProcessor(processor)
                                                           .setContext(context)
                                                           .setExec(exec)
                                                           .setScheduler(scheduler)
                                                           .build()));
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
