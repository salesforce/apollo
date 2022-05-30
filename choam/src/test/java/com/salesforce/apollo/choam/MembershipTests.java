/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
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
//        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Session.class)).setLevel(Level.TRACE);
//        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(CHOAM.class)).setLevel(Level.TRACE);
//        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(GenesisAssembly.class)).setLevel(Level.TRACE);
//        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ViewAssembly.class)).setLevel(Level.TRACE);
//        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Producer.class)).setLevel(Level.TRACE);
//        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Committee.class)).setLevel(Level.TRACE);
//        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Fsm.class)).setLevel(Level.TRACE);
    }

    private Map<Digest, AtomicInteger> blocks;
    private Map<Digest, CHOAM>         choams;
    private List<SigningMember>        members;
    private Map<Digest, Router>        routers;

    @AfterEach
    public void after() throws Exception {
        shutdown();
        members = null;
        blocks = null;
    }

    @Test
    public void genesisBootstrap() throws Exception {
        @SuppressWarnings("preview")
        var exec = Executors.newVirtualThreadPerTaskExecutor();
        SigningMember testSubject = initialize(2000, 11);
        System.out.println("Test subject: " + testSubject);
        routers.entrySet()
               .stream()
               .filter(e -> !e.getKey().equals(testSubject.getId()))
               .forEach(r -> r.getValue().start());
        choams.entrySet()
              .stream()
              .filter(e -> !e.getKey().equals(testSubject.getId()))
              .forEach(ch -> ch.getValue().start());

        final Duration timeout = Duration.ofSeconds(30);
        @SuppressWarnings("preview")
        final var scheduler = Executors.newScheduledThreadPool(10, Thread.ofVirtual().factory());

        var txneer = choams.get(members.get(0).getId());

        assertTrue(Utils.waitForCondition(120_00, 1_000, () -> txneer.active()),
                   "Transactioneer did not become active");

        final var countdown = new CountDownLatch(1); 
        var transactioneer = new Transactioneer(txneer.getSession(), exec, timeout, 1,
                                                scheduler, countdown, exec);

        transactioneer.start();
        assertTrue(countdown.await(timeout.toSeconds(), TimeUnit.SECONDS), "Could not submit transaction");

        var target = blocks.values().stream().mapToInt(l -> l.get()).max().getAsInt();

        routers.get(testSubject.getId()).start();
        choams.get(testSubject.getId()).start();
        assertTrue(Utils.waitForCondition(30_000, 1_000, () -> blocks.get(testSubject.getId()).get() >= target),
                   "Expecting: " + target + " completed: " + blocks.get(testSubject.getId()).get());

    }

    public SigningMember initialize(int checkpointBlockSize, int cardinality) {
        @SuppressWarnings("preview")
        var exec = Executors.newVirtualThreadPerTaskExecutor();
        blocks = new ConcurrentHashMap<>();
        var context = new ContextImpl<>(DigestAlgorithm.DEFAULT.getOrigin(), cardinality, 0.2, 3);
        @SuppressWarnings("preview")
        var scheduler = Executors.newScheduledThreadPool(cardinality, Thread.ofVirtual().factory());

        var params = Parameters.newBuilder()
                               .setSynchronizeTimeout(Duration.ofSeconds(1))
                               .setBootstrap(BootstrapParameters.newBuilder()
                                                                .setGossipDuration(Duration.ofMillis(20))
                                                                .build())
                               .setGenesisViewId(DigestAlgorithm.DEFAULT.getOrigin())
                               .setGossipDuration(Duration.ofMillis(10))
                               .setProducer(ProducerParameters.newBuilder()
                                                              .setGossipDuration(Duration.ofMillis(20))
                                                              .setBatchInterval(Duration.ofMillis(150))
                                                              .setMaxBatchByteSize(1024 * 1024)
                                                              .setMaxBatchCount(10_000)
                                                              .build())
                               .setCheckpointBlockDelta(checkpointBlockSize);
        params.getProducer().ethereal().setNumberOfEpochs(5).setFpr(0.0125);
        members = IntStream.range(0, cardinality)
                           .mapToObj(i -> Utils.getMember(i))
                           .map(cpk -> new SigningMemberImpl(cpk))
                           .map(e -> (SigningMember) e)
                           .peek(m -> context.activate(m))
                           .toList();
        SigningMember testSubject = members.get(members.size() - 1); // hardwired
        final var prefix = UUID.randomUUID().toString();
        routers = members.stream().collect(Collectors.toMap(m -> m.getId(), m -> {
            var comm = new LocalRouter(prefix, ServerConnectionCache.newBuilder().setTarget(cardinality),
                                       exec, null);
            comm.setMember(m);
            return comm;
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
                    if (f != null) {
                        f.complete(new Object());
                    }
                }
            };
            params.getProducer().ethereal().setSigner(m);
            if (m.equals(testSubject)) {
                params.setSynchronizationCycles(20);
            } else {
                params.setSynchronizationCycles(1);
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
