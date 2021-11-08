/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.h2.mvstore.MVStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.ethereal.proto.ByteMessage;
import com.salesforce.apollo.choam.CHOAM.TransactionExecutor;
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

        private final AtomicInteger completed = new AtomicInteger();
        private final AtomicInteger failed    = new AtomicInteger();
        private final int           max;
        private final AtomicBoolean proceed;
        private final Session       session;
        private final Duration      timeout;
        private final ByteMessage   tx        = ByteMessage.newBuilder()
                                                           .setContents(ByteString.copyFromUtf8("Give me food or give me slack or kill me"))
                                                           .build();

        Transactioneer(Session session, Duration timeout, AtomicBoolean proceed, int max) {
            this.proceed = proceed;
            this.session = session;
            this.timeout = timeout;
            this.max = max;
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
                            decorate(session.submit(tx, timeout));
                        } catch (InvalidTransaction e) {
                            e.printStackTrace();
                        }
                    }, entropy.nextInt(10), TimeUnit.MILLISECONDS);
                } else {
                    if (completed.incrementAndGet() < max) {
                        try {
                            decorate(session.submit(tx, timeout));
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
                    decorate(session.submit(tx, timeout));
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
    private ScheduledExecutorService       scheduler;
    private Map<Digest, List<Transaction>> transactions;

    @AfterEach
    public void after() throws Exception {
        if (choams != null) {
            choams.values().forEach(e -> e.stop());
            choams = null;
        }
        if (routers != null) {
            routers.values().forEach(e -> e.close());
            routers = null;
        }
        members = null;
        transactions = null;
        blocks = null;
        scheduler = null;
    }

    @Test
    public void genesisBootstrap() throws Exception {
        initialize(2000, 5);
        SigningMember testSubject = members.get(4);
        System.out.println("Test subject: " + testSubject);
        routers.entrySet().stream().filter(e -> !e.getKey().equals(testSubject.getId()))
               .forEach(r -> r.getValue().start());
        choams.values().forEach(ch -> ch.start());
        final int expected = 23;

        Utils.waitForCondition(300_000, 1_000, () -> blocks.values().stream().mapToInt(l -> l.size())
                                                           .filter(s -> s >= expected).count() == choams.size() - 1);
        assertEquals(choams.size() - 1,
                     blocks.values().stream().mapToInt(l -> l.size()).filter(s -> s >= expected).count(),
                     "Failed: " + blocks.get(members.get(0).getId()).size());

        final Duration timeout = Duration.ofSeconds(5);

        AtomicBoolean proceed = new AtomicBoolean(true);
        var transactioneers = new ArrayList<Transactioneer>();
        final int clientCount = 1;
        final int max = 1;
        for (int i = 0; i < clientCount; i++) {
            choams.entrySet().stream().filter(e -> !e.getKey().equals(testSubject.getId())).map(e -> e.getValue())
                  .map(c -> new Transactioneer(c.getSession(), timeout, proceed, max))
                  .forEach(e -> transactioneers.add(e));
        }

        transactioneers.stream().forEach(e -> e.start());
        final boolean success;
        try {
            success = Utils.waitForCondition(10_000, 1_000,
                                             () -> transactioneers.stream().filter(e -> e.completed.get() >= max)
                                                                  .count() == transactioneers.size());
        } finally {
            proceed.set(false);
        }
        assertTrue(success,
                   "Only completed: " + transactioneers.stream().filter(e -> e.completed.get() >= max).count());
    }

    public void initialize(int checkpointBlockSize, int cardinality) {
        transactions = new ConcurrentHashMap<>();
        blocks = new ConcurrentHashMap<>();
        Random entropy = new Random();
        var context = new Context<>(DigestAlgorithm.DEFAULT.getOrigin().prefix(entropy.nextLong()), 0.2, cardinality,
                                    3);
        scheduler = Executors.newScheduledThreadPool(cardinality);

        AtomicInteger sd = new AtomicInteger();
        Executor submitDispatcher = Executors.newFixedThreadPool(cardinality, r -> {
            Thread thread = new Thread(r, "Submit Dispatcher [" + sd.getAndIncrement() + "]");
            thread.setDaemon(true);
            return thread;
        });
        AtomicInteger d = new AtomicInteger();
        Executor dispatcher = Executors.newCachedThreadPool(r -> {
            Thread thread = new Thread(r, "Dispatcher [" + d.getAndIncrement() + "]");
            thread.setDaemon(true);
            return thread;
        });
        AtomicInteger exec = new AtomicInteger();
        Executor routerExec = Executors.newFixedThreadPool(cardinality, r -> {
            Thread thread = new Thread(r, "Router exec [" + exec.getAndIncrement() + "]");
            thread.setDaemon(true);
            return thread;
        });

        Function<Long, File> checkpointer = h -> {
            File cp;
            try {
                cp = File.createTempFile("cp-" + h, ".chk");
                cp.deleteOnExit();
                try (var os = new FileOutputStream(cp)) {
                    os.write("Give me food or give me slack or kill me".getBytes());
                }
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            return cp;
        };

        var params = Parameters.newBuilder().setContext(context).setSynchronizationCycles(1)
                               .setGenesisViewId(DigestAlgorithm.DEFAULT.getOrigin().prefix(entropy.nextLong()))
                               .setGossipDuration(Duration.ofMillis(5)).setScheduler(scheduler)
                               .setSubmitDispatcher(submitDispatcher).setDispatcher(dispatcher)
                               .setProducer(ProducerParameters.newBuilder().setGossipDuration(Duration.ofMillis(1))
                                                              .build())
                               .setTxnPermits(10_000).setCheckpointBlockSize(checkpointBlockSize)
                               .setCheckpointer(checkpointer);
        params.getProducer().ethereal().setFpr(0.00000125);

        members = IntStream.range(0, cardinality).mapToObj(i -> Utils.getMember(i))
                           .map(cpk -> new SigningMemberImpl(cpk)).map(e -> (SigningMember) e)
                           .peek(m -> context.activate(m)).toList();
        routers = members.stream()
                         .collect(Collectors.toMap(m -> m.getId(),
                                                   m -> new LocalRouter(m,
                                                                        ServerConnectionCache.newBuilder()
                                                                                             .setTarget(cardinality)
                                                                                             .setMetrics(params.getMetrics()),
                                                                        routerExec)));
        Executor clients = Executors.newCachedThreadPool();
        choams = members.stream().collect(Collectors.toMap(m -> m.getId(), m -> {
            final TransactionExecutor processor = new TransactionExecutor() {

                @Override
                public void beginBlock(long height, Digest hash) {
                    blocks.computeIfAbsent(m.getId(), d -> new ArrayList<>()).add(hash);
                }

                @SuppressWarnings({ "unchecked", "rawtypes" })
                @Override
                public void execute(Transaction t, CompletableFuture f) {
                    transactions.computeIfAbsent(m.getId(), d -> new ArrayList<>()).add(t);
                    if (f != null) {
                        f.completeAsync(() -> new Object(), clients);
                    }
                }
            };
            params.getProducer().ethereal().setSigner(m);
            return new CHOAM(params.setMember(m).setCommunications(routers.get(m.getId())).setProcessor(processor)
                                   .build(),
                             MVStore.open(null));
        }));
    }
}
