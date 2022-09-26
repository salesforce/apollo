/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
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

    private Map<Digest, CHOAM>  choams;
    private List<SigningMember> members;
    private Map<Digest, Router> routers;

    @AfterEach
    public void after() throws Exception {
        shutdown();
        members = null;
    }

    @Test
    public void genesisBootstrap() throws Exception {
        SigningMember testSubject = initialize(2000, 5);
        System.out.println("Test subject: " + testSubject.getId() + " membership: "
        + members.stream().map(e -> e.getId()).toList());
        routers.entrySet()
               .stream()
               .filter(e -> !e.getKey().equals(testSubject.getId()))
               .forEach(r -> r.getValue().start());
        choams.entrySet()
              .stream()
              .filter(e -> !e.getKey().equals(testSubject.getId()))
              .forEach(ch -> ch.getValue().start());

        final Duration timeout = Duration.ofSeconds(6);
        final var scheduler = Executors.newScheduledThreadPool(1);

        var txneer = choams.get(members.get(0).getId());

        System.out.println("Transactioneer: " + txneer.getId());

        boolean actived = Utils.waitForCondition(12_000, 1_000,
                                                 () -> choams.entrySet()
                                                             .stream()
                                                             .filter(e -> !testSubject.getId().equals(e.getKey()))
                                                             .map(e -> e.getValue())
                                                             .filter(c -> !c.active())
                                                             .count() == 0);
        assertTrue(actived,
                   "Group did not become active, test subject: " + testSubject.getId() + " txneer: " + txneer.getId()
                   + " inactive: "
                   + choams.entrySet()
                           .stream()
                           .filter(e -> !testSubject.getId().equals(e.getKey()))
                           .map(e -> e.getValue())
                           .filter(c -> !c.active())
                           .map(c -> c.getId())
                           .toList());

        final var countdown = new CountDownLatch(1);
        var transactioneer = new Transactioneer(txneer.getSession(), Executors.newSingleThreadExecutor(), timeout, 1,
                                                scheduler, countdown, Executors.newSingleThreadExecutor());

        transactioneer.start();
        assertTrue(countdown.await(30, TimeUnit.SECONDS), "Could not submit transaction");

        var target = choams.values()
                           .stream()
                           .map(l -> l.currentHeight())
                           .filter(h -> h != null)
                           .mapToInt(u -> u.intValue())
                           .max()
                           .getAsInt();

        routers.get(testSubject.getId()).start();
        choams.get(testSubject.getId()).start();
        final var targetMet = Utils.waitForCondition(30_000, 1_000, () -> {
            final var currentHeight = choams.get(testSubject.getId()).currentHeight();
            return currentHeight != null && currentHeight.intValue() >= target;
        });
        assertTrue(targetMet,
                   "Expecting: " + target + " completed: " + choams.get(testSubject.getId()).currentHeight());

    }

    public SigningMember initialize(int checkpointBlockSize, int cardinality) throws Exception {
        var context = new ContextImpl<>(DigestAlgorithm.DEFAULT.getOrigin(), cardinality, 0.2, 3);

        var params = Parameters.newBuilder()
                               .setBootstrap(BootstrapParameters.newBuilder()
                                                                .setGossipDuration(Duration.ofMillis(20))
                                                                .build())
                               .setGenesisViewId(DigestAlgorithm.DEFAULT.getOrigin())
                               .setGossipDuration(Duration.ofMillis(10))
                               .setProducer(ProducerParameters.newBuilder()
                                                              .setGossipDuration(Duration.ofMillis(20))
                                                              .setBatchInterval(Duration.ofMillis(10))
                                                              .setMaxBatchByteSize(1024 * 1024)
                                                              .setMaxBatchCount(10_000)
                                                              .build())
                               .setCheckpointBlockDelta(checkpointBlockSize);
        params.getDrainPolicy().setInitialBackoff(Duration.ofMillis(1)).setMaxBackoff(Duration.ofMillis(1));
        params.getProducer().ethereal().setNumberOfEpochs(2).setEpochLength(20);

        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);

        members = IntStream.range(0, cardinality).mapToObj(i -> {
            try {
                return stereotomy.newIdentifier().get();
            } catch (InterruptedException | ExecutionException e1) {
                throw new IllegalStateException(e1);
            }
        })
                           .map(cpk -> new ControlledIdentifierMember(cpk))
                           .map(e -> (SigningMember) e)
                           .peek(m -> context.activate(m))
                           .toList();
        SigningMember testSubject = members.get(members.size() - 1); // hardwired
        final var prefix = UUID.randomUUID().toString();
        ConcurrentSkipListMap<Digest, Member> serverMembers = new ConcurrentSkipListMap<>();
        routers = members.stream().collect(Collectors.toMap(m -> m.getId(), m -> {
            var comm = new LocalRouter(prefix, serverMembers, ServerConnectionCache.newBuilder().setTarget(cardinality),
                                       Executors.newFixedThreadPool(2), null);
            comm.setMember(m);
            return comm;
        }));
        choams = members.stream().collect(Collectors.toMap(m -> m.getId(), m -> {

            final TransactionExecutor processor = new TransactionExecutor() {
                @Override
                public void endBlock(ULong height, Digest hash) {
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
            if (!m.equals(testSubject)) {
                params.setSynchronizationCycles(1);
            }
            return new CHOAM(params.build(RuntimeParameters.newBuilder()
                                                           .setMember(m)
                                                           .setCommunications(routers.get(m.getId()))
                                                           .setScheduler(Executors.newSingleThreadScheduledExecutor())
                                                           .setProcessor(processor)
                                                           .setContext(context)
                                                           .setExec(Executors.newFixedThreadPool(2))
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
