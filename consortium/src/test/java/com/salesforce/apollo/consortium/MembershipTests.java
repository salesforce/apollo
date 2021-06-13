/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import static com.salesforce.apollo.test.pregen.PregenPopulation.getMember;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.salesfoce.apollo.consortium.proto.ByteTransaction;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.messaging.proto.ByteMessage;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.consortium.fsm.CollaboratorFsm;
import com.salesforce.apollo.consortium.fsm.Transitions;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.membership.messaging.Messenger;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class MembershipTests {
    private static Map<Digest, CertificateWithPrivateKey> certs;

    private static final Message GENESIS_DATA = ByteMessage.newBuilder()
                                                           .setContents(ByteString.copyFromUtf8("Give me food or give me slack or kill me"))
                                                           .build();

    private static final Digest GENESIS_VIEW_ID = DigestAlgorithm.DEFAULT.digest("Give me food or give me slack or kill me".getBytes());

    private static final int CARDINALITY = 10;

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(0, CARDINALITY)
                         .parallel()
                         .mapToObj(i -> getMember(i))
                         .collect(Collectors.toMap(cert -> Member.getMemberIdentifier(cert.getX509Certificate()),
                                                   cert -> cert));
    }

    private Map<Digest, Router>           communications = new ConcurrentHashMap<>();
    private final Map<Member, Consortium> consortium     = new ConcurrentHashMap<>();
    private Context<Member>               context;
    private List<SigningMember>           members;
    private int                           testCardinality;

    @AfterEach
    public void after() {
        consortium.values().forEach(e -> e.stop());
        consortium.clear();
        communications.values().forEach(e -> e.close());
        communications.clear();
    }

    @BeforeEach
    public void before() {

        context = new Context<>(DigestAlgorithm.DEFAULT.getOrigin(), 3);

        members = certs.values()
                       .stream()
                       .map(c -> new SigningMemberImpl(Member.getMemberIdentifier(c.getX509Certificate()),
                               c.getX509Certificate(), c.getPrivateKey(), new Signer(0, c.getPrivateKey()),
                               c.getX509Certificate().getPublicKey()))
                       .peek(m -> context.activate(m))
                       .collect(Collectors.toList());
        ForkJoinPool executor = ForkJoinPool.commonPool();
        ServerConnectionCache.Builder builder = ServerConnectionCache.newBuilder().setTarget(30);
        members.forEach(node -> {
            communications.put(node.getId(), new LocalRouter(node, builder, executor));
        });
    }

    @Test
    public void testCheckpointBootstrap() throws Exception {
        testCardinality = 3;
        AtomicReference<CountDownLatch> processed = new AtomicReference<>(new CountDownLatch(testCardinality - 1));
        gatherConsortium(Duration.ofMillis(150), processed);

        Set<Consortium> blueRibbon = new HashSet<>();
        ViewContext.viewFor(GENESIS_VIEW_ID, context).allMembers().forEach(e -> {
            blueRibbon.add(consortium.get(e));
        });

        final Consortium testSubject = consortium.values()
                                                 .stream()
                                                 .filter(c -> !blueRibbon.contains(c))
                                                 .findFirst()
                                                 .orElse(null);
        System.out.println("test subject: " + testSubject.getMember().getId());

        System.out.println("starting consortium");
        communications.entrySet()
                      .stream()
                      .filter(r -> !r.getKey().equals(testSubject.getMember().getId()))
                      .peek(e -> System.out.println(e.getKey()))
                      .map(e -> e.getValue())
                      .forEach(r -> r.start());
        consortium.values().stream().filter(c -> !c.equals(testSubject)).forEach(e -> e.start());

        assertTrue(processed.get().await(30, TimeUnit.SECONDS));

        Consortium client = consortium.values()
                                      .stream()
                                      .filter(c -> !blueRibbon.contains(c) && !testSubject.equals(c))
                                      .findFirst()
                                      .get();
        Semaphore outstanding = new Semaphore(50); // outstanding, unfinalized txns
        int bunchCount = 500;
        System.out.println("Awaiting " + bunchCount + " transactions");
        ArrayList<Digest> submitted = new ArrayList<>();
        final CountDownLatch submittedBunch = new CountDownLatch(bunchCount);
        for (int i = 0; i < bunchCount; i++) {
            outstanding.acquire();
            AtomicReference<Digest> pending = new AtomicReference<>();
            pending.set(client.submit(null, (h, t) -> {
                outstanding.release();
                submitted.remove(pending.get());
                submittedBunch.countDown();
            }, Any.pack(ByteTransaction.newBuilder().setContent(ByteString.copyFromUtf8("Hello world")).build())));
            submitted.add(pending.get());
        }

        boolean completed = submittedBunch.await(125, TimeUnit.SECONDS);
        assertTrue(completed, "Did not process transaction bunch: " + submittedBunch.getCount());
        System.out.println("Completed additional " + bunchCount + " transactions");

        testSubject.start();
        communications.get(testSubject.getMember().getId()).start();

        bunchCount = 100;
        submitted.clear();
        final CountDownLatch nextBunch = new CountDownLatch(bunchCount);
        for (int i = 0; i < bunchCount; i++) {
            outstanding.acquire();
            AtomicReference<Digest> pending = new AtomicReference<>();
            pending.set(client.submit(null, (h, t) -> {
                outstanding.release();
                submitted.remove(pending.get());
                nextBunch.countDown();
            }, Any.pack(ByteTransaction.newBuilder().setContent(ByteString.copyFromUtf8("Hello world")).build())));
            submitted.add(pending.get());
        }

        completed = nextBunch.await(10, TimeUnit.SECONDS);
        assertTrue(completed, "Did not process transaction bunch: " + nextBunch.getCount());
        System.out.println("Completed additional " + bunchCount + " transactions");

        completed = Utils.waitForCondition(10_000, () -> {
            return testSubject.fsm.getCurrentState() == CollaboratorFsm.CLIENT;
        });

        assertTrue(completed, "Test subject did not successfully bootstrap: " + testSubject.getMember().getId()
                + " state: " + testSubject.fsm.getCurrentState());
    }

    @Test
    public void testGenesisBootstrap() throws Exception {
        testCardinality = 3;
        AtomicReference<CountDownLatch> processed = new AtomicReference<>(new CountDownLatch(testCardinality - 1));
        gatherConsortium(Duration.ofMillis(150), processed);

        Set<Consortium> blueRibbon = new HashSet<>();
        ViewContext.viewFor(GENESIS_VIEW_ID, context).allMembers().forEach(e -> {
            blueRibbon.add(consortium.get(e));
        });

        final Consortium testSubject = consortium.values()
                                                 .stream()
                                                 .filter(c -> !blueRibbon.contains(c))
                                                 .findFirst()
                                                 .orElse(null);
        System.out.println("test subject: " + testSubject.getMember().getId());

        System.out.println("starting consortium");
        communications.entrySet()
                      .stream()
                      .filter(r -> !r.getKey().equals(testSubject.getMember().getId()))
                      .peek(e -> System.out.println(e.getKey()))
                      .map(e -> e.getValue())
                      .forEach(r -> r.start());
        consortium.values().stream().filter(c -> !c.equals(testSubject)).forEach(e -> e.start());

        assertTrue(processed.get().await(30, TimeUnit.SECONDS));

        Consortium client = consortium.values()
                                      .stream()
                                      .filter(c -> !blueRibbon.contains(c) && !testSubject.equals(c))
                                      .findFirst()
                                      .get();
        Semaphore outstanding = new Semaphore(50); // outstanding, unfinalized txns
        int bunchCount = 150;
        System.out.println("Awaiting " + bunchCount + " transactions");
        ArrayList<Digest> submitted = new ArrayList<>();
        final CountDownLatch submittedBunch = new CountDownLatch(bunchCount);
        for (int i = 0; i < bunchCount; i++) {
            outstanding.acquire();
            AtomicReference<Digest> pending = new AtomicReference<>();
            pending.set(client.submit(null, (h, t) -> {
                outstanding.release();
                submitted.remove(pending.get());
                submittedBunch.countDown();
            }, Any.pack(ByteTransaction.newBuilder().setContent(ByteString.copyFromUtf8("Hello world")).build())));
            submitted.add(pending.get());
        }

        boolean completed = submittedBunch.await(125, TimeUnit.SECONDS);
        assertTrue(completed, "Did not process transaction bunch: " + submittedBunch.getCount());
        System.out.println("Completed additional " + bunchCount + " transactions");

        testSubject.start();
        communications.get(testSubject.getMember().getId()).start();

        bunchCount = 100;
        submitted.clear();
        final CountDownLatch nextBunch = new CountDownLatch(bunchCount);
        for (int i = 0; i < bunchCount; i++) {
            outstanding.acquire();
            AtomicReference<Digest> pending = new AtomicReference<>();
            pending.set(client.submit(null, (h, t) -> {
                outstanding.release();
                submitted.remove(pending.get());
                nextBunch.countDown();
            }, Any.pack(ByteTransaction.newBuilder().setContent(ByteString.copyFromUtf8("Hello world")).build())));
            submitted.add(pending.get());
        }

        completed = nextBunch.await(10, TimeUnit.SECONDS);
        assertTrue(completed, "Did not process transaction bunch: " + nextBunch.getCount());
        System.out.println("Completed additional " + bunchCount + " transactions");

        completed = Utils.waitForCondition(10_000, () -> {
            return testSubject.fsm.getCurrentState() == CollaboratorFsm.CLIENT;
        });

        assertTrue(completed, "Test subject did not successfully bootstrap: " + testSubject.getMember().getId());
    }

    private void gatherConsortium(Duration gossipDuration, AtomicReference<CountDownLatch> processed) {
        Messenger.Parameters msgParameters = Messenger.Parameters.newBuilder()
                                                                 .setFalsePositiveRate(0.001)
                                                                 .setBufferSize(1000)
                                                                 .build();
        Executor cPipeline = Executors.newSingleThreadExecutor();
        Set<Digest> decided = Collections.newSetFromMap(new ConcurrentHashMap<>());
        BiFunction<CertifiedBlock, CompletableFuture<?>, Digest> consensus = (c, f) -> {
            Digest hash = DigestAlgorithm.DEFAULT.digest(c.getBlock().toByteString());
            if (decided.add(hash)) {
                cPipeline.execute(() -> consortium.values().parallelStream().forEach(m -> {
                    m.process(c);
                    processed.get().countDown();
                }));
            }
            return hash;
        };
        TransactionExecutor executor = (h, et, c) -> {
            if (c != null) {
                c.accept(new Digest(et.getHash()), null);
            }
        };
        members.stream()
               .map(m -> new Consortium(parameters(m, consensus, executor, msgParameters, gossipDuration)))
               .peek(c -> context.activate(c.getMember()))
               .forEach(e -> consortium.put(e.getMember(), e));
    }

    private Parameters parameters(SigningMember m, BiFunction<CertifiedBlock, CompletableFuture<?>, Digest> consensus,
                                  TransactionExecutor executor, Messenger.Parameters msgParameters,
                                  Duration gossipDuration) {
        return Parameters.newBuilder()
                         .setConsensus(consensus)
                         .setMember(m)
                         .setContext(context)
                         .setMsgParameters(msgParameters)
                         .setMaxBatchByteSize(1024 * 1024)
                         .setMaxBatchSize(1000)
                         .setCommunications(communications.get(m.getId()))
                         .setMaxBatchDelay(Duration.ofMillis(1000))
                         .setGossipDuration(gossipDuration)
                         .setViewTimeout(Duration.ofMillis(1500))
                         .setSynchronizeTimeout(Duration.ofMillis(1500))
                         .setJoinTimeout(Duration.ofSeconds(5))
                         .setTransactonTimeout(Duration.ofSeconds(30))
                         .setScheduler(Executors.newSingleThreadScheduledExecutor())
                         .setExecutor(executor)
                         .setGenesisData(GENESIS_DATA)
                         .setGenesisViewId(GENESIS_VIEW_ID)
                         .setDeltaCheckpointBlocks(5)
                         .setCheckpointer(l -> {
                             File temp;
                             try {
                                 temp = File.createTempFile("foo", "bar");
                                 temp.deleteOnExit();
                                 try (FileOutputStream fos = new FileOutputStream(temp)) {
                                     fos.write("Give me food or give me slack or kill me".getBytes());
                                     fos.flush();
                                 }
                             } catch (IOException e) {
                                 throw new IllegalStateException("Cannot create temp file", e);
                             }

                             return temp;
                         })
                         .build();
    }

    @SuppressWarnings("unused")
    private void validateState(Set<Consortium> blueRibbon) {
        long clientsInWrongState = consortium.values()
                                             .stream()
                                             .filter(c -> !blueRibbon.contains(c))
                                             .map(c -> c.fsm.getCurrentState())
                                             .filter(b -> b != CollaboratorFsm.CLIENT)
                                             .count();
        Set<Transitions> failedMembers = consortium.values()
                                                   .stream()
                                                   .filter(c -> !blueRibbon.contains(c))
                                                   .filter(c -> c.fsm.getCurrentState() != CollaboratorFsm.CLIENT)
                                                   .map(c -> c.fsm.getCurrentState())
                                                   .collect(Collectors.toSet());
        assertEquals(0, clientsInWrongState, "True clients gone bad: " + failedMembers);
        assertEquals(context.getRingCount() - 1,
                     blueRibbon.stream()
                               .map(c -> c.fsm.getCurrentState())
                               .filter(b -> b == CollaboratorFsm.FOLLOWER)
                               .count(),
                     "True follower gone bad: " + blueRibbon.stream().map(c -> {
                         Transitions cs = c.fsm.getCurrentState();
                         return c.fsm.prettyPrint(cs);
                     }).collect(Collectors.toSet()));
        assertEquals(1,
                     blueRibbon.stream()
                               .map(c -> c.fsm.getCurrentState())
                               .filter(b -> b == CollaboratorFsm.LEADER)
                               .count(),
                     "True leader gone bad: "
                             + blueRibbon.stream().map(c -> c.fsm.getCurrentState()).collect(Collectors.toSet()));
        System.out.println("Blue ribbon cimittee toOrder state: " + blueRibbon.stream()
                                                                              .map(c -> c.getState())
                                                                              .map(cc -> cc.getToOrder().size())
                                                                              .collect(Collectors.toList()));
        assertEquals(0,
                     blueRibbon.stream()
                               .map(c -> c.getState())
                               .map(cc -> cc.getToOrder().size())
                               .filter(c -> c > 0)
                               .count(),
                     "Blue ribbion committee did not flush toOrder");
    }
}
