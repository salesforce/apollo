/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import static com.salesforce.apollo.test.pregen.PregenPopulation.getMember;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.ByteTransaction;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.consortium.proto.Header;
import com.salesfoce.apollo.messaging.proto.ByteMessage;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.comm.ServerConnectionCache.Builder;
import com.salesforce.apollo.consortium.fsm.CollaboratorFsm;
import com.salesforce.apollo.consortium.fsm.Transitions;
import com.salesforce.apollo.consortium.support.HashedBlock;
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
public class TestConsortium {

    private static Map<Digest, CertificateWithPrivateKey> certs;
    private static final Message                          GENESIS_DATA    = ByteMessage.newBuilder()
                                                                                       .setContents(ByteString.copyFromUtf8("Give me food or give me slack or kill me"))
                                                                                       .build();
    private static final Digest                           GENESIS_VIEW_ID = DigestAlgorithm.DEFAULT.digest("Give me food or give me slack or kill me".getBytes());
    private static final Duration                         gossipDuration  = Duration.ofMillis(10);
    private final static int                              testCardinality = 5;

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(0, testCardinality)
                         .parallel()
                         .mapToObj(i -> getMember(i))
                         .collect(Collectors.toMap(cert -> Member.getMemberIdentifier(cert.getX509Certificate()),
                                                   cert -> cert));
    }

    private File                          baseDir;
    private Builder                       builder        = ServerConnectionCache.newBuilder().setTarget(30);
    private Map<Digest, Router>           communications = new ConcurrentHashMap<>();
    private final Map<Member, Consortium> consortium     = new ConcurrentHashMap<>();
    private List<SigningMember>           members;
    private ForkJoinPool                  dispatcher;

    @AfterEach
    public void after() {
        consortium.values().forEach(e -> e.stop());
        consortium.clear();
        communications.values().forEach(e -> e.close());
        communications.clear();
        dispatcher.shutdown();
        dispatcher = null;
    }

    @BeforeEach
    public void before() {

        baseDir = new File(System.getProperty("user.dir"), "target/cluster");
        Utils.clean(baseDir);
        baseDir.mkdirs();

        assertTrue(certs.size() >= testCardinality);

        members = new ArrayList<>();
        for (CertificateWithPrivateKey cert : certs.values()) {
            if (members.size() < testCardinality) {
                members.add(new SigningMemberImpl(Member.getMemberIdentifier(cert.getX509Certificate()),
                        cert.getX509Certificate(), cert.getPrivateKey(), new Signer(0, cert.getPrivateKey()),
                        cert.getX509Certificate().getPublicKey()));
            } else {
                break;
            }
        }

        assertEquals(testCardinality, members.size());
        dispatcher = Router.createFjPool();
        members.forEach(node -> {
            communications.put(node.getId(), new LocalRouter(node, builder, dispatcher));
        });

    }

    @Test
    public void smoke() throws Exception {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(testCardinality);

        Context<Member> view = new Context<>(DigestAlgorithm.DEFAULT.getOrigin().prefix(1), 3);
        Messenger.Parameters msgParameters = Messenger.Parameters.newBuilder()
                                                                 .setFalsePositiveRate(0.001)
                                                                 .setBufferSize(1000)
                                                                 .build();
        Executor cPipeline = Executors.newSingleThreadExecutor();
        AtomicInteger cLabel = new AtomicInteger();
        Executor blockPool = Executors.newFixedThreadPool(15, r -> {
            Thread t = new Thread(r, "Consensus [" + cLabel.getAndIncrement() + "]");
            t.setDaemon(true);
            return t;
        });
        AtomicReference<CountDownLatch> processed = new AtomicReference<>(new CountDownLatch(testCardinality));
        Set<Digest> decided = Collections.newSetFromMap(new ConcurrentHashMap<>());
        BiFunction<CertifiedBlock, CompletableFuture<?>, Digest> consensus = (c, f) -> {
            Digest hash = DigestAlgorithm.DEFAULT.digest(c.getBlock().toByteString());
            if (decided.add(hash)) {
                cPipeline.execute(() -> {
                    CountDownLatch executed = new CountDownLatch(testCardinality);
                    consortium.values().forEach(m -> {
                        blockPool.execute(() -> {
                            m.process(c);
                            executed.countDown();
                            processed.get().countDown();
                        });
                    });
                    try {
                        executed.await();
                    } catch (InterruptedException e) {
                        fail("Interrupted consensus", e);
                    }
                });
            }
            return hash;
        };
        TransactionExecutor executor = (h, et, c) -> {
            if (c != null) {
                c.accept(new Digest(et.getHash()), null);
            }
        };
        gatherConsortium(view, consensus, gossipDuration, scheduler, msgParameters, executor);

        Set<Consortium> blueRibbon = new HashSet<>();
        ViewContext.viewFor(GENESIS_VIEW_ID, view).allMembers().forEach(e -> {
            blueRibbon.add(consortium.get(e));
        });

        communications.values().forEach(r -> r.start());

        System.out.println("starting consortium");
        consortium.values().forEach(e -> e.start());

        System.out.println("awaiting genesis processing");

        assertTrue(processed.get().await(30, TimeUnit.SECONDS),
                   "Did not converge, end state of true clients gone bad: "
                           + consortium.values()
                                       .stream()
                                       .filter(c -> !blueRibbon.contains(c))
                                       .map(c -> c.fsm.getCurrentState())
                                       .filter(b -> b != CollaboratorFsm.CLIENT)
                                       .collect(Collectors.toSet())
                           + " : "
                           + consortium.values()
                                       .stream()
                                       .filter(c -> !blueRibbon.contains(c))
                                       .filter(c -> c.fsm.getCurrentState() != CollaboratorFsm.CLIENT)
                                       .map(c -> c.getMember())
                                       .collect(Collectors.toList()));

        System.out.println("processing complete, validating state");

        validateState(view, blueRibbon);

        processed.set(new CountDownLatch(testCardinality));
        Consortium client = consortium.values()
                                      .stream()
                                      .filter(c -> c.fsm.getCurrentState() == CollaboratorFsm.CLIENT)
                                      .findFirst()
                                      .get();
        AtomicBoolean txnProcessed = new AtomicBoolean();

        System.out.println("Submitting transaction");
        Digest hash = client.submit((h, t) -> {
            if (t != null) {
                t.printStackTrace();
            } else {
                System.out.println("Transaction accepted: " + h + ", awaiting processing");
            }
        }, (h, t) -> txnProcessed.set(true),
                                    ByteTransaction.newBuilder()
                                                   .setContent(ByteString.copyFromUtf8("Hello world"))
                                                   .build(),
                                    Duration.ofSeconds(20));

        System.out.println("Submitted transaction: " + hash + ", awaiting processing of next block");
        assertTrue(processed.get().await(30, TimeUnit.SECONDS), "Did not process transaction block");

        System.out.println("block processed, waiting for transaction completion: " + hash);
        assertTrue(Utils.waitForCondition(5_000, () -> txnProcessed.get()), "Transaction not completed");
        System.out.println("transaction completed: " + hash);
        System.out.println();

        Semaphore outstanding = new Semaphore(1000); // outstanding, unfinalized txns
        int bunchCount = 10_000;
        System.out.println("Submitting bunch: " + bunchCount);
        Set<Digest> submitted = new HashSet<>();
        CountDownLatch submittedBunch = new CountDownLatch(bunchCount);
        final Duration timeout = Duration.ofSeconds(10);
        for (int i = 0; i < bunchCount; i++) {
            MembershipTests.submit(client, outstanding, submitted, submittedBunch, timeout);
        }

        System.out.println("Awaiting " + bunchCount + " transactions");
        boolean completed = submittedBunch.await(125, TimeUnit.SECONDS);
        submittedBunch.getCount();
        assertTrue(completed, "Did not process transaction bunch: " + submittedBunch.getCount());
        System.out.println("Completed additional " + bunchCount + " transactions");
    }

    @Test
    public void testGaps() throws Exception {
        long nextBlock = 56L;
        List<CertifiedBlock> blocks = new ArrayList<>();
        Digest prev = DigestAlgorithm.DEFAULT.getOrigin();
        for (int i = 0; i < 10; i++) {
            Block block = Block.newBuilder()
                               .setHeader(Header.newBuilder().setHeight(nextBlock).setPrevious(prev.toDigeste()))
                               .build();
            nextBlock++;
            blocks.add(CertifiedBlock.newBuilder().setBlock(block).build());
            prev = DigestAlgorithm.DEFAULT.digest(block.toByteString());
        }
        Map<Long, HashedBlock> cache = new HashMap<>();
        assertEquals(0, CollaboratorContext.noGaps(DigestAlgorithm.DEFAULT, blocks, cache).size());
        ArrayList<CertifiedBlock> gapped = new ArrayList<>(blocks);
        gapped.remove(5);
        assertEquals(1, CollaboratorContext.noGaps(DigestAlgorithm.DEFAULT, gapped, cache).size());
        assertEquals(0, CollaboratorContext.noGaps(DigestAlgorithm.DEFAULT, blocks.subList(1, blocks.size()), cache)
                                           .size());
        CertifiedBlock cb = blocks.get(5);
        cache.put(cb.getBlock().getHeader().getHeight(),
                  new HashedBlock(DigestAlgorithm.DEFAULT.digest(cb.getBlock().toByteString()), cb.getBlock()));
        assertEquals(0, CollaboratorContext.noGaps(DigestAlgorithm.DEFAULT, gapped, cache).size());
    }

    private void gatherConsortium(Context<Member> view,
                                  BiFunction<CertifiedBlock, CompletableFuture<?>, Digest> consensus,
                                  Duration gossipDuration, ScheduledExecutorService scheduler,
                                  Messenger.Parameters msgParameters, TransactionExecutor executor) {
        members.stream()
               .map(m -> new Consortium(Parameters.newBuilder()
                                                  .setConsensus(consensus)
                                                  .setDispatcher(dispatcher)
                                                  .setMember(m)
                                                  .setContext(view)
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
                                                  .setScheduler(scheduler)
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
                                                  .build()))
               .peek(c -> view.activate(c.getMember()))
               .forEach(e -> consortium.put(e.getMember(), e));
    }

    private void validateState(Context<Member> view, Set<Consortium> blueRibbon) {
        long clientsInWrongState = consortium.values()
                                             .stream()
                                             .filter(c -> !blueRibbon.contains(c))
                                             .map(c -> c.fsm.getCurrentState())
                                             .filter(b -> b != CollaboratorFsm.CLIENT
                                                     && b != CollaboratorFsm.JOINING_MEMBER)
                                             .count();
        Set<Transitions> failedMembers = consortium.values()
                                                   .stream()
                                                   .filter(c -> !blueRibbon.contains(c))
                                                   .filter(c -> c.fsm.getCurrentState() != CollaboratorFsm.CLIENT
                                                           && c.fsm.getCurrentState() != CollaboratorFsm.JOINING_MEMBER)
                                                   .map(c -> c.fsm.getCurrentState())
                                                   .collect(Collectors.toSet());
        assertEquals(0, clientsInWrongState, "True clients gone bad: " + failedMembers);
        assertEquals(view.getRingCount() - 1,
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
