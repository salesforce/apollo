/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import static com.salesforce.apollo.test.pregen.PregenPopulation.getCa;
import static com.salesforce.apollo.test.pregen.PregenPopulation.getMember;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.h2.mvstore.MVStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.salesfoce.apollo.consortium.proto.ByteTransaction;
import com.salesfoce.apollo.proto.ByteMessage;
import com.salesforce.apollo.avalanche.Avalanche;
import com.salesforce.apollo.avalanche.AvalancheParameters;
import com.salesforce.apollo.avalanche.DagDao;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.comm.ServerConnectionCache.Builder;
import com.salesforce.apollo.consortium.fsm.CollaboratorFsm;
import com.salesforce.apollo.fireflies.FirefliesParameters;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.fireflies.View;
import com.salesforce.apollo.membership.CertWithKey;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.Messenger;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.Utils;

import io.github.olivierlemasle.ca.CertificateWithPrivateKey;
import io.github.olivierlemasle.ca.RootCertificate;

/**
 * @author hal.hildebrand
 *
 */
public class AvaConsensusTest {

    private static final RootCertificate                   ca              = getCa();
    private static Map<HashKey, CertificateWithPrivateKey> certs;
    private static final Message                           GENESIS_DATA    = ByteMessage.newBuilder()
                                                                                        .setContents(ByteString.copyFromUtf8("Give me food or give me slack or kill me"))
                                                                                        .build();
    private static final HashKey                           GENESIS_VIEW_ID = new HashKey(
            Conversion.hashOf("Give me food or give me slack or kill me".getBytes()));
    private static final Duration                          gossipDuration  = Duration.ofMillis(10);
    private static final FirefliesParameters               parameters      = new FirefliesParameters(
            ca.getX509Certificate());
    private final static int                               testCardinality = 5;

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(1, testCardinality + 1)
                         .parallel()
                         .mapToObj(i -> getMember(i))
                         .collect(Collectors.toMap(cert -> Utils.getMemberId(cert.getX509Certificate()), cert -> cert));
    }

    protected ScheduledExecutorService    scheduler;
    protected List<View>                  views;
    private final Map<Member, Avalanche>  avas           = new HashMap<>();
    private File                          baseDir;
    private Builder                       builder        = ServerConnectionCache.newBuilder().setTarget(30);
    private Map<HashKey, Router>          communications = new ConcurrentHashMap<>();
    private final Map<Member, Consortium> consortium     = new ConcurrentHashMap<>();
    private List<Node>                    members;

    @AfterEach
    public void after() {
        avas.values().forEach(e -> e.stop());
        avas.clear();
        consortium.values().forEach(e -> e.stop());
        consortium.clear();
        communications.values().forEach(e -> e.close());
        communications.clear();
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
                members.add(new Node(new CertWithKey(cert.getX509Certificate(), cert.getPrivateKey()), parameters));
            } else {
                break;
            }
        }

        assertEquals(testCardinality, members.size());

        ForkJoinPool executor = ForkJoinPool.commonPool();
        members.forEach(node -> communications.put(node.getId(), new LocalRouter(node, builder, executor)));

        System.out.println("Test cardinality: " + testCardinality);

    }

    @Test
    public void e2e() throws Exception {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(testCardinality);

        Context<Member> view = new Context<>(HashKey.ORIGIN.prefix(1), 3);
        Messenger.Parameters msgParameters = Messenger.Parameters.newBuilder()
                                                                 .setFalsePositiveRate(0.00001)
                                                                 .setBufferSize(1000)
                                                                 .build();
        AtomicReference<CountDownLatch> processed = new AtomicReference<>(new CountDownLatch(testCardinality));
        Map<Member, AvaAdapter> adapters = gatherConsortium(view, processed, gossipDuration, scheduler, msgParameters);
        gatherAvalanche(view, adapters);

        Set<Consortium> blueRibbon = new HashSet<>();
        ViewContext.viewFor(GENESIS_VIEW_ID, view).allMembers().forEach(e -> {
            blueRibbon.add(consortium.get(e));
        });

        communications.values().forEach(r -> r.start());
        adapters.values().forEach(p -> p.getAvalanche().start(scheduler, Duration.ofMillis(2)));
        genesis(adapters.get(members.get(0)).getAvalanche());

        System.out.println("starting consortium");

        consortium.values().forEach(e -> e.start());

        System.out.println("awaiting genesis processing");

        awaitGenesis(processed, blueRibbon);

        System.out.println("genesis processing complete, validating state");

        processed.set(new CountDownLatch(testCardinality));
        Consortium client = consortium.values().stream().filter(c -> !blueRibbon.contains(c)).findFirst().get();
        AtomicBoolean txnProcessed = new AtomicBoolean();

        System.out.println("Submitting transaction");
        HashKey hash;
        try {
            hash = client.submit(null, (h, t) -> txnProcessed.set(true),
                                 ByteTransaction.newBuilder()
                                                .setContent(ByteString.copyFromUtf8("Hello world"))
                                                .build());
        } catch (TimeoutException e) {
            fail();
            return;
        }

        System.out.println("Submitted transaction: " + hash + ", awaiting processing of next block");
        assertTrue(processed.get().await(30, TimeUnit.SECONDS), "Did not process transaction block");

        System.out.println("block processed, waiting for transaction completion: " + hash);
        assertTrue(Utils.waitForCondition(5_000, () -> txnProcessed.get()), "Transaction not completed");
        System.out.println("transaction completed: " + hash);
        System.out.println();

        Semaphore outstanding = new Semaphore(1000, true); // outstanding, unfinalized txns
        int bunchCount = 10_000;
        System.out.println("Submitting bunch: " + bunchCount);
        ArrayList<HashKey> submitted = new ArrayList<>();
        CountDownLatch submittedBunch = new CountDownLatch(bunchCount);
        for (int i = 0; i < bunchCount; i++) {
            outstanding.acquire();
            try {
                AtomicReference<HashKey> pending = new AtomicReference<>();
                pending.set(client.submit(null, (h, t) -> {
                    outstanding.release();
                    submitted.remove(pending.get());
                    submittedBunch.countDown();
                }, Any.pack(ByteTransaction.newBuilder().setContent(ByteString.copyFromUtf8("Hello world")).build())));
                submitted.add(pending.get());
            } catch (TimeoutException e) {
                fail();
                return;
            }
        }

        System.out.println("Awaiting " + bunchCount + " transactions");
        boolean completed = submittedBunch.await(125, TimeUnit.SECONDS);
        submittedBunch.getCount();
        assertTrue(completed, "Did not process transaction bunch: " + submittedBunch.getCount());
        System.out.println("Completed additional " + bunchCount + " transactions");
    }

    private void awaitGenesis(AtomicReference<CountDownLatch> processed,
                              Set<Consortium> blueRibbon) throws InterruptedException {
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
    }

    private void gatherAvalanche(Context<Member> view, Map<Member, AvaAdapter> adapters) {
        members.forEach(node -> {
            AvalancheParameters aParams = new AvalancheParameters();

            // Avalanche protocol parameters
            aParams.core.alpha = 0.6;
            aParams.core.k = Math.min(9, testCardinality);
            aParams.core.beta1 = 3;
            aParams.core.beta2 = 5;

            // Avalanche implementation parameters
            // parent selection target for avalanche dag voting
            aParams.parentCount = 5;
            aParams.queryBatchSize = 400;
            aParams.noOpsPerRound = 10;
            aParams.maxNoOpParents = 10;
            aParams.outstandingQueries = 5;
            aParams.noOpQueryFactor = 40;

            AvaAdapter adapter = adapters.get(node);
            Avalanche ava = new Avalanche(node, view, communications.get(node.getId()), aParams, null, adapter,
                    new MVStore.Builder().open(), ForkJoinPool.commonPool());
            adapter.setAva(ava);
            avas.put(node, ava);
        });
    }

    private Map<Member, AvaAdapter> gatherConsortium(Context<Member> view, AtomicReference<CountDownLatch> processed,
                                                     Duration gossipDuration, ScheduledExecutorService scheduler,
                                                     Messenger.Parameters msgParameters) {
        Map<Member, AvaAdapter> adapters = new HashMap<>();
        members.stream().map(m -> {
            AvaAdapter adapter = new AvaAdapter(processed);
            TransactionExecutor executor = (h, t, c) -> {
                if (c != null) {
                    ForkJoinPool.commonPool().execute(() -> c.accept(new HashKey(t.getHash()), null));
                }
            };
            Consortium member = new Consortium(Parameters.newBuilder()
                                                         .setConsensus(adapter.getConsensus())
                                                         .setMember(m)
                                                         .setSignature(() -> m.forSigning())
                                                         .setContext(view)
                                                         .setMsgParameters(msgParameters)
                                                         .setMaxBatchByteSize(1024 * 1024)
                                                         .setMaxBatchSize(1000)
                                                         .setCommunications(communications.get(m.getId()))
                                                         .setMaxBatchDelay(Duration.ofMillis(100))
                                                         .setGossipDuration(gossipDuration)
                                                         .setViewTimeout(Duration.ofMillis(500))
                                                         .setExecutor(executor)
                                                         .setJoinTimeout(Duration.ofSeconds(5))
                                                         .setTransactonTimeout(Duration.ofSeconds(15))
                                                         .setScheduler(scheduler)
                                                         .setGenesisData(GENESIS_DATA)
                                                         .setGenesisViewId(GENESIS_VIEW_ID)
                                                         .setDeltaCheckpointBlocks(10)
                                                         .setCheckpointer(l -> {
                                                             File temp;
                                                             try {
                                                                 temp = File.createTempFile("foo", "bar");
                                                                 temp.deleteOnExit();
                                                                 try (FileOutputStream fos = new FileOutputStream(
                                                                         temp)) {
                                                                     fos.write("Give me food or give me slack or kill me".getBytes());
                                                                     fos.flush();
                                                                 }
                                                             } catch (IOException e) {
                                                                 throw new IllegalStateException(
                                                                         "Cannot create temp file", e);
                                                             }

                                                             return temp;
                                                         })
                                                         .build());
            adapter.setConsortium(member);
            adapters.put(m, adapter);
            return member;
        }).peek(c -> view.activate(c.getMember())).forEach(e -> consortium.put(e.getMember(), e));
        return adapters;
    }

    private HashKey genesis(Avalanche master) {
        HashKey genesisKey = master.submitGenesis(ByteMessage.newBuilder()
                                                             .setContents(ByteString.copyFromUtf8("Genesis"))
                                                             .build());
        assertNotNull(genesisKey);
        DagDao dao = new DagDao(master.getDag());
        boolean completed = Utils.waitForCondition(10_000, () -> dao.isFinalized(genesisKey));
        assertTrue(completed, "did not generate genesis root");
        return genesisKey;
    }
}
