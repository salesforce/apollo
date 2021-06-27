/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import static com.salesforce.apollo.state.Mutator.batch;
import static com.salesforce.apollo.state.Mutator.batchOf;
import static com.salesforce.apollo.test.pregen.PregenPopulation.getMember;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.h2.mvstore.MVStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.salesfoce.apollo.messaging.proto.ByteMessage;
import com.salesfoce.apollo.state.proto.BatchUpdate;
import com.salesforce.apollo.avalanche.Avalanche;
import com.salesforce.apollo.avalanche.AvalancheParameters;
import com.salesforce.apollo.avalanche.DagDao;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.comm.ServerConnectionCache.Builder;
import com.salesforce.apollo.consortium.Consortium;
import com.salesforce.apollo.consortium.Parameters;
import com.salesforce.apollo.consortium.ViewContext;
import com.salesforce.apollo.consortium.fsm.CollaboratorFsm;
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
public class AvaTest {

    private static Map<Digest, CertificateWithPrivateKey> certs;
    private static final Message                          GENESIS_DATA    = batch("create table books (id int, title varchar(50), author varchar(50), price float, qty int,  primary key (id))");
    private static final Digest                           GENESIS_VIEW_ID = DigestAlgorithm.DEFAULT.digest("Give me food or give me slack or kill me".getBytes());
    private static final Duration                         gossipDuration  = Duration.ofMillis(10);

    private final static int      testCardinality = 25;
    private static final Duration timeout         = Duration.ofSeconds(20);

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(0, testCardinality)
                         .parallel()
                         .mapToObj(i -> getMember(i))
                         .collect(Collectors.toMap(cert -> Member.getMemberIdentifier(cert.getX509Certificate()),
                                                   cert -> cert));
    }

    private final Map<Member, Avalanche>       avas           = new ConcurrentHashMap<>();
    private File                               baseDir;
    private Builder                            builder        = ServerConnectionCache.newBuilder().setTarget(30);
    private File                               checkpointDirBase;
    private Map<Digest, Router>                communications = new ConcurrentHashMap<>();
    private final Map<Member, Consortium>      consortium     = new ConcurrentHashMap<>();
    private SecureRandom                       entropy;
    private List<SigningMember>                members;
    private final Map<Member, SqlStateMachine> updaters       = new ConcurrentHashMap<>();

    @AfterEach
    public void after() {
        avas.values().forEach(e -> e.stop());
        avas.clear();
        consortium.values().forEach(e -> e.stop());
        consortium.clear();
        communications.values().forEach(e -> e.close());
        communications.clear();
        updaters.values().forEach(up -> up.close());
        updaters.clear();
    }

    @BeforeEach
    public void before() {
        checkpointDirBase = new File("target/ava-chkpoints");
        Utils.clean(checkpointDirBase);
        baseDir = new File(System.getProperty("user.dir"), "target/cluster");
        Utils.clean(baseDir);
        baseDir.mkdirs();
        entropy = new SecureRandom();

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

        ForkJoinPool executor = ForkJoinPool.commonPool();
        members.forEach(node -> communications.put(node.getId(), new LocalRouter(node, builder, executor)));

    }

    @Test
    public void smoke() throws Exception {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(testCardinality);

        Context<Member> view = new Context<>(DigestAlgorithm.DEFAULT.getOrigin().prefix(1), 5);
        Messenger.Parameters msgParameters = Messenger.Parameters.newBuilder()
                                                                 .setFalsePositiveRate(0.001)
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
        adapters.values().forEach(p -> p.getAvalanche().start(scheduler, Duration.ofMillis(50)));
        genesis(adapters.get(members.get(0)).getAvalanche());

        System.out.println("starting consortium");
        consortium.values().forEach(e -> e.start());

        System.out.println("awaiting genesis processing");
        awaitGenesis(processed, blueRibbon);

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

        System.out.println("genesis processing complete");

        processed.set(new CountDownLatch(testCardinality));
        Consortium client = consortium.values().stream().filter(c -> !blueRibbon.contains(c)).findFirst().get();
        AtomicBoolean txnProcessed = new AtomicBoolean();

        System.out.println("Submitting transaction");
        Mutator mutator = new Mutator(client);
        Digest hash = mutator.execute(batch("insert into books values (1001, 'Java for dummies', 'Tan Ah Teck', 11.11, 11)",
                                            "insert into books values (1002, 'More Java for dummies', 'Tan Ah Teck', 22.22, 22)",
                                            "insert into books values (1003, 'More Java for more dummies', 'Mohammad Ali', 33.33, 33)",
                                            "insert into books values (1004, 'A Cup of Java', 'Kumar', 44.44, 44)",
                                            "insert into books values (1005, 'A Teaspoon of Java', 'Kevin Jones', 55.55, 55)"),
                                      (h, t) -> txnProcessed.set(true), timeout);
        System.out.println("Submitted transaction: " + hash + ", awaiting processing of next block");
        assertTrue(processed.get().await(30, TimeUnit.SECONDS), "Did not process transaction block");

        System.out.println("block processed, waiting for transaction completion: " + hash);
        assertTrue(Utils.waitForCondition(5_000, () -> txnProcessed.get()), "Transaction not completed");
        System.out.println("transaction completed: " + hash);
        System.out.println();

        long then = System.currentTimeMillis();
        Semaphore outstanding = new Semaphore(500); // outstanding, unfinalized txns
        int bunchCount = 10_000;
        System.out.println("Submitting bunch: " + bunchCount);
        Set<Digest> submitted = new HashSet<>();
        CountDownLatch submittedBunch = new CountDownLatch(bunchCount);

        AtomicInteger txnr = new AtomicInteger();
        Executor exec = Executors.newFixedThreadPool(5, r -> {
            Thread t = new Thread(r, "Transactioneer [" + txnr.getAndIncrement() + "]");
            t.setDaemon(true);
            return t;
        });
        IntStream.range(0, bunchCount).parallel().forEach(i -> exec.execute(() -> {
            try {
                outstanding.acquire();
            } catch (InterruptedException e1) {
                throw new IllegalStateException(e1);
            }
            List<List<Object>> batch = new ArrayList<>();
            for (int rep = 0; rep < 10; rep++) {
                for (int id = 1; id < 6; id++) {
                    batch.add(Arrays.asList(entropy.nextInt(), 1000 + id));
                }
            }
            BatchUpdate update = batchOf("update books set qty = ? where id = ?", batch);
            AtomicReference<Digest> key = new AtomicReference<>();

            try {
                key.set(mutator.execute(update, (h, t) -> {
                    outstanding.release();
                    submitted.remove(key.get());
                    submittedBunch.countDown();
                }, timeout));
                submitted.add(key.get());
            } catch (TimeoutException e1) {
                outstanding.release();
                submittedBunch.countDown();
            }
        }));

        System.out.println("Awaiting " + bunchCount + " batches");
        boolean completed = submittedBunch.await(125, TimeUnit.SECONDS);
        long now = System.currentTimeMillis() - then;
        assertTrue(completed, "Did not process transaction bunch: " + submittedBunch.getCount());
        System.out.println("Completed additional " + bunchCount + " transactions");
        double perSecond = now / 1000.0;
        System.out.println("Statements per second: " + (bunchCount * 40) / perSecond);
        System.out.println("Transactions per second: " + (bunchCount) / perSecond);
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
            ForkJoinPool fj = Router.createFjPool();
            AvaAdapter adapter = new AvaAdapter(processed);
            String url = String.format("jdbc:h2:mem:test_engine-%s-%s", m.getId(), entropy.nextLong());
            System.out.println("DB URL: " + url);
            SqlStateMachine up = new SqlStateMachine(url, new Properties(),
                    new File(checkpointDirBase, m.getId().toString()), fj);
            updaters.put(m, up);
            Consortium c = new Consortium(Parameters.newBuilder()
                                                    .setConsensus(adapter.getConsensus())
                                                    .setMember(m)
                                                    .setContext(view)
                                                    .setDispatcher(fj)
                                                    .setMsgParameters(msgParameters)
                                                    .setMaxBatchByteSize(1024 * 1024 * 32)
                                                    .setMaxBatchSize(1000)
                                                    .setCommunications(communications.get(m.getId()))
                                                    .setMaxBatchDelay(Duration.ofMillis(500))
                                                    .setGossipDuration(gossipDuration)
                                                    .setViewTimeout(Duration.ofMillis(500))
                                                    .setJoinTimeout(Duration.ofSeconds(5))
                                                    .setTransactonTimeout(Duration.ofSeconds(15))
                                                    .setExecutor(up.getExecutor())
                                                    .setScheduler(scheduler)
                                                    .setGenesisData(GENESIS_DATA)
                                                    .setGenesisViewId(GENESIS_VIEW_ID)
                                                    .setCheckpointer(up.getCheckpointer())
                                                    .setDeltaCheckpointBlocks(10)
                                                    .build());
            adapter.setConsortium(c);
            adapters.put(m, adapter);
            return c;
        }).peek(c -> view.activate(c.getMember())).forEach(e -> consortium.put(e.getMember(), e));
        return adapters;
    }

    private Digest genesis(Avalanche master) {
        Digest genesisKey = master.submitGenesis(ByteMessage.newBuilder()
                                                            .setContents(ByteString.copyFromUtf8("Genesis"))
                                                            .build());
        assertNotNull(genesisKey);
        DagDao dao = new DagDao(master.getDag());
        boolean completed = Utils.waitForCondition(10_000, () -> dao.isFinalized(genesisKey));
        assertTrue(completed, "did not generate genesis root");
        return genesisKey;
    }

}
