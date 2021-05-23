/*
f * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import static com.salesforce.apollo.test.pregen.PregenPopulation.getMember;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
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

import org.apache.commons.math3.random.BitsStreamGenerator;
import org.apache.commons.math3.random.MersenneTwister;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.protobuf.Message;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.state.proto.BatchUpdate;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.comm.ServerConnectionCache.Builder;
import com.salesforce.apollo.consortium.Consortium;
import com.salesforce.apollo.consortium.Parameters;
import com.salesforce.apollo.consortium.ViewContext;
import com.salesforce.apollo.consortium.fsm.CollaboratorFsm;
import com.salesforce.apollo.consortium.support.SigningUtils;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.ReservoirSampler;
import com.salesforce.apollo.membership.messaging.Messenger;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.Utils;

import io.github.olivierlemasle.ca.CertificateWithPrivateKey;

/**
 * @author hal.hildebrand
 *
 */
public class ConsortiumTest {

    private static Map<HashKey, CertificateWithPrivateKey> certs;
    private static final Message                           GENESIS_DATA    = Helper.batch(Helper.batch("create table books (id int, title varchar(50), author varchar(50), price float, qty int,  primary key (id))"));
    private static final HashKey                           GENESIS_VIEW_ID = new HashKey(
            Conversion.hashOf("Give me food or give me slack or kill me".getBytes()));
    private static final Duration                          gossipDuration  = Duration.ofMillis(10);
    private final static int                               testCardinality = 25;

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(1, testCardinality + 1)
                         .parallel()
                         .mapToObj(i -> getMember(i))
                         .collect(Collectors.toMap(cert -> Utils.getMemberId(cert.getX509Certificate()), cert -> cert));
    }

    private File                               baseDir;
    private Builder                            builder        = ServerConnectionCache.newBuilder().setTarget(30);
    private File                               checkpointDirBase;
    private Map<HashKey, Router>               communications = new ConcurrentHashMap<>();
    private final Map<Member, Consortium>      consortium     = new ConcurrentHashMap<>();
    private List<Member>                       members;
    private final Map<Member, SqlStateMachine> updaters       = new ConcurrentHashMap<>();

    @AfterEach
    public void after() {
        updaters.values().forEach(up -> up.close());
        updaters.clear();
        consortium.values().forEach(e -> e.stop());
        consortium.clear();
        communications.values().forEach(e -> e.close());
        communications.clear();
    }

    @BeforeEach
    public void before() {
        checkpointDirBase = new File("target/ct-chkpoints");
        Utils.clean(checkpointDirBase);
        baseDir = new File(System.getProperty("user.dir"), "target/cluster");
        Utils.clean(baseDir);
        baseDir.mkdirs();
        assertTrue(certs.size() >= testCardinality);

        members = new ArrayList<>();
        for (CertificateWithPrivateKey cert : certs.values()) {
            if (members.size() < testCardinality) {
                members.add(new Member(cert.getX509Certificate()));
            } else {
                break;
            }
        }

        assertEquals(testCardinality, members.size());

        ExecutorService serverThreads = ForkJoinPool.commonPool();
        members.forEach(node -> {
            communications.put(node.getId(), new LocalRouter(node, builder, serverThreads));
        });

        System.out.println(members.stream().map(m -> m.getId()).collect(Collectors.toList()));

    }

    @Test
    public void smoke() throws Exception {
        AtomicInteger label = new AtomicInteger();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "Scheduler [" + label.getAndIncrement() + "]");
            t.setDaemon(true);
            return t;
        });

        Context<Member> view = new Context<>(HashKey.ORIGIN.prefix(1), 5);
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
        Set<HashKey> decided = Collections.newSetFromMap(new ConcurrentHashMap<>());
        BiFunction<CertifiedBlock, CompletableFuture<?>, HashKey> consensus = (c, f) -> {
            HashKey hash = new HashKey(Conversion.hashOf(c.getBlock().toByteString()));
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
        gatherConsortium(view, consensus, gossipDuration, scheduler, msgParameters);

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

        System.out.println("genesis processing complete");

        processed.set(new CountDownLatch(testCardinality));
        BitsStreamGenerator entropy = new MersenneTwister();
        AtomicBoolean txnProcessed = new AtomicBoolean();

        System.out.println("Submitting transaction");
        HashKey hash;
        Consortium client = consortium.values()
                                      .stream()
                                      .collect(new ReservoirSampler<Consortium>(null, 1, entropy))
                                      .get(0);
        hash = client.submit(null, (h, t) -> txnProcessed.set(true),
                             Helper.batch("insert into books values (1001, 'Java for dummies', 'Tan Ah Teck', 11.11, 11)",
                                          "insert into books values (1002, 'More Java for dummies', 'Tan Ah Teck', 22.22, 22)",
                                          "insert into books values (1003, 'More Java for more dummies', 'Mohammad Ali', 33.33, 33)",
                                          "insert into books values (1004, 'A Cup of Java', 'Kumar', 44.44, 44)",
                                          "insert into books values (1005, 'A Teaspoon of Java', 'Kevin Jones', 55.55, 55)"));

        System.out.println("Submitted transaction: " + hash + ", awaiting processing of next block");
        assertTrue(processed.get().await(30, TimeUnit.SECONDS), "Did not process transaction block");

        System.out.println("block processed, waiting for transaction completion: " + hash);
        assertTrue(Utils.waitForCondition(5_000, () -> txnProcessed.get()), "Transaction not completed");
        System.out.println("transaction completed: " + hash);
        System.out.println();

        long then = System.currentTimeMillis();
        Semaphore outstanding = new Semaphore(2000); // outstanding, unfinalized txns
        int bunchCount = 10_000;
        System.out.println("Submitting batches: " + bunchCount);
        Set<HashKey> submitted = new HashSet<>();
        CountDownLatch submittedBunch = new CountDownLatch(bunchCount);

        AtomicInteger txnr = new AtomicInteger();
        Executor exec = Executors.newFixedThreadPool(5, r -> {
            Thread t = new Thread(r, "Transactioneer [" + txnr.getAndIncrement() + "]");
            t.setDaemon(true);
            return t;
        });
        IntStream.range(0, bunchCount).forEach(i -> exec.execute(() -> {
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
            BatchUpdate update = Helper.batchOf("update books set qty = ? where id = ?", batch);
            AtomicReference<HashKey> key = new AtomicReference<>();
            Consortium c = consortium.values().stream().collect(new ReservoirSampler<Consortium>(null, 1, entropy)).get(0);
            key.set(c.submit(null, (h, t) -> {
                outstanding.release();
                submitted.remove(key.get());
                submittedBunch.countDown();
            }, Helper.batch(update)));
            submitted.add(key.get());
        }));

        System.out.println("Awaiting " + bunchCount + " batches");
        boolean completed = submittedBunch.await(240, TimeUnit.SECONDS);
        long now = System.currentTimeMillis() - then;
        assertTrue(completed, "Did not process transaction batches: " + submittedBunch.getCount());
        System.out.println("Completed additional " + bunchCount + " transactions");
        double perSecond = now / 1000.0;
        System.out.println("Statements per second: " + (bunchCount * 40) / perSecond);
        System.out.println("Transactions per second: " + (bunchCount) / perSecond);

        Connection connection = updaters.get(members.get(0)).newConnection();
        Statement statement = connection.createStatement();
        ResultSet results = statement.executeQuery("select ID, from books");
        ResultSetMetaData rsmd = results.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        while (results.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1)
                    System.out.print(",  ");
                Object columnValue = results.getObject(i);
                System.out.print(columnValue + " " + rsmd.getColumnName(i));
            }
            System.out.println("");
        }
    }

    private void gatherConsortium(Context<Member> view,
                                  BiFunction<CertifiedBlock, CompletableFuture<?>, HashKey> consensus,
                                  Duration gossipDuration, ScheduledExecutorService scheduler,
                                  Messenger.Parameters msgParameters) {
        AtomicBoolean frist = new AtomicBoolean(true);
        Random entropy = new Random(0x1638);
        members.stream().map(m -> {
            ForkJoinPool fj = ForkJoinPool.commonPool();
            String url = String.format("jdbc:h2:mem:test_engine-%s-%s", m.getId(), entropy.nextLong());
            frist.set(false);
            System.out.println("DB URL: " + url);
            SqlStateMachine up = new SqlStateMachine(url, new Properties(),
                    new File(checkpointDirBase, m.getId().toString()), fj);
            updaters.put(m, up);
            Consortium c = new Consortium(
                    Parameters.newBuilder()
                              .setConsensus(consensus)
                              .setMember(m)
                              .setSignature(() -> SigningUtils.forSigning(certs.get(m.getId()).getPrivateKey(),
                                                                          Utils.secureEntropy()))
                              .setContext(view)
                              .setMsgParameters(msgParameters)
                              .setMaxBatchByteSize(1024 * 1024 * 32)
                              .setMaxBatchSize(4000)
                              .setCommunications(communications.get(m.getId()))
                              .setMaxBatchDelay(Duration.ofMillis(500))
                              .setGossipDuration(gossipDuration)
                              .setViewTimeout(Duration.ofMillis(500))
                              .setJoinTimeout(Duration.ofSeconds(2))
                              .setDeltaCheckpointBlocks(10)
                              .setTransactonTimeout(Duration.ofSeconds(15))
                              .setExecutor(up.getExecutor())
                              .setScheduler(scheduler)
                              .setGenesisData(GENESIS_DATA)
                              .setGenesisViewId(GENESIS_VIEW_ID)
                              .setCheckpointer(up.getCheckpointer())
                              .build());
            return c;
        }).peek(c -> view.activate(c.getMember())).forEach(e -> consortium.put(e.getMember(), e));
    }
}
