/*
 * Copyright (c) 2020, salesforce.com, inc.
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
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
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
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.comm.ServerConnectionCache.Builder;
import com.salesforce.apollo.consortium.Consortium;
import com.salesforce.apollo.consortium.Parameters;
import com.salesforce.apollo.consortium.SigningUtils;
import com.salesforce.apollo.consortium.ViewContext;
import com.salesforce.apollo.consortium.fsm.CollaboratorFsm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
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
    private static final ByteString                        GENESIS_DATA    = ByteString.copyFromUtf8("Give me FOOD or give me SLACK or KILL ME");
    private static final Duration                          gossipDuration  = Duration.ofMillis(10);
    private final static int                               testCardinality = 5;

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(1, testCardinality + 1)
                         .parallel()
                         .mapToObj(i -> getMember(i))
                         .collect(Collectors.toMap(cert -> Utils.getMemberId(cert.getX509Certificate()), cert -> cert));
    }

    private File                          baseDir;
    private Builder                       builder        = ServerConnectionCache.newBuilder().setTarget(30);
    private File                          checkpointDirBase;
    private Map<HashKey, Router>          communications = new HashMap<>();
    private final Map<Member, Consortium> consortium     = new HashMap<>();
    private SecureRandom                  entropy;
    private List<Member>                  members;
    private final Map<Member, Updater>    updaters       = new HashMap<>();

    @AfterEach
    public void after() {
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
                members.add(new Member(cert.getX509Certificate()));
            } else {
                break;
            }
        }

        assertEquals(testCardinality, members.size());

        members.forEach(node -> communications.put(node.getId(), new LocalRouter(node.getId(), builder)));

        System.out.println(members.stream().map(m -> m.getId()).collect(Collectors.toList()));

    }

    @Test
    public void smoke() throws Exception {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(testCardinality);

        Context<Member> view = new Context<>(HashKey.ORIGIN.prefix(1), 3);
        Messenger.Parameters msgParameters = Messenger.Parameters.newBuilder()
                                                                 .setFalsePositiveRate(0.001)
                                                                 .setBufferSize(1000)
                                                                 .setEntropy(new SecureRandom())
                                                                 .build();
        Executor cPipeline = Executors.newSingleThreadExecutor();
        AtomicReference<CountDownLatch> processed = new AtomicReference<>(new CountDownLatch(testCardinality));
        Set<HashKey> decided = Collections.newSetFromMap(new ConcurrentHashMap<>());
        BiFunction<CertifiedBlock, Future<?>, HashKey> consensus = (c, f) -> {
            HashKey hash = new HashKey(Conversion.hashOf(c.getBlock().toByteString()));
            if (decided.add(hash)) {
                cPipeline.execute(() -> {
                    Map<Member, Consortium> copy = new HashMap<>(consortium);
                    copy.values().parallelStream().forEach(m -> {
                        m.process(c);
                        processed.get().countDown();
                    });
                });
            }
            return hash;
        };
        gatherConsortium(view, consensus, gossipDuration, scheduler, msgParameters);

        Set<Consortium> blueRibbon = new HashSet<>();
        ViewContext.viewFor(new HashKey(Conversion.hashOf(GENESIS_DATA)), view).allMembers().forEach(e -> {
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
                                       .map(c -> c.fsm().getCurrentState())
                                       .filter(b -> b != CollaboratorFsm.CLIENT)
                                       .collect(Collectors.toSet())
                           + " : "
                           + consortium.values()
                                       .stream()
                                       .filter(c -> !blueRibbon.contains(c))
                                       .filter(c -> c.fsm().getCurrentState() != CollaboratorFsm.CLIENT)
                                       .map(c -> c.getMember())
                                       .collect(Collectors.toList()));

        System.out.println("genesis processing complete");

        processed.set(new CountDownLatch(testCardinality));
        Consortium client = consortium.values().stream().filter(c -> !blueRibbon.contains(c)).findFirst().get();
        AtomicBoolean txnProcessed = new AtomicBoolean();

        System.out.println("Submitting transaction");
        HashKey hash;
        try {
            String[] statements = { "create table books (id int, title varchar(50), author varchar(50), price float, qty int,  primary key (id))" };
            hash = client.submit((h, t) -> txnProcessed.set(true), Helper.batch(Helper.batch(statements)));
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

        long then = System.currentTimeMillis();
        Semaphore outstanding = new Semaphore(500); // outstanding, unfinalized txns
        int bunchCount = 10_000;
        System.out.println("Submitting batches: " + bunchCount);
        ArrayList<HashKey> submitted = new ArrayList<>();
        CountDownLatch submittedBunch = new CountDownLatch(bunchCount);
        String[] statements = { "insert into books values (1001, 'Java for dummies', 'Tan Ah Teck', 11.11, 11)",
                                "insert into books values (1002, 'More Java for dummies', 'Tan Ah Teck', 22.22, 22)",
                                "insert into books values (1003, 'More Java for more dummies', 'Mohammad Ali', 33.33, 33)",
                                "insert into books values (1004, 'A Cup of Java', 'Kumar', 44.44, 44)",
                                "insert into books values (1005, 'A Teaspoon of Java', 'Kevin Jones', 55.55, 55)" };
        HashKey pending = client.submit((h, t) -> {
            outstanding.release();
            submitted.remove(h);
            submittedBunch.countDown();
        }, Helper.batch(Helper.batch(statements)));
        submitted.add(pending);
        IntStream.range(0, bunchCount).forEach(i -> {
            try {
                outstanding.acquire();
            } catch (InterruptedException e1) {
                throw new IllegalStateException(e1);
            }
            try {
                String[] statements1 = { "update books set qty = " + entropy.nextInt() + " where id = 1001",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1002",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1003",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1004",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1005",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1001",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1002",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1003",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1004",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1005",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1001",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1002",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1003",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1004",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1005",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1001",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1002",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1003",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1004",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1005",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1001",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1002",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1003",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1004",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1005",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1001",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1002",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1003",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1004",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1005",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1001",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1002",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1003",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1004",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1005",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1001",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1002",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1003",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1004",
                                         "update books set qty = " + entropy.nextInt() + " where id = 1005" };
                HashKey key = client.submit((h, t) -> {
                    outstanding.release();
                    submitted.remove(h);
                    submittedBunch.countDown();
                }, Helper.batch(Helper.batch(statements1)));
                submitted.add(key);
            } catch (TimeoutException e) {
                fail();
                return;
            }
        });

        System.out.println("Awaiting " + bunchCount + " batches");
        boolean completed = submittedBunch.await(125, TimeUnit.SECONDS);
        long now = System.currentTimeMillis() - then;
        assertTrue(completed, "Did not process transaction batches: " + submittedBunch.getCount());
        System.out.println("Completed additional " + bunchCount + " transactions");
        double perSecond = now / 1000.0;
        System.out.println("Statements per second: " + (bunchCount * 40) / perSecond);
        System.out.println("Transactions per second: " + (bunchCount) / perSecond);

        Connection connection = updaters.get(members.get(0)).newConnection();
        Statement statement = connection.createStatement();
        ResultSet results = statement.executeQuery("select ID, __BLOCK_HEIGHT__ from books");
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

    private void gatherConsortium(Context<Member> view, BiFunction<CertifiedBlock, Future<?>, HashKey> consensus,
                                  Duration gossipDuration, ScheduledExecutorService scheduler,
                                  Messenger.Parameters msgParameters) {
        AtomicBoolean frist = new AtomicBoolean(true);
        members.stream().map(m -> {
            String url = String.format("jdbc:h2:mem:test_engine-%s-%s", m.getId(), entropy.nextLong());
            frist.set(false);
            System.out.println("DB URL: " + url);
            Updater up = new Updater(url, new Properties(), new File(checkpointDirBase, m.getId().toString()));
            updaters.put(m, up);
            Consortium c = new Consortium(
                    Parameters.newBuilder()
                              .setConsensus(consensus)
                              .setMember(m)
                              .setSignature(() -> SigningUtils.forSigning(certs.get(m.getId()).getPrivateKey(),
                                                                          entropy))
                              .setContext(view)
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
                              .setGenesisData(GENESIS_DATA.toByteArray())
                              .setCheckpointer(up.getCheckpointer())
                              .build());
            return c;
        }).peek(c -> view.activate(c.getMember())).forEach(e -> consortium.put(e.getMember(), e));
    }
}
