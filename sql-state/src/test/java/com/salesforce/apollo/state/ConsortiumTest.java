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
import java.sql.SQLException;
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

import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.state.proto.Statement;
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
    private static final byte[]                            GENESIS_DATA    = "Give me FOOD or give me SLACK or KILL ME".getBytes();
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
    private Map<HashKey, Router>          communications = new HashMap<>();
    private final Map<Member, Consortium> consortium     = new HashMap<>();
    private final Map<Member, CdcEngine>  engines        = new HashMap<>();
    private SecureRandom                  entropy;
    private List<Member>                  members;
    private final Map<Member, Updater>    updaters       = new HashMap<>();

    @AfterEach
    public void after() {
        consortium.values().forEach(e -> e.stop());
        consortium.clear();
        communications.values().forEach(e -> e.close());
        communications.clear();
        engines.values().forEach(cdc -> cdc.close());
        engines.clear();
        updaters.values().forEach(up -> up.close());
        updaters.clear();
    }

    @BeforeEach
    public void before() {

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

    }

    @Test
    public void smoke() throws Exception {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(testCardinality);

        Context<Member> view = new Context<>(HashKey.ORIGIN.prefix(1), 3);
        Messenger.Parameters msgParameters = Messenger.Parameters.newBuilder()
                                                                 .setFalsePositiveRate(0.001)
                                                                 .setBufferSize(100)
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
                        if (m.process(c)) {
                            processed.get().countDown();
                        }
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
            hash = client.submit((h, t) -> txnProcessed.set(true),
                                 batch("insert into books values (1002, 'More Java for dummies', 'Tan Ah Teck', 22.22, 22)"));
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

        Semaphore outstanding = new Semaphore(100); // outstanding, unfinalized txns
        int bunchCount = 1_000;
        System.out.println("Submitting bunch: " + bunchCount);
        ArrayList<HashKey> submitted = new ArrayList<>();
        CountDownLatch submittedBunch = new CountDownLatch(bunchCount);
        for (int i = 0; i < bunchCount; i++) {
            outstanding.acquire();
            try {
                HashKey pending = client.submit((h, t) -> {
                    outstanding.release();
                    submitted.remove(h);
                    submittedBunch.countDown();
                }, batch("insert into books values (1001, 'Java for dummies', 'Tan Ah Teck', 11.11, 11)"),
                                                batch("insert into books values (1002, 'More Java for dummies', 'Tan Ah Teck', 22.22, 22)"),
                                                batch("insert into books values (1003, 'More Java for more dummies', 'Mohammad Ali', 33.33, 33)"),
                                                batch("insert into books values (1004, 'A Cup of Java', 'Kumar', 44.44, 44)"),
                                                batch("insert into books values (1005, 'A Teaspoon of Java', 'Kevin Jones', 55.55, 55)"));
                submitted.add(pending);
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

    private void gatherConsortium(Context<Member> view, BiFunction<CertifiedBlock, Future<?>, HashKey> consensus,
                                  Duration gossipDuration, ScheduledExecutorService scheduler,
                                  Messenger.Parameters msgParameters) {
        members.stream().map(m -> {
            String url = String.format("jdbc:h2:mem:test_engine-%s-%s", m.getId(), entropy.nextLong());
            System.out.println("DB URL: " + url);
            CdcEngine engine = new CdcEngine(url, new Properties());
            engines.put(m, engine);
            Updater up = engine.getUpdater();
            updaters.put(m, up);
            Connection connection = engine.newConnection();

            java.sql.Statement statement;
            try {
                statement = connection.createStatement();
                statement.execute("create table books (id int, title varchar(50), author varchar(50), price float, qty int,  primary key (id))");
            } catch (SQLException e1) {
                throw new IllegalStateException(e1);
            }
            Consortium c = new Consortium(
                    Parameters.newBuilder()
                              .setConsensus(consensus)
                              .setValidator(engine)
                              .setMember(m)
                              .setSignature(() -> SigningUtils.forSigning(certs.get(m.getId()).getPrivateKey(),
                                                                          entropy))
                              .setContext(view)
                              .setMsgParameters(msgParameters)
                              .setMaxBatchByteSize(1024 * 1024)
                              .setMaxBatchSize(1000)
                              .setCommunications(communications.get(m.getId()))
                              .setMaxBatchDelay(Duration.ofMillis(100))
                              .setGossipDuration(gossipDuration)
                              .setViewTimeout(Duration.ofMillis(500))
                              .setJoinTimeout(Duration.ofSeconds(5))
                              .setTransactonTimeout(Duration.ofSeconds(15))
                              .setExecutor(up)
                              .setScheduler(scheduler)
                              .setGenesisData(GENESIS_DATA)
                              .build());
            return c;
        }).peek(c -> view.activate(c.getMember())).forEach(e -> consortium.put(e.getMember(), e));
    }

    private Statement batch(String sql) {
        return Statement.newBuilder().setSql(sql).build();
    }
}
