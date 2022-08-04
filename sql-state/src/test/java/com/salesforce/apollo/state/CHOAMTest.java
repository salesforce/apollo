/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import static com.salesforce.apollo.state.Mutator.batch;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.joou.ULong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.state.proto.Txn;
import com.salesforce.apollo.choam.CHOAM;
import com.salesforce.apollo.choam.CHOAM.TransactionExecutor;
import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Parameters.Builder;
import com.salesforce.apollo.choam.Parameters.ProducerParameters;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.choam.support.ChoamMetrics;
import com.salesforce.apollo.choam.support.ChoamMetricsImpl;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.ContextImpl;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class CHOAMTest {
    private static final int CARDINALITY;

    private static final List<Transaction> GENESIS_DATA;
    private static final Digest            GENESIS_VIEW_ID = DigestAlgorithm.DEFAULT.digest("Give me food or give me slack or kill me".getBytes());
    private static final boolean           LARGE_TESTS     = Boolean.getBoolean("large_tests");
    static {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
            LoggerFactory.getLogger(CHOAMTest.class).error("Error on thread: {}", t.getName(), e);
        });
        var txns = MigrationTest.initializeBookSchema();
        txns.add(initialInsert());
        GENESIS_DATA = CHOAM.toGenesisData(txns);
        CARDINALITY = LARGE_TESTS ? 10 : 5;
    }

    private static Txn initialInsert() {
        return Txn.newBuilder()
                  .setBatch(batch("insert into books values (1001, 'Java for dummies', 'Tan Ah Teck', 11.11, 11)",
                                  "insert into books values (1002, 'More Java for dummies', 'Tan Ah Teck', 22.22, 22)",
                                  "insert into books values (1003, 'More Java for more dummies', 'Mohammad Ali', 33.33, 33)",
                                  "insert into books values (1004, 'A Cup of Java', 'Kumar', 44.44, 44)",
                                  "insert into books values (1005, 'A Teaspoon of Java', 'Kevin Jones', 55.55, 55)"))
                  .build();
    }

    private File                baseDir;
    private File                checkpointDirBase;
    private Map<Digest, CHOAM>  choams;
    private List<SigningMember> members;
    private MetricRegistry      registry;
    private Map<Digest, Router> routers;

    private final Map<Member, SqlStateMachine> updaters = new ConcurrentHashMap<>();

    @AfterEach
    public void after() throws Exception {
        if (routers != null) {
            routers.values().forEach(e -> e.close());
            routers = null;
        }
        if (choams != null) {
            choams.values().forEach(e -> e.stop());
            choams = null;
        }
        updaters.values().forEach(up -> up.close());
        updaters.clear();
        members = null;
        System.out.println();

        ConsoleReporter.forRegistry(registry)
                       .convertRatesTo(TimeUnit.SECONDS)
                       .convertDurationsTo(TimeUnit.MILLISECONDS)
                       .build()
                       .report();
        registry = null;
    }

    @BeforeEach
    public void before() throws Exception {
        registry = new MetricRegistry();
        checkpointDirBase = new File("target/ct-chkpoints-" + Entropy.nextBitsStreamLong());
        Utils.clean(checkpointDirBase);
        baseDir = new File(System.getProperty("user.dir"), "target/cluster-" + Entropy.nextBitsStreamLong());
        Utils.clean(baseDir);
        baseDir.mkdirs();
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var context = new ContextImpl<>(DigestAlgorithm.DEFAULT.getOrigin(), CARDINALITY, 0.2, 3);
        var metrics = new ChoamMetricsImpl(context.getId(), registry);

        var params = Parameters.newBuilder()
                               .setGenesisViewId(GENESIS_VIEW_ID)
                               .setGossipDuration(Duration.ofMillis(10))
                               .setProducer(ProducerParameters.newBuilder()
                                                              .setGossipDuration(Duration.ofMillis(10))
                                                              .setBatchInterval(Duration.ofMillis(15))
                                                              .setMaxBatchByteSize(1024 * 1024)
                                                              .setMaxBatchCount(3000)
                                                              .build())
                               .setCheckpointBlockDelta(2);

        params.getProducer().ethereal().setNumberOfEpochs(7).setEpochLength(60);
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);

        members = IntStream.range(0, CARDINALITY).mapToObj(i -> {
            try {
                return stereotomy.newIdentifier().get();
            } catch (InterruptedException | ExecutionException e) {
                throw new IllegalStateException(e);
            }
        }).map(cpk -> new ControlledIdentifierMember(cpk)).map(e -> (SigningMember) e).toList();
        members.forEach(m -> context.activate(m));
        final var prefix = UUID.randomUUID().toString();
        routers = members.stream().collect(Collectors.toMap(m -> m.getId(), m -> {
            var localRouter = new LocalRouter(prefix, ServerConnectionCache.newBuilder().setTarget(30),
                                              Executors.newFixedThreadPool(2,
                                                                           r -> new Thread(r,
                                                                                           "Comm[" + m.getId() + "]")),
                                              metrics.limitsMetrics());
            localRouter.setMember(m);
            return localRouter;
        }));
        choams = members.stream().collect(Collectors.toMap(m -> m.getId(), m -> {
            return createCHOAM(entropy, params, m, context, metrics);
        }));
    }

    @Test
    public void submitMultiplTxn() throws Exception {
        final Random entropy = new Random();
        final Duration timeout = Duration.ofSeconds(12);
        var transactioneers = new ArrayList<Transactioneer>();
        final int clientCount = LARGE_TESTS ? 1_000 : 2;
        final int max = LARGE_TESTS ? 50 : 10;
        final CountDownLatch countdown = new CountDownLatch(choams.size() * clientCount);

        System.out.println("Warm up");
        routers.values().forEach(r -> r.start());
        choams.values().forEach(ch -> ch.start());

        final var activated = Utils.waitForCondition(30_000, 1_000,
                                                     () -> choams.values()
                                                                 .stream()
                                                                 .filter(c -> !c.active())
                                                                 .count() == 0);
        assertTrue(activated, "System did not become active: "
        + (choams.entrySet().stream().map(e -> e.getValue()).filter(c -> !c.active()).map(c -> c.getId()).toList()));

        updaters.entrySet().forEach(e -> {
            var mutator = e.getValue().getMutator(choams.get(e.getKey().getId()).getSession());
            var txScheduler = Executors.newScheduledThreadPool(1);
            var exec = Executors.newFixedThreadPool(2);
            for (int i = 0; i < clientCount; i++) {
                transactioneers.add(new Transactioneer(() -> update(entropy, mutator), mutator, timeout, max, exec,
                                                       countdown, txScheduler));
            }
        });
        System.out.println("Starting txns");
        transactioneers.stream().forEach(e -> e.start());
        final var finished = countdown.await(LARGE_TESTS ? 1200 : 120, TimeUnit.SECONDS);
        assertTrue(finished, "did not finish transactions: " + countdown.getCount() + " txneers: "
        + transactioneers.stream().map(t -> t.completed()).toList());

        try {
            assertTrue(Utils.waitForCondition(20_000, 1000, () -> {
                if (transactioneers.stream()
                                   .mapToInt(t -> t.inFlight())
                                   .filter(t -> t == 0)
                                   .count() != transactioneers.size()) {
                    return false;
                }
                final ULong target = updaters.values()
                                             .stream()
                                             .map(ssm -> ssm.getCurrentBlock())
                                             .filter(cb -> cb != null)
                                             .map(cb -> cb.height())
                                             .max((a, b) -> a.compareTo(b))
                                             .get();
                return members.stream()
                              .map(m -> updaters.get(m))
                              .map(ssm -> ssm.getCurrentBlock())
                              .filter(cb -> cb != null)
                              .map(cb -> cb.height())
                              .filter(l -> l.compareTo(target) == 0)
                              .count() == members.size();
            }), "members did not stabilize at same block: " + updaters.values()
                                                                      .stream()
                                                                      .map(ssm -> ssm.getCurrentBlock())
                                                                      .filter(cb -> cb != null)
                                                                      .map(cb -> cb.height())
                                                                      .toList());
        } finally {
            System.out.println("Final block height: " + members.stream()
                                                               .map(m -> updaters.get(m))
                                                               .map(ssm -> ssm.getCurrentBlock())
                                                               .filter(cb -> cb != null)
                                                               .map(cb -> cb.height())
                                                               .toList());
        }

        choams.values().forEach(e -> e.stop());
        routers.values().forEach(e -> e.close());

        record row(float price, int quantity) {}

        System.out.println("Validating consistency");

        Map<Member, Map<Integer, row>> manifested = new HashMap<>();

        for (Member m : members) {
            Connection connection = updaters.get(m).newConnection();
            Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery("select ID, PRICE, QTY from books");
            while (results.next()) {
                manifested.computeIfAbsent(m, k -> new HashMap<>())
                          .put(results.getInt("ID"), new row(results.getFloat("PRICE"), results.getInt("QTY")));
            }
            connection.close();
        }

        Map<Integer, row> standard = manifested.get(members.get(0));
        for (Member m : members) {
            var candidate = manifested.get(m);
            for (var entry : standard.entrySet()) {
                assertTrue(candidate.containsKey(entry.getKey()));
                assertEquals(entry.getValue(), candidate.get(entry.getKey()));
            }
        }
    }

    private CHOAM createCHOAM(Random entropy, Builder params, SigningMember m, Context<Member> context,
                              ChoamMetrics metrics) {
        String url = String.format("jdbc:h2:mem:test_engine-%s-%s", m.getId(), entropy.nextLong());
        System.out.println("DB URL: " + url);
        SqlStateMachine up = new SqlStateMachine(url, new Properties(),
                                                 new File(checkpointDirBase, m.getId().toString()));
        updaters.put(m, up);

        params.getProducer().ethereal().setSigner(m);
        var ei = new AtomicInteger();
        return new CHOAM(params.build(RuntimeParameters.newBuilder()
                                                       .setContext(context)
                                                       .setGenesisData(view -> GENESIS_DATA)
                                                       .setScheduler(Executors.newScheduledThreadPool(1,
                                                                                                      r -> new Thread(r,
                                                                                                                      "Sched["
                                                                                                                      + m.getId()
                                                                                                                      + "]")))
                                                       .setMember(m)
                                                       .setCommunications(routers.get(m.getId()))
                                                       .setExec(Executors.newFixedThreadPool(2,
                                                                                             r -> new Thread(r, "Exec["
                                                                                             + m.getId() + ":"
                                                                                             + ei.incrementAndGet()
                                                                                             + "]")))
                                                       .setCheckpointer(up.getCheckpointer())
                                                       .setMetrics(metrics)
                                                       .setProcessor(new TransactionExecutor() {

                                                           @Override
                                                           public void beginBlock(ULong height, Digest hash) {
                                                               up.getExecutor().beginBlock(height, hash);
                                                           }

                                                           @Override
                                                           public void endBlock(ULong height, Digest hash) {
                                                               up.getExecutor().endBlock(height, hash);
                                                           }

                                                           @Override
                                                           public void execute(int i, Digest hash, Transaction tx,
                                                                               @SuppressWarnings("rawtypes") CompletableFuture onComplete) {
                                                               up.getExecutor().execute(i, hash, tx, onComplete);
                                                           }

                                                           @Override
                                                           public void genesis(Digest hash,
                                                                               List<Transaction> initialization) {
                                                               up.getExecutor().genesis(hash, initialization);
                                                           }
                                                       })
                                                       .build()));
    }

    private Txn update(Random entropy, Mutator mutator) {

        List<List<Object>> batch = new ArrayList<>();
        for (int rep = 0; rep < 10; rep++) {
            for (int id = 1; id < 6; id++) {
                batch.add(Arrays.asList(entropy.nextInt(10), 1000 + id));
            }
        }
        return Txn.newBuilder().setBatchUpdate(mutator.batchOf("update books set qty = ? where id = ?", batch)).build();
    }
}
