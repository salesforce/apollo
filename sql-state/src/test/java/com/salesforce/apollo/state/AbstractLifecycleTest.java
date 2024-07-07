/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import com.salesforce.apollo.archipelago.LocalServer;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.ServerConnectionCache;
import com.salesforce.apollo.archipelago.UnsafeExecutors;
import com.salesforce.apollo.choam.CHOAM;
import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Parameters.BootstrapParameters;
import com.salesforce.apollo.choam.Parameters.Builder;
import com.salesforce.apollo.choam.Parameters.ProducerParameters;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.choam.proto.Transaction;
import com.salesforce.apollo.context.Context;
import com.salesforce.apollo.context.DynamicContextImpl;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.state.proto.Txn;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.Utils;
import org.joou.ULong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.salesforce.apollo.state.Mutator.batch;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author hal.hildebrand
 */
abstract public class AbstractLifecycleTest {
    protected static final int                          CARDINALITY      = 5;
    private static final   Digest                       GENESIS_VIEW_ID  = DigestAlgorithm.DEFAULT.digest(
    "Give me food or give me slack or kill me".getBytes());
    protected final        AtomicReference<ULong>       checkpointHeight = new AtomicReference<>();
    protected final        Map<Member, SqlStateMachine> updaters         = new HashMap<>();
    //    static {
    //        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Session.class)).setLevel(Level.TRACE);
    //        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(CHOAM.class)).setLevel(Level.TRACE);
    //        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(GenesisAssembly.class)).setLevel(Level.TRACE);
    //        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ViewAssembly.class)).setLevel(Level.TRACE);
    //        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Producer.class)).setLevel(Level.TRACE);
    //        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Committee.class)).setLevel(Level.TRACE);
    //        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Fsm.class)).setLevel(Level.TRACE);
    //        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(TxDataSource.class)).setLevel(Level.TRACE);
    //    }
    private final          List<Transaction>            GENESIS_DATA;
    private final          Map<Member, Parameters>      parameters       = new HashMap<>();
    protected              ExecutorService              executor;
    protected              SecureRandom                 entropy;
    protected              CountDownLatch               checkpointOccurred;
    protected              Map<Digest, CHOAM>           choams;
    protected              List<SigningMember>          members;
    protected              Map<Digest, Router>          routers;
    protected              SigningMember                testSubject;
    protected              int                          toleranceLevel;
    DynamicContextImpl<Member> context;
    private File                     baseDir;
    private File                     checkpointDirBase;
    private List<Transactioneer>     transactioneers;
    private ScheduledExecutorService scheduler;

    {
        var txns = MigrationTest.initializeBookSchema();
        txns.add(initialInsert());
        GENESIS_DATA = CHOAM.toGenesisData(txns);
    }

    public AbstractLifecycleTest() {
        super();
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

    @AfterEach
    public void after() throws Exception {
        if (routers != null) {
            routers.values().forEach(e -> e.close(Duration.ofSeconds(0)));
            routers = null;
        }
        if (choams != null) {
            choams.values().forEach(e -> e.stop());
            choams = null;
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
        if (executor != null) {
            executor.shutdown();
        }
        updaters.values().forEach(up -> up.close());
        updaters.clear();
        parameters.clear();
        members = null;
        transactioneers = null;
        context = null;
    }

    @BeforeEach
    public void before() throws Exception {
        scheduler = Executors.newScheduledThreadPool(10, Thread.ofVirtual().factory());
        executor = UnsafeExecutors.newVirtualThreadPerTaskExecutor();
        checkpointOccurred = new CountDownLatch(CARDINALITY);
        checkpointDirBase = new File("target/ct-chkpoints-" + Entropy.nextBitsStreamLong());
        Utils.clean(checkpointDirBase);
        baseDir = new File(System.getProperty("user.dir"), "target/cluster-" + Entropy.nextBitsStreamLong());
        Utils.clean(baseDir);
        baseDir.mkdirs();
        entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6, disc() });
        context = new DynamicContextImpl<>(DigestAlgorithm.DEFAULT.getOrigin(), CARDINALITY, 0.2, 3);
        toleranceLevel = context.toleranceLevel();

        var params = parameters(context);
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);

        members = IntStream.range(0, CARDINALITY).mapToObj(i -> {
            return stereotomy.newIdentifier();
        }).map(ControlledIdentifierMember::new).map(e -> (SigningMember) e).collect(Collectors.toList());
        System.out.println("Members: " + members.stream().map(Member::getId).toList());
        members.forEach(m -> context.activate(m));

        testSubject = new ControlledIdentifierMember(stereotomy.newIdentifier());

        members.stream().filter(s -> s != testSubject).forEach(s -> context.activate(s));
        final var prefix = UUID.randomUUID().toString();
        routers = members.stream()
                         .collect(Collectors.toMap(Member::getId, m -> new LocalServer(prefix, m).router(
                         ServerConnectionCache.newBuilder().setTarget(30), executor)));
        routers.put(testSubject.getId(),
                    new LocalServer(prefix, testSubject).router(ServerConnectionCache.newBuilder().setTarget(30)));
        choams = members.stream()
                        .collect(Collectors.toMap(Member::getId,
                                                  m -> createChoam(entropy, params, m, m.equals(testSubject),
                                                                   context)));
        choams.put(testSubject.getId(), createChoam(entropy, params, testSubject, true, context));
        members.stream().filter(m -> !m.equals(testSubject)).forEach(m -> context.activate(m));
        System.out.println(
        "test subject: " + testSubject.getId() + "\nmembers: " + members.stream().map(Member::getId).toList());
    }

    protected abstract int checkpointBlockSize();

    abstract protected byte disc();

    protected void post() throws Exception {

        final var clearTxns = Utils.waitForCondition(30_000, 1000, () ->
        transactioneers.stream().mapToInt(t -> t.inFlight()).filter(t -> t == 0).count() == transactioneers.size());
        assertTrue(clearTxns, "Transactions did not clear: " + Collections.singletonList(
        transactioneers.stream().mapToInt(t -> t.inFlight()).filter(t -> t == 0).toArray()));

        final var synchd = Utils.waitForCondition(30_000, 1000, () -> {

            var max = members.stream()
                             .map(m -> updaters.get(m))
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
                          .filter(l -> l.compareTo(max) == 0)
                          .count() == members.size();
        });
        assertTrue(synchd, "state did not synchronize: " + members.stream()
                                                                  .map(m -> updaters.get(m))
                                                                  .map(ssm -> ssm.getCurrentBlock())
                                                                  .filter(cb -> cb != null)
                                                                  .map(cb -> cb.height())
                                                                  .toList());

        choams.values().forEach(e -> e.stop());
        routers.values().forEach(e -> e.close(Duration.ofSeconds(0)));
        final ULong target = updaters.values()
                                     .stream()
                                     .map(ssm -> ssm.getCurrentBlock())
                                     .filter(cb -> cb != null)
                                     .map(cb -> cb.height())
                                     .max((a, b) -> a.compareTo(b))
                                     .get();
        assertEquals(members.stream()
                            .map(m -> updaters.get(m))
                            .map(ssm -> ssm.getCurrentBlock())
                            .filter(cb -> cb != null)
                            .map(cb -> cb.height())
                            .filter(l -> l.compareTo(target) == 0)
                            .count(), members.size(), "members did not end at same block: " + updaters.values()
                                                                                                      .stream()
                                                                                                      .map(
                                                                                                      ssm -> ssm.getCurrentBlock())
                                                                                                      .filter(
                                                                                                      cb -> cb != null)
                                                                                                      .map(
                                                                                                      cb -> cb.height())
                                                                                                      .toList());

        System.out.println("Final state: " + members.stream()
                                                    .map(m -> updaters.get(m))
                                                    .map(ssm -> ssm.getCurrentBlock())
                                                    .filter(cb -> cb != null)
                                                    .map(cb -> cb.height())
                                                    .toList());

        System.out.println();
        System.out.println();
        System.out.println();

        record row(float price, int quantity) {
        }

        System.out.println("Checking replica consistency");

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

    protected void pre() throws Exception {

        final Duration timeout = Duration.ofSeconds(6);
        transactioneers = new ArrayList<>();
        final CountDownLatch countdown = new CountDownLatch(1);

        routers.entrySet()
               .stream()
               .filter(e -> !e.getKey().equals(testSubject.getId()))
               .map(Map.Entry::getValue)
               .forEach(Router::start);
        choams.entrySet()
              .stream()
              .filter(e -> !e.getKey().equals(testSubject.getId()))
              .map(Map.Entry::getValue)
              .forEach(CHOAM::start);

        var txneer = updaters.get(members.getLast());

        final var activated = Utils.waitForCondition(30_000, 1_000, () -> choams.entrySet()
                                                                                .stream()
                                                                                .filter(e -> !e.getKey()
                                                                                               .equals(
                                                                                               testSubject.getId()))
                                                                                .map(Map.Entry::getValue)
                                                                                .allMatch(CHOAM::active));
        assertTrue(activated, "Group did not become active: " + (choams.entrySet()
                                                                       .stream()
                                                                       .filter(
                                                                       e -> !e.getKey().equals(testSubject.getId()))
                                                                       .map(Map.Entry::getValue)
                                                                       .filter(c -> !c.active())
                                                                       .map(CHOAM::logState)
                                                                       .toList()));

        var mutator = txneer.getMutator(choams.get(members.getLast().getId()).getSession());
        transactioneers.add(
        new Transactioneer(scheduler, () -> update(entropy, mutator), mutator, timeout, 1, countdown));
        System.out.println("Transaction member: " + members.getLast().getId());
        System.out.println("Starting txns");
        transactioneers.forEach(Transactioneer::start);
        var success = countdown.await(60, TimeUnit.SECONDS);
        assertTrue(success, "Did not complete transactions: " + (transactioneers.stream()
                                                                                .mapToInt(Transactioneer::completed)
                                                                                .sum()));

    }

    protected Txn update(Random entropy, Mutator mutator) {

        List<List<Object>> batch = new ArrayList<>();
        for (int rep = 0; rep < 10; rep++) {
            for (int id = 1; id < 6; id++) {
                batch.add(Arrays.asList(entropy.nextInt(), 1000 + id));
            }
        }
        return Txn.newBuilder().setBatchUpdate(mutator.batchOf("update books set qty = ? where id = ?", batch)).build();
    }

    private CHOAM createChoam(Random entropy, Builder params, SigningMember m, boolean testSubject,
                              Context<Member> context) {
        String url = String.format("jdbc:h2:mem:test_engine-%s-%s", m.getId(), entropy.nextLong());
        System.out.println("DB URL: " + url);
        SqlStateMachine up = new SqlStateMachine(m.getId(), url, new Properties(),
                                                 new File(checkpointDirBase, m.getId().toString()));
        updaters.put(m, up);
        if (testSubject) {
            params.setGenerateGenesis(false);
        }
        params.getProducer().ethereal().setSigner(m);
        return new CHOAM(params.setSynchronizationCycles(testSubject ? 100 : 10)
                               .build(RuntimeParameters.newBuilder()
                                                       .setContext(context)
                                                       .setGenesisData(view -> GENESIS_DATA)
                                                       .setMember(m)
                                                       .setCommunications(routers.get(m.getId()))
                                                       .setCheckpointer(wrap(up))
                                                       .setRestorer(up.getBootstrapper())
                                                       .setProcessor(up.getExecutor())
                                                       .build()));
    }

    private Builder parameters(Context<Member> context) {
        var params = Parameters.newBuilder()
                               .setGenerateGenesis(true)
                               .setGenesisViewId(GENESIS_VIEW_ID)
                               .setBootstrap(BootstrapParameters.newBuilder()
                                                                .setGossipDuration(Duration.ofMillis(10))
                                                                .setMaxSyncBlocks(1000)
                                                                .setMaxViewBlocks(1000)
                                                                .build())
                               .setProducer(ProducerParameters.newBuilder()
                                                              .setGossipDuration(Duration.ofMillis(10))
                                                              .setBatchInterval(Duration.ofMillis(10))
                                                              .setMaxBatchByteSize(1024 * 1024)
                                                              .setMaxBatchCount(3000)
                                                              .setEthereal(Config.newBuilder()
                                                                                 .setEpochLength(7)
                                                                                 .setNumberOfEpochs(3))
                                                              .build())
                               .setGossipDuration(Duration.ofMillis(10))
                               .setCheckpointBlockDelta(checkpointBlockSize())
                               .setCheckpointSegmentSize(128);

        params.getProducer().ethereal().setNumberOfEpochs(2).setEpochLength(20);

        return params;
    }

    private Function<ULong, File> wrap(SqlStateMachine up) {
        final var checkpointer = up.getCheckpointer();
        return l -> {
            try {
                final var check = checkpointer.apply(l);
                checkpointHeight.compareAndSet(null, l.add(1));
                checkpointOccurred.countDown();
                return check;
            } catch (Throwable e) {
                e.printStackTrace();
                throw e;
            }
        };
    }

}
