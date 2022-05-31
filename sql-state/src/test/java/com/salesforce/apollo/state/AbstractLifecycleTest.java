/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import static com.salesforce.apollo.state.Mutator.batch;

import java.io.File;
import java.security.SecureRandom;
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
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.joou.ULong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.state.proto.Txn;
import com.salesforce.apollo.choam.CHOAM;
import com.salesforce.apollo.choam.CHOAM.TransactionExecutor;
import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Parameters.BootstrapParameters;
import com.salesforce.apollo.choam.Parameters.Builder;
import com.salesforce.apollo.choam.Parameters.ProducerParameters;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
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
abstract public class AbstractLifecycleTest {
    protected static final int                      CARDINALITY = 5;
    protected static final Random                   entropy     = new Random();
    protected static final Executor                 txExecutor  = Executors.newFixedThreadPool(CARDINALITY);
    protected static final ScheduledExecutorService txScheduler = Executors.newScheduledThreadPool(CARDINALITY);

    private static final List<Transaction> GENESIS_DATA;
    private static final Digest            GENESIS_VIEW_ID = DigestAlgorithm.DEFAULT.digest("Give me food or give me slack or kill me".getBytes());

    static {
        var txns = MigrationTest.initializeBookSchema();
        txns.add(initialInsert());
        GENESIS_DATA = CHOAM.toGenesisData(txns);
//        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Session.class)).setLevel(Level.TRACE);
//        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(CHOAM.class)).setLevel(Level.TRACE);
//        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(GenesisAssembly.class)).setLevel(Level.TRACE);
//        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ViewAssembly.class)).setLevel(Level.TRACE);
//        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Producer.class)).setLevel(Level.TRACE);
//        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Committee.class)).setLevel(Level.TRACE);
//        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Fsm.class)).setLevel(Level.TRACE);
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

    protected Map<Digest, AtomicInteger>         blocks;
    protected final AtomicReference<ULong>       checkpointHeight = new AtomicReference<>();
    protected CountDownLatch                     checkpointOccurred;
    protected Map<Digest, CHOAM>                 choams;
    protected List<SigningMember>                members;
    protected Map<Digest, Router>                routers;
    protected SigningMember                      testSubject;
    protected int                                toleranceLevel;
    protected final Map<Member, SqlStateMachine> updaters         = new HashMap<>();
    private File                                 baseDir;
    private File                                 checkpointDirBase;
    private final Map<Member, Parameters>        parameters       = new HashMap<>();

    public AbstractLifecycleTest() {
        super();
    }

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
        parameters.clear();
        members = null;
    }

    @BeforeEach
    public void before() throws Exception {
        checkpointOccurred = new CountDownLatch(CARDINALITY - 1);
        checkpointDirBase = new File("target/ct-chkpoints-" + Entropy.nextBitsStreamLong());
        Utils.clean(checkpointDirBase);
        baseDir = new File(System.getProperty("user.dir"), "target/cluster-" + Entropy.nextBitsStreamLong());
        Utils.clean(baseDir);
        baseDir.mkdirs();
        blocks = new ConcurrentHashMap<>();
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var context = new ContextImpl<>(DigestAlgorithm.DEFAULT.getOrigin(), CARDINALITY, 0.2, 3);
        toleranceLevel = context.toleranceLevel();

        var params = parameters(context);
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);

        members = IntStream.range(0, CARDINALITY)
                           .mapToObj(i -> stereotomy.newIdentifier().get())
                           .map(cpk -> new ControlledIdentifierMember(cpk))
                           .map(e -> (SigningMember) e)
                           .toList();
        members.forEach(m -> context.activate(m));
        testSubject = members.get(CARDINALITY - 1);
        members.stream().filter(s -> s != testSubject).forEach(s -> context.activate(s));
        final var prefix = UUID.randomUUID().toString();
        routers = members.stream().collect(Collectors.toMap(m -> m.getId(), m -> {
            var localRouter = new LocalRouter(prefix, ServerConnectionCache.newBuilder().setTarget(30),
                                              Executors.newFixedThreadPool(2), null);
            localRouter.setMember(m);
            return localRouter;
        }));
        choams = members.stream()
                        .collect(Collectors.toMap(m -> m.getId(), m -> createChoam(entropy, params, m,
                                                                                   m.equals(testSubject), context)));
        members.stream().filter(m -> !m.equals(testSubject)).forEach(m -> context.activate(m));
        System.out.println("test subject: " + testSubject.getId() + "\nmembers: "
        + members.stream().map(e -> e.getId()).toList());
    }

    protected abstract int checkpointBlockSize();

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
        blocks.put(m.getId(), new AtomicInteger());
        String url = String.format("jdbc:h2:mem:test_engine-%s-%s", m.getId(), entropy.nextLong());
        System.out.println("DB URL: " + url);
        SqlStateMachine up = new SqlStateMachine(url, new Properties(),
                                                 new File(checkpointDirBase, m.getId().toString()));
        updaters.put(m, up);

        params.getProducer().ethereal().setSigner(m);
        return new CHOAM(params.setSynchronizationCycles(testSubject ? 100 : 10)
                               .build(RuntimeParameters.newBuilder()
                                                       .setContext(context)
                                                       .setExec(Executors.newFixedThreadPool(2))
                                                       .setGenesisData(view -> GENESIS_DATA)
                                                       .setScheduler(Executors.newSingleThreadScheduledExecutor())
                                                       .setMember(m)
                                                       .setCommunications(routers.get(m.getId()))
                                                       .setCheckpointer(wrap(up))
                                                       .setRestorer(up.getBootstrapper())
                                                       .setProcessor(wrap(m, up))
                                                       .build()));
    }

    private Builder parameters(Context<Member> context) {
        var params = Parameters.newBuilder()
                               .setGenesisViewId(GENESIS_VIEW_ID)
                               .setSynchronizeTimeout(Duration.ofSeconds(1))
                               .setBootstrap(BootstrapParameters.newBuilder()
                                                                .setGossipDuration(Duration.ofMillis(10))
                                                                .setMaxSyncBlocks(1000)
                                                                .setMaxViewBlocks(1000)
                                                                .build())
                               .setSynchronizeDuration(Duration.ofMillis(10))
                               .setProducer(ProducerParameters.newBuilder()
                                                              .setGossipDuration(Duration.ofMillis(10))
                                                              .setBatchInterval(Duration.ofMillis(100))
                                                              .setMaxBatchByteSize(1024 * 1024)
                                                              .setMaxBatchCount(3000)
                                                              .build())
                               .setGossipDuration(Duration.ofMillis(10))
                               .setCheckpointBlockDelta(checkpointBlockSize())
                               .setCheckpointSegmentSize(128);

        params.getProducer().ethereal().setEpochLength(10);
        return params;
    }

    private TransactionExecutor wrap(SigningMember m, SqlStateMachine up) {
        return new TransactionExecutor() {
            @Override
            public void beginBlock(ULong height, Digest hash) {
                blocks.get(m.getId()).incrementAndGet();
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
            public void genesis(Digest hash, List<Transaction> initialization) {
                blocks.get(m.getId()).incrementAndGet();
                up.getExecutor().genesis(hash, initialization);
            }
        };
    }

    private Function<ULong, File> wrap(SqlStateMachine up) {
        final var checkpointer = up.getCheckpointer();
        return l -> {
            try {
                final var check = checkpointer.apply(l);
                checkpointOccurred.countDown();
                checkpointHeight.compareAndSet(null, l.add(1));
                return check;
            } catch (Throwable e) {
                e.printStackTrace();
                throw e;
            }
        };
    }

}
