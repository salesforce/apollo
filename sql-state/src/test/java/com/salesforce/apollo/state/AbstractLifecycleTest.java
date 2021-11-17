/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import static com.salesforce.apollo.state.Mutator.batch;
import static com.salesforce.apollo.state.Mutator.batchOf;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.h2.mvstore.MVStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.state.proto.Txn;
import com.salesforce.apollo.choam.CHOAM;
import com.salesforce.apollo.choam.CHOAM.TransactionExecutor;
import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Session;
import com.salesforce.apollo.choam.Parameters.Builder;
import com.salesforce.apollo.choam.Parameters.ProducerParameters;
import com.salesforce.apollo.choam.support.InvalidTransaction;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
abstract public class AbstractLifecycleTest {
    class Transactioneer {
        private final static Random entropy = new Random();

        private final AtomicInteger            completed = new AtomicInteger();
        private final CountDownLatch           countdown;
        private final AtomicInteger            failed    = new AtomicInteger();
        private final Timer                    latency;
        private final AtomicInteger            lineTotal;
        private final int                      max;
        private final AtomicBoolean            proceed;
        private final ScheduledExecutorService scheduler;
        private final Session                  session;
        private final Duration                 timeout;
        private final Counter                  timeouts;

        public Transactioneer(CHOAM c, Duration timeout, Counter timeouts, Timer latency, AtomicBoolean proceed,
                              AtomicInteger lineTotal, int max, CountDownLatch countdown,
                              ScheduledExecutorService txScheduler) {
            this.latency = latency;
            this.proceed = proceed;
            this.session = c.getSession();
            this.timeout = timeout;
            this.lineTotal = lineTotal;
            this.timeouts = timeouts;
            this.max = max;
            this.countdown = countdown;
            this.scheduler = txScheduler;
        }

        void decorate(CompletableFuture<?> fs, Timer.Context time) {
            fs.whenCompleteAsync((o, t) -> {
                if (!proceed.get()) {
                    return;
                }

                if (t != null) {
                    timeouts.inc();
                    var tc = latency.time();
                    failed.incrementAndGet();

                    if (completed.get() < max) {
                        scheduler.schedule(() -> {
                            try {
                                decorate(session.submit(update(entropy), timeout), tc);
                            } catch (InvalidTransaction e) {
                                e.printStackTrace();
                            }
                        }, entropy.nextInt(10), TimeUnit.MILLISECONDS);
                    }
                } else {
                    time.close();
                    final int tot = lineTotal.incrementAndGet();
                    if (tot % 100 == 0) {
                        System.out.println(".");
                    } else {
                        System.out.print(".");
                    }
                    var tc = latency.time();
                    final var complete = completed.incrementAndGet();
                    if (complete < max) {
                        scheduler.schedule(() -> {
                            try {
                                decorate(session.submit(update(entropy), timeout), tc);
                            } catch (InvalidTransaction e) {
                                e.printStackTrace();
                            }
                        }, entropy.nextInt(10), TimeUnit.MILLISECONDS);
                    } else if (complete == max) {
                        countdown.countDown();
                    }
                }
            });
        }

        void start() {
            scheduler.schedule(() -> {
                Timer.Context time = latency.time();
                try {
                    decorate(session.submit(update(entropy), timeout), time);
                } catch (InvalidTransaction e) {
                    throw new IllegalStateException(e);
                }
            }, entropy.nextInt(2000), TimeUnit.MILLISECONDS);
        }
    }

    protected static final int                   CARDINALITY     = 5;
    private static final List<Transaction>       GENESIS_DATA    = CHOAM.toGenesisData(Collections.singletonList(Txn.newBuilder()
                                                                                                                    .setBatch(batch("create table books (id int, title varchar(50), author varchar(50), price float, qty int,  primary key (id))"))
                                                                                                                    .build()));
    private static final Digest                  GENESIS_VIEW_ID = DigestAlgorithm.DEFAULT.digest("Give me food or give me slack or kill me".getBytes());

    protected Map<Digest, List<Digest>>          blocks;
    protected CompletableFuture<Boolean>         checkpointOccurred;
    protected Map<Digest, CHOAM>                 choams;
    protected List<SigningMember>                members;
    protected Map<Digest, Router>                routers;
    protected final Map<Member, SqlStateMachine> updaters        = new HashMap<>();

    private File                                 baseDir;
    private File                                 checkpointDirBase;
    private final Map<Member, Parameters>        parameters      = new HashMap<>();
    private ScheduledExecutorService             scheduler;

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
    public void before() {
        checkpointOccurred = new CompletableFuture<>();
        checkpointDirBase = new File("target/ct-chkpoints");
        Utils.clean(checkpointDirBase);
        baseDir = new File(System.getProperty("user.dir"), "target/cluster");
        Utils.clean(baseDir);
        baseDir.mkdirs();
        blocks = new ConcurrentHashMap<>();
        Random entropy = new Random();
        var context = new Context<>(DigestAlgorithm.DEFAULT.getOrigin().prefix(entropy.nextLong()), 0.2, CARDINALITY,
                                    3);
        scheduler = Executors.newScheduledThreadPool(3 * CARDINALITY);

        AtomicInteger sd = new AtomicInteger();
        Executor submitDispatcher = Executors.newFixedThreadPool(CARDINALITY, r -> {
            Thread thread = new Thread(r, "Submit Dispatcher [" + sd.getAndIncrement() + "]");
            thread.setDaemon(true);
            return thread;
        });
        AtomicInteger d = new AtomicInteger();
        Executor dispatcher = Executors.newCachedThreadPool(r -> {
            Thread thread = new Thread(r, "Dispatcher [" + d.getAndIncrement() + "]");
            thread.setDaemon(true);
            return thread;
        });
        AtomicInteger exec = new AtomicInteger();
        Executor routerExec = Executors.newCachedThreadPool(r -> {
            Thread thread = new Thread(r, "Router exec [" + exec.getAndIncrement() + "]");
            thread.setDaemon(true);
            return thread;
        });

        var params = parameters(context, submitDispatcher, dispatcher);

        members = IntStream.range(0, CARDINALITY).mapToObj(i -> Utils.getMember(i))
                           .map(cpk -> new SigningMemberImpl(cpk)).map(e -> (SigningMember) e)
                           .peek(m -> context.activate(m)).toList();
        routers = members.stream()
                         .collect(Collectors.toMap(m -> m.getId(),
                                                   m -> new LocalRouter(m,
                                                                        ServerConnectionCache.newBuilder()
                                                                                             .setTarget(CARDINALITY)
                                                                                             .setMetrics(params.getMetrics()),
                                                                        routerExec)));
        choams = members.stream().collect(Collectors.toMap(m -> m.getId(), m -> createChoam(entropy, params, m)));
    }

    protected Txn initialInsert() {
        return Txn.newBuilder()
                  .setBatch(batch("insert into books values (1001, 'Java for dummies', 'Tan Ah Teck', 11.11, 11)",
                                  "insert into books values (1002, 'More Java for dummies', 'Tan Ah Teck', 22.22, 22)",
                                  "insert into books values (1003, 'More Java for more dummies', 'Mohammad Ali', 33.33, 33)",
                                  "insert into books values (1004, 'A Cup of Java', 'Kumar', 44.44, 44)",
                                  "insert into books values (1005, 'A Teaspoon of Java', 'Kevin Jones', 55.55, 55)"))
                  .build();
    }

    protected Txn update(Random entropy) {

        List<List<Object>> batch = new ArrayList<>();
        for (int rep = 0; rep < 10; rep++) {
            for (int id = 1; id < 6; id++) {
                batch.add(Arrays.asList(entropy.nextInt(), 1000 + id));
            }
        }
        return Txn.newBuilder().setBatchUpdate(batchOf("update books set qty = ? where id = ?", batch)).build();
    }

    private CHOAM createChoam(Random entropy, Builder params, SigningMember m) {
        blocks.put(m.getId(), new CopyOnWriteArrayList<Digest>());
        String url = String.format("jdbc:h2:mem:test_engine-%s-%s", m.getId(), entropy.nextLong());
        System.out.println("DB URL: " + url);
        SqlStateMachine up = new SqlStateMachine(url, new Properties(),
                                                 new File(checkpointDirBase, m.getId().toString()));
        updaters.put(m, up);

        params.getProducer().ethereal().setSigner(m);
        return new CHOAM(params.setMember(m).setCommunications(routers.get(m.getId())).setCheckpointer(wrap(up))
                               .setRestorer(up.getBootstrapper()).setProcessor(wrap(m, up)).build(),
                         MVStore.open(null));
    }

    private Builder parameters(Context<Member> context, Executor submitDispatcher, Executor dispatcher) {
        var params = Parameters.newBuilder().setContext(context).setSynchronizationCycles(1)
                               .setGenesisViewId(GENESIS_VIEW_ID).setGenesisData(GENESIS_DATA)
                               .setGossipDuration(Duration.ofMillis(10)).setScheduler(scheduler)
                               .setSubmitDispatcher(submitDispatcher).setDispatcher(dispatcher)
                               .setProducer(ProducerParameters.newBuilder().setGossipDuration(Duration.ofMillis(10))
                                                              .setBatchInterval(Duration.ofMillis(150))
                                                              .setMaxBatchByteSize(1024 * 1024).setMaxBatchCount(10000)
                                                              .build())
                               .setTxnPermits(10_000).setCheckpointBlockSize(2);
        params.getClientBackoff().setBase(20).setCap(150).setInfiniteAttempts().setJitter()
              .setExceptionHandler(t -> System.out.println(t.getClass().getSimpleName()));
        return params;
    }

    private TransactionExecutor wrap(SigningMember m, SqlStateMachine up) {
        return new TransactionExecutor() {
            @Override
            public void beginBlock(long height, Digest hash) {
                blocks.get(m.getId()).add(hash);
                up.getExecutor().beginBlock(height, hash);
            }

            @Override
            public void execute(Transaction tx, @SuppressWarnings("rawtypes") CompletableFuture onComplete) {
                up.getExecutor().execute(tx, onComplete);
            }

            @Override
            public void genesis(List<Transaction> initialization) {
                up.getExecutor().genesis(initialization);
            }
        };
    }

    private Function<Long, File> wrap(SqlStateMachine up) {
        final var checkpointer = up.getCheckpointer();
        return l -> {
            final var check = checkpointer.apply(l);
            checkpointOccurred.complete(true);
            return check;
        };
    }

}
