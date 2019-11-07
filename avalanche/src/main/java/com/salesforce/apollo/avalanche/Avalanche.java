/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche;

import static com.salesforce.apollo.dagwood.schema.Tables.PROCESSORS;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.avro.AvroRemoteException;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer.Context;
import com.salesforce.apollo.avalanche.WorkingSet.FinalizationData;
import com.salesforce.apollo.avalanche.communications.AvalancheClientCommunications;
import com.salesforce.apollo.avalanche.communications.AvalancheCommunications;
import com.salesforce.apollo.avro.DagEntry;
import com.salesforce.apollo.avro.HASH;
import com.salesforce.apollo.avro.QueryResult;
import com.salesforce.apollo.avro.Vote;
import com.salesforce.apollo.fireflies.Member;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.fireflies.RandomMemberGenerator;
import com.salesforce.apollo.fireflies.View;
import com.salesforce.apollo.protocols.HashKey;

import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;

/**
 * Implementation of the Avalanche consensus protocol.
 * 
 * @author hal.hildebrand
 * @since 220
 */
public class Avalanche {

    public static class PendingTransaction {
        public final CompletableFuture<HashKey> pending;
        public final ScheduledFuture<?>         timer;

        public PendingTransaction(CompletableFuture<HashKey> pending, ScheduledFuture<?> timer) {
            this.pending = pending;
            this.timer = timer;
        }

        public void complete(HashKey key) {
            log.trace("Finalizing transaction: {}", key);
            timer.cancel(true);
            if (pending != null) {
                pending.complete(key);
            }
        }

    }

    public class Service {

        public QueryResult onQuery(List<ByteBuffer> transactions, List<HASH> wanted) {
            if (!running.get()) {
                ArrayList<Vote> results = new ArrayList<>();
                for (int i = 0; i < transactions.size(); i++) {
                    results.add(Vote.UNKNOWN);
                    if (metrics != null) {
                        metrics.getInboundQueryUnknownRate().mark();
                    }
                }
                return new QueryResult(results, Collections.emptyList());
            }
            long now = System.currentTimeMillis();
            Context timer = metrics == null ? null : metrics.getInboundQueryTimer().time();

            final List<HashKey> inserted = dag.insertSerialized(transactions, System.currentTimeMillis());
            List<Boolean> stronglyPreferred = dag.isStronglyPreferred(inserted);
            log.trace("onquery {} txn in {} ms", stronglyPreferred, System.currentTimeMillis() - now);
            List<Vote> queried = stronglyPreferred.stream().map(r -> {
                if (r == null) {
                    if (metrics != null) {
                        metrics.getInboundQueryUnknownRate().mark();
                    }
                    return Vote.UNKNOWN;
                } else if (r) {
                    return Vote.TRUE;
                } else {
                    return Vote.FALSE;
                }
            }).collect(Collectors.toList());

            if (timer != null) {
                timer.close();
                metrics.getInboundQueryRate().mark(transactions.size());
            }
            assert queried.size() == transactions.size() : "on query results " + queried.size() + " != "
                    + transactions.size();

            return new QueryResult(queried,
                    dag.getQuerySerializedEntries(wanted.stream()
                                                        .map(e -> new HashKey(e))
                                                        .collect(Collectors.toList())));
        }

        public List<ByteBuffer> requestDAG(List<HASH> want) {
            if (!running.get()) {
                return new ArrayList<>();
            }
            return dag.getEntries(want.stream().map(e -> new HashKey(e)).collect(Collectors.toList()));
        }
    }

    private static final int    AVALANCHE_TXN_FLOOD_CHANNEL = 2;
    private static final String DAG_SCHEMA_YML              = "dag-schema.yml";

    private final static Logger log = LoggerFactory.getLogger(Avalanche.class);

    private static final String PASSWORD  = "";
    private static final String USER_NAME = "Apollo";

    public static void loadSchema(String dbConnect) {
        Connection connection;
        try {
            connection = DriverManager.getConnection(dbConnect, "apollo", PASSWORD);
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot get DB Connection: " + dbConnect, e);
        }
        Database database;
        try {
            database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
        } catch (DatabaseException e) {
            throw new IllegalStateException("Unable to get DB factory for: " + dbConnect, e);
        }
        database.setLiquibaseSchemaName("public");
        Liquibase liquibase = new Liquibase(DAG_SCHEMA_YML,
                new ClassLoaderResourceAccessor(Avalanche.class.getClassLoader()), database);
        try {
            liquibase.update((Contexts) null);
        } catch (LiquibaseException e) {
            throw new IllegalStateException("Cannot load schema", e);
        }

        try {
            connection.commit();
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot create trigger", e);
        }
    }

    private final AvalancheCommunications                    comm;
    private final WorkingSet                                 dag;
    private final ExecutorService                            finalizer;
    @SuppressWarnings("unused")
    private final RingBuffer<HashKey>                        frontier;
    private final AtomicBoolean                              generateNoOps       = new AtomicBoolean();
    private final int                                        invalidThreshold;
    private final AvaMetrics                                 metrics;
    private final Deque<HashKey>                             noOpParentSample    = new LinkedBlockingDeque<>();
    private final AvalancheParameters                        parameters;
    private final Deque<HashKey>                             parentSample        = new LinkedBlockingDeque<>();
    private final ConcurrentMap<HashKey, PendingTransaction> pendingTransactions = new ConcurrentSkipListMap<>();
    private final Map<HashKey, EntryProcessor>               processors          = new ConcurrentSkipListMap<>();
    private final int                                        required;
    private final AtomicInteger                              round               = new AtomicInteger();
    private final AtomicBoolean                              running             = new AtomicBoolean();
    private final RandomMemberGenerator                      sampler;
    private final Service                                    service             = new Service();
    private final View                                       view;

    public Avalanche(View view, AvalancheCommunications communications, AvalancheParameters p) {
        this(view, communications, p, null, null);
    }

    public Avalanche(View view, AvalancheCommunications communications, AvalancheParameters p, AvaMetrics metrics) {
        this(view, communications, p, metrics, null);
    }

    public Avalanche(View view, AvalancheCommunications communications, AvalancheParameters p, AvaMetrics metrics,
            ClassLoader resolver) {
        this.metrics = metrics;
        parameters = p;
        this.view = view;
        this.comm = communications;
        comm.initialize(this);
        loadSchema(parameters.dbConnect);
        this.dag = new WorkingSet(parameters, new DagWood(parameters.dagWood), metrics);

        view.registerRoundListener(() -> {
            round.incrementAndGet();
            if (round.get() % parameters.delta == 0) {
                generateNoOps.set(true);
            }
        });

        view.register(AVALANCHE_TXN_FLOOD_CHANNEL, txns -> {
            dag.insertSerializedRaw(txns.stream().map(e -> e.content).collect(Collectors.toList()),
                                    System.currentTimeMillis());
        });

        sampler = new RandomMemberGenerator(view);
        required = (int) (parameters.k * parameters.alpha);
        invalidThreshold = parameters.k - required - 1;

        ClassLoader loader = resolver;
        if (resolver == null) {
            loader = Thread.currentThread().getContextClassLoader();
            if (loader == null) {
                loader = getClass().getClassLoader();
            }
        }

        initializeProcessors(loader);
        AtomicInteger i = new AtomicInteger();
        finalizer = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "Finalizer[" + getNode().getId() + "] : " + i.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
        frontier = new RingBuffer<>(parameters.frontier);
    }

    /**
     * Create the genesis block for this view
     * 
     * @param data    - the genesis transaction content
     * @param timeout -how long to wait for finalization of the transaction
     * @return a CompleteableFuture indicating whether the transaction is finalized
     *         or not, or whether an exception occurred that prevented processing.
     *         The returned HashKey is the hash key of the the finalized genesis
     *         transaction in the DAG
     */
    public CompletableFuture<HashKey> createGenesis(byte[] data, Duration timeout, ScheduledExecutorService scheduler) {
        if (!running.get()) {
            throw new IllegalStateException("Service is not running");
        }
        CompletableFuture<HashKey> futureSailor = new CompletableFuture<>();
        DagEntry dagEntry = new DagEntry();
        dagEntry.setDescription(WellKnownDescriptions.GENESIS.toHash());
        dagEntry.setData(ByteBuffer.wrap("Genesis".getBytes()));
        // genesis has no parents
        HashKey key = dag.insert(dagEntry, new HashKey(WellKnownDescriptions.GENESIS.toHash()),
                                 System.currentTimeMillis());
        pendingTransactions.put(key, new PendingTransaction(futureSailor,
                scheduler.schedule(() -> timeout(key), timeout.toMillis(), TimeUnit.MILLISECONDS)));
        flood(dagEntry);
        return futureSailor;
    }

    public WorkingSet getDag() {
        return dag;
    }

    public DagDao getDagDao() {
        return new DagDao(dag);
    }

    public Node getNode() {
        return view.getNode();
    }

    public int getRoundCounter() {
        return round.get();
    }

    public Service getService() {
        return service;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        int advance = getEntropy().nextInt(50);
        for (int i = 0; i < advance; i++) {
            sampler.next();
        }

        comm.start();

        Thread queryThread = new Thread(() -> {
            while (running.get()) {
                round();
            }
        }, "Query[" + view.getNode().getId() + "]");
        queryThread.setDaemon(true);

        queryThread.start();
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        comm.close();
        pendingTransactions.values().clear();
    }

    /**
     * Submit a transaction to the group.
     * 
     * @param data    - the transaction content
     * @param timeout -how long to wait for finalization of the transaction
     * @param future  - optional future to be notified of finalization
     * @return the HASH of the transaction, null if invalid
     */
    public HASH submitTransaction(HASH description, byte[] data, Duration timeout, CompletableFuture<HashKey> future,
                                  ScheduledExecutorService scheduler) {
        if (!running.get()) {
            throw new IllegalStateException("Service is not running");
        }
        if (parentSample.isEmpty()) {
            sampleParents();
        }
        TreeSet<HashKey> parents = new TreeSet<>();
        while (parents.size() < parameters.parentCount) {
            HashKey parent = parentSample.poll();
            if (parent == null) {
                break;
            }
            parents.add(parent);
        }
        if (parents.isEmpty()) {
            if (future != null) {
                future.completeExceptionally(new IllegalStateException("No parents available for transaction"));
            }
            return null;
        }
        Context timer = metrics == null ? null : metrics.getSubmissionTimer().time();
        try {
            DagEntry dagEntry = new DagEntry();
            dagEntry.setDescription(description);
            dagEntry.setData(ByteBuffer.wrap(data));
            dagEntry.setLinks(parents.stream().map(e -> e.toHash()).collect(Collectors.toList()));
            HashKey key = dag.insert(dagEntry, System.currentTimeMillis());
            if (future != null) {
                pendingTransactions.put(key, new PendingTransaction(future,
                        scheduler.schedule(() -> timeout(key), timeout.toMillis(), TimeUnit.MILLISECONDS)));
            }
            if (metrics != null) {
                metrics.getSubmissionRate().mark();
                metrics.getInputRate().mark();
            }
            flood(dagEntry);
            return key.toHash();
        } finally {
            if (timer != null) {
                timer.close();
            }
        }
    }

    /**
     * Submit a transaction to the group.
     * 
     * @param data    - the transaction content
     * @param timeout -how long to wait for finalization of the transaction
     * @return a CompleteableFuture indicating whether the transaction is finalized
     *         or not, or whether an exception occurred that prevented processing.
     *         The returned HashKey is the hash key of the the finalized transaction
     *         in the DAG
     */
    public CompletableFuture<HashKey> submitTransaction(HASH description, byte[] data, Duration timeout,
                                                        ScheduledExecutorService scheduler) {
        CompletableFuture<HashKey> futureSailor = new CompletableFuture<>();
        submitTransaction(description, data, timeout, futureSailor, scheduler);
        return futureSailor;
    }

    void finalize(List<HashKey> preferings) {
        long then = System.currentTimeMillis();
        Context timer = metrics == null ? null : metrics.getFinalizeTimer().time();
        final FinalizationData finalized = dag.tryFinalize(preferings);
        if (timer != null) {
            timer.close();
        }
        if (metrics != null) {
            metrics.getFinalizerRate().mark(finalized.finalized.size());
        }
        ForkJoinPool.commonPool().execute(() -> {
            finalized.finalized.forEach(key -> {
                PendingTransaction pending = pendingTransactions.remove(key);
                if (pending != null) {
                    pending.complete(key);
                }
            });

            finalized.deleted.forEach(key -> {
                PendingTransaction pending = pendingTransactions.remove(key);
                if (pending != null) {
                    pending.complete(null);
                }
            });
        });
        log.debug("Finalizing: {}, deleting: {} in {} ms", finalized.finalized.size(), finalized.deleted.size(),
                  System.currentTimeMillis() - then);
    }

    void flood(DagEntry dagEntry) {
//        view.publish(AVALANCHE_TXN_FLOOD_CHANNEL, serialize(dagEntry));
    }

    /**
     * Periodically issue no op transactions for the neglected noOp transactions
     */
    void generateNoOpTxns() {
        log.trace("generating NoOp transactions");
        if (noOpParentSample.isEmpty()) {
            sampleNoOpParents();
        }

        for (int i = 0; i < parameters.noOpsPerRound; i++) {
            TreeSet<HashKey> parents = new TreeSet<>();
            while (parents.size() < parameters.maxNoOpParents) {
                if (noOpParentSample.peek() == null) {
                    break;
                }
                parents.add(noOpParentSample.removeFirst());
            }
            if (parents.isEmpty()) {
                return;
            }
            DagEntry dagEntry = new DagEntry();
            byte[] dummy = new byte[4];
            getEntropy().nextBytes(dummy);
            dagEntry.setData(ByteBuffer.wrap(dummy));
            dagEntry.setLinks(parents.stream().map(e -> e.toHash()).collect(Collectors.toList()));
            dag.insert(dagEntry, System.currentTimeMillis());
            if (log.isTraceEnabled()) {
                log.trace("noOp transaction {}", parents);
            }
        }
    }

    /**
     * for test access
     */
    ConcurrentMap<HashKey, PendingTransaction> getPendingTransactions() {
        return pendingTransactions;
    }

    void prefer(List<HashKey> preferings) {
        Context timer = metrics == null ? null : metrics.getPreferTimer().time();
        dag.prefer(preferings);
        if (timer != null) {
            timer.close();
        }
        if (metrics != null) {
            metrics.getPreferRate().mark(preferings.size());
        }
    }

    /**
     * Query a random sample of ye members for their votes on the next batch of
     * unqueried transactions for this node, determine confidence from the results
     * of the query results.
     */
    int query() {
        long start = System.currentTimeMillis();
        long now = System.currentTimeMillis();
        long retrieveTime = System.currentTimeMillis() - now;
        Collection<Member> sample = sampler.sample(parameters.k);
        if (sample.isEmpty()) {
            return 0;
        }
        assert sample.size() >= parameters.k : "not enough members: " + sample.size();
        now = System.currentTimeMillis();
        Context timer = metrics == null ? null : metrics.getQueryTimer().time();
        List<HashKey> unqueried = dag.query(parameters.queryBatchSize);
        if (unqueried.isEmpty()) {
            if (timer != null) {
                timer.close();
            }
            return 0;
        }
        List<ByteBuffer> query = dag.getQuerySerializedEntries(unqueried);
        List<Boolean> results = query(query, sample);
        if (timer != null) {
            timer.close();
        }
        if (metrics != null) {
            metrics.getQueryRate().mark(query.size());
        }
        long sampleTime = System.currentTimeMillis() - now;
        List<HashKey> unpreferings = new ArrayList<>();
        List<HashKey> preferings = new ArrayList<>();
        for (int i = 0; i < results.size(); i++) {
            final Boolean result = results.get(i);
            HashKey key = unqueried.get(i);
            if (result == null) {
                log.trace("Could not get a valid sample on this txn, scheduling for requery: {}", key);
                dag.queueUnqueried(key);
                if (metrics != null) {
                    metrics.getResampledRate().mark();
                }
            } else if (result) {
                preferings.add(key);
            } else {
                if (metrics != null) {
                    metrics.getFailedTxnQueryRate().mark();
                }
                unpreferings.add(key);
            }
        }
        finalizer.execute(() -> {
            if (running.get()) {
                prefer(preferings);
                finalize(preferings);
            }
        });

        if (metrics != null) {
            metrics.getFailedTxnQueryRate().mark(unpreferings.size());
        }
        if (!unpreferings.isEmpty()) {
            log.info("querying {} txns in {} ms failures: {}", unqueried.size(), System.currentTimeMillis() - start,
                     unpreferings.size());
        }
        log.trace("querying {} txns in {} ms ({} Query) ({} Sample)", unqueried.size(),
                  System.currentTimeMillis() - start, retrieveTime, sampleTime);

        getWanted();

        return results.size();
    }

    /**
     * Query the sample of members for their boolean opinion on the query of
     * transaction entries
     * 
     * @param batch  - the batch of unqueried nodes
     * @param sample - the random sample of members to query
     * @return for each sampled member, the list of query results for the
     *         transaction batch, or NULL if there could not be a determination
     *         (such as comm failure) of the query for that member
     */
    List<Boolean> query(List<ByteBuffer> batch, Collection<Member> sample) {
        long now = System.currentTimeMillis();
        AtomicInteger[] invalid = new AtomicInteger[batch.size()];
        AtomicInteger[] votes = new AtomicInteger[batch.size()];
        for (int i = 0; i < votes.length; i++) {
            invalid[i] = new AtomicInteger();
            votes[i] = new AtomicInteger();
        }

        CompletionService<Boolean> frist = new ExecutorCompletionService<>(ForkJoinPool.commonPool());
        List<Future<Boolean>> futures;
        futures = sample.stream().map(m -> frist.submit(() -> {
            QueryResult result;
            AvalancheClientCommunications connection = comm.connectToNode(m, getNode());
            if (connection == null) {
                log.info("No connection querying {} for {}", m, batch);
                for (int i = 0; i < batch.size(); i++) {
                    invalid[i].incrementAndGet();
                }
                return false;
            }
            try {
                result = connection.query(batch, Collections.emptyList());
            } catch (AvroRemoteException e) {
                for (int i = 0; i < batch.size(); i++) {
                    invalid[i].incrementAndGet();
                }
                log.info("Error querying {} for {}", m, batch, e);
                return false;
            } finally {
                connection.close();
            }
            log.trace("queried: {} for: {} result: {}", m, batch.size(), result.getResult());
            if (result.getResult().isEmpty()) {
                for (int i = 0; i < batch.size(); i++) {
                    invalid[i].incrementAndGet();
                }
                return false;
            }
            for (int i = 0; i < batch.size(); i++) {
                switch (result.getResult().get(i)) {
                case FALSE:
                    break;
                case TRUE:
                    votes[i].incrementAndGet();
                    break;
                case UNKNOWN:
                    invalid[i].incrementAndGet();
                    break;
                }
            }
            return true;
        })).collect(Collectors.toList());

        long remainingTimout = parameters.unit.toMillis(parameters.timeout);

        AtomicInteger responded = new AtomicInteger();
        try {
            while (responded.get() < futures.size() && remainingTimout > 0) {
                long then = System.currentTimeMillis();
                Future<Boolean> result = null;
                try {
                    result = frist.poll(remainingTimout, TimeUnit.MILLISECONDS);
                    if (result != null) {
                        responded.incrementAndGet();
                        try {
                            result.get();
                        } catch (ExecutionException e) {
                            if (log.isDebugEnabled()) {
                                log.debug("exception querying {}", batch.size(), e.getCause());
                            }
                        }
                    }
                } catch (InterruptedException e) {
                }
                remainingTimout = remainingTimout - (System.currentTimeMillis() - then);
            }
        } finally {
            futures.forEach(f -> f.cancel(true));
        }
        List<Boolean> queryResults = new ArrayList<>();
        for (int i = 0; i < batch.size(); i++) {
            if ((invalidThreshold <= invalid[i].get())) {
                queryResults.add(null);
            } else {
                queryResults.add(votes[i].get() >= required);
            }
        }

        log.debug("query results: {} in: {} ms", queryResults.size(), System.currentTimeMillis() - now);

        return queryResults;
    }

    void round() {
        log.trace("Performing round");
        try {
            query();
            if (generateNoOps.compareAndSet(true, false)) {
                generateNoOpTxns();
            } else {
                Thread.sleep(1);
            }
        } catch (Throwable t) {
            log.error("Error performing Avalanche batch round", t);
        }
    }

    void sampleNoOpParents() {
        Context timer = metrics == null ? null : metrics.getNoOpTimer().time();
        dag.sampleNoOpParents(noOpParentSample, getEntropy());
        if (timer != null) {
            timer.close();
        }
    }

    void sampleParents() {
        Context timer = metrics == null ? null : metrics.getParentSampleTimer().time();
        int sampleCount = 0;
        try {
            sampleCount = dag.sampleParents(parentSample, getEntropy());
        } finally {
            if (timer != null) {
                timer.close();
            }
            if (metrics != null) {
                metrics.getParentSampleRate().mark(sampleCount);
            }
        }

    }

    /**
     * Timeout the pending transaction
     * 
     * @param key
     */
    void timeout(HashKey key) {
        PendingTransaction pending = pendingTransactions.remove(key);
        if (pending == null) {
            return;
        }
        pending.timer.cancel(true);
        pending.pending.complete(null);
    }

    private SecureRandom getEntropy() {
        return getNode().getParameters().entropy;
    }

    private void getWanted() {
        List<HashKey> want = dag.getWanted(getEntropy());
        if (want.isEmpty()) {
            return;
        }

        if (metrics != null) {
            metrics.getWantedRate().mark(want.size());
        }

        Member next = sampler.next();
        if (next == null) {
            return;
        }

        AvalancheClientCommunications connection = comm.connectToNode(next, getNode());
        if (connection == null) {
            return;
        }

        List<ByteBuffer> requested;
        try {
            requested = connection.requestDAG(want.stream().map(e -> e.toHash()).collect(Collectors.toList()));
        } catch (AvroRemoteException e) {
            log.info("error getting wanted from {} ", next);
            return;
        }

        if (metrics != null) {
            metrics.getSatisfiedRate().mark(requested.size());
        }

        if (requested.isEmpty()) {
            return;
        }
        dag.insertSerialized(requested, System.currentTimeMillis());
    }

    private void initializeProcessors(ClassLoader resolver) {
        DSLContext create = null;
        try {
            try {
                final Connection connection = DriverManager.getConnection(parameters.dbConnect, USER_NAME, PASSWORD);
                connection.setAutoCommit(false);
                create = DSL.using(connection, SQLDialect.H2);
            } catch (SQLException e) {
                throw new IllegalStateException("unable to create jdbc connection", e);
            }
            create.transaction(config -> {
                DSLContext context = DSL.using(config);
                context.mergeInto(PROCESSORS)
                       .columns(PROCESSORS.HASH, PROCESSORS.PROCESSORCLASS)
                       .values(WellKnownDescriptions.GENESIS.toHash().bytes(), "<Genesis Handler>");
                context.mergeInto(PROCESSORS)
                       .columns(PROCESSORS.HASH, PROCESSORS.PROCESSORCLASS)
                       .values(WellKnownDescriptions.BYTE_CONTENT.toHash().bytes(), "<Unconstrainted Byte content>");
            });
            create.select(PROCESSORS.HASH, PROCESSORS.PROCESSORCLASS).from(PROCESSORS).stream().forEach(r -> {
                processors.computeIfAbsent(new HashKey(r.value1()), k -> resolve(r.value2(), resolver));
            });
        } finally {
            if (create != null) {
                create.close();
            }
        }
    }

    private EntryProcessor resolve(String processor, ClassLoader resolver) {
        try {
            return (EntryProcessor) resolver.loadClass(processor).getConstructor(new Class[0]).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            log.warn("Unresolved processor configured: {} : {}", processor, e);
            return null;
        }
    }
}
