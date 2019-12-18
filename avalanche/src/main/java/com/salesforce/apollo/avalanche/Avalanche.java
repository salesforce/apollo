/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.avro.AvroRemoteException;
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
import com.salesforce.apollo.fireflies.View;
import com.salesforce.apollo.protocols.HashKey;

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
                    dag.getEntries(wanted.stream().map(e -> new HashKey(e)).collect(Collectors.toList())));
        }

        public List<ByteBuffer> requestDAG(List<HASH> want) {
            return dag.getEntries(want.stream().map(e -> new HashKey(e)).collect(Collectors.toList()));
        }
    }

    private final static Logger log = LoggerFactory.getLogger(Avalanche.class);

    private final AvalancheCommunications                    comm;
    private final WorkingSet                                 dag;
    private final ExecutorService                            finalizer;
    private final int                                        invalidThreshold;
    private final AvaMetrics                                 metrics;
    private final AvalancheParameters                        parameters;
    private final Deque<HashKey>                             parentSample        = new LinkedBlockingDeque<>();
    private final ConcurrentMap<HashKey, PendingTransaction> pendingTransactions = new ConcurrentSkipListMap<>();
    private final Executor                                   queryPool;
    private final AtomicLong                                 queryRounds         = new AtomicLong();
    private final ViewSampler                                querySampler;
    private final int                                        required;
    private final AtomicBoolean                              running             = new AtomicBoolean();
    private volatile ScheduledFuture<?>                      scheduledNoOpsCull;
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
        this.dag = new WorkingSet(parameters, new DagWood(parameters.dagWood), metrics);

        querySampler = new ViewSampler(view, getEntropy());
        required = (int) (parameters.core.k * parameters.core.alpha);
        invalidThreshold = parameters.core.k - required - 1;

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
        queryPool = Executors.newFixedThreadPool(parameters.outstandingQueries, r -> {
            Thread t = new Thread(r, "Outbound Query[" + getNode().getId() + "] : " + i.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
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

    public Service getService() {
        return service;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        queryRounds.set(0);
        comm.start();

        Thread queryThread = new Thread(() -> {
            while (running.get()) {
                round();
                try {
                    Thread.sleep(0, 50);
                } catch (InterruptedException e) {
                }
            }
        }, "Query[" + view.getNode().getId() + "]");
        queryThread.setDaemon(true);

        queryThread.start();

        ScheduledExecutorService timer = Executors.newScheduledThreadPool(1);
        scheduledNoOpsCull = timer.scheduleWithFixedDelay(() -> dag.purgeNoOps(), parameters.noOpGenerationCullMillis,
                                                          parameters.noOpGenerationCullMillis, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        comm.close();
        pendingTransactions.values().clear();
        ScheduledFuture<?> current = scheduledNoOpsCull;
        scheduledNoOpsCull = null;
        if (current != null) {
            current.cancel(true);
        }
    }

    public HashKey submitTransaction(HASH description, byte[] data) {
        return submitTransaction(description, data, null, null, null);
    }

    /**
     * Submit a transaction to the group.
     * 
     * @param data    - the transaction content
     * @param timeout -how long to wait for finalization of the transaction
     * @param future  - optional future to be notified of finalization
     * @return the HashKey of the transaction, null if invalid
     */
    public HashKey submitTransaction(HASH description, byte[] data, Duration timeout, CompletableFuture<HashKey> future,
                                     ScheduledExecutorService scheduler) {
        if (!running.get()) {
            throw new IllegalStateException("Service is not running");
        }
        if (parentSample.isEmpty()) {
            dag.sampleParents(parentSample, getEntropy());
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
            log.info("No parents available for txn");
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
            return key;
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

    /**
     * for test access
     */
    ConcurrentMap<HashKey, PendingTransaction> getPendingTransactions() {
        return pendingTransactions;
    }

    private void finalize(List<HashKey> preferings) {
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

    /**
     * Periodically issue no op transactions for the neglected noOp transactions
     */
    private void generateNoOpTxns() {
        if (queryRounds.get() % parameters.noOpQueryFactor != 0) {
            return;
        }
        Deque<HashKey> sample = dag.sampleNoOpParents(getEntropy());

        for (int i = 0; i < parameters.noOpsPerRound; i++) {
            TreeSet<HashKey> parents = new TreeSet<>();
            while (parents.size() < parameters.maxNoOpParents) {
                if (sample.isEmpty()) {
                    break;
                }
                parents.add(sample.removeFirst());
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
                log.trace("noOp transaction {}", parents.size());
            }
            if (metrics != null) {
                metrics.getNoOpGenerationRate().mark();
            }
        }
    }

    private SecureRandom getEntropy() {
        return getNode().getParameters().entropy;
    }

    private void initializeProcessors(ClassLoader resolver) {
    }

    private void prefer(List<HashKey> preferings) {
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
    private int query() {
        long start = System.currentTimeMillis();
        long now = System.currentTimeMillis();
        List<Member> sample = querySampler.sample(parameters.core.k);

        if (sample.isEmpty()) {
            return 0;
        }
        if (sample.size() < parameters.core.k) {
            log.trace("not enough members in sample: {} < {}", sample.size(), parameters.core.k);
            return 0;
        }
        queryRounds.incrementAndGet();
        now = System.currentTimeMillis();
        Context timer = metrics == null ? null : metrics.getQueryTimer().time();
        List<HashKey> unqueried = dag.query(parameters.queryBatchSize);
        if (unqueried.isEmpty()) {
            log.trace("no queries available");
            if (timer != null) {
                timer.close();
            }
            List<HASH> wanted = dag.getWanted().stream().map(e -> e.toHash()).collect(Collectors.toList());
            if (wanted.isEmpty()) {
                log.trace("no wanted DAG entries");
                return 0;
            }
            Member member = sample.get(getEntropy().nextInt(sample.size()));
            AvalancheClientCommunications connection = comm.connectToNode(member, getNode());
            if (connection == null) {
                log.info("No connection requesting DAG from {} for {} entries", member, wanted.size());
            }
            try {
                List<ByteBuffer> entries = connection.requestDAG(wanted);
                dag.insertSerialized(entries, System.currentTimeMillis());
                if (metrics != null) {
                    metrics.getWantedRate().mark(wanted.size());
                    metrics.getSatisfiedRate().mark(entries.size());
                }
            } catch (AvroRemoteException e) {
                log.warn("Error requesting DAG {} for {}", member, wanted.size(), e);
            }
            return 0;
        }
        List<ByteBuffer> query = dag.getQuerySerializedEntries(unqueried);
        long sampleTime = System.currentTimeMillis() - now;

        now = System.currentTimeMillis();
        List<Boolean> results = query(query, sample);
        if (timer != null) {
            timer.close();
        }
        long retrieveTime = System.currentTimeMillis() - now;
        if (metrics != null) {
            metrics.getQueryRate().mark(query.size());
        }
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
    private List<Boolean> query(List<ByteBuffer> batch, List<Member> sample) {
        long now = System.currentTimeMillis();
        AtomicInteger[] invalid = new AtomicInteger[batch.size()];
        AtomicInteger[] votes = new AtomicInteger[batch.size()];
        for (int i = 0; i < votes.length; i++) {
            invalid[i] = new AtomicInteger();
            votes[i] = new AtomicInteger();
        }
        List<HASH> want = dag.getWanted().stream().map(e -> e.toHash()).collect(Collectors.toList());
        if (want.size() > 0 && metrics != null) {
            metrics.getWantedRate().mark(want.size());
        }

        CompletionService<Boolean> frist = new ExecutorCompletionService<>(queryPool);
        List<Future<Boolean>> futures;
        Member wanted = sample.get(getEntropy().nextInt(sample.size()));
        futures = sample.stream().map(m -> frist.submit(() -> {
            QueryResult result;
            AvalancheClientCommunications connection = comm.connectToNode(m, getNode());
            if (connection == null) {
                log.info("No connection querying {} for {} queries", m, batch.size());
                for (int i = 0; i < batch.size(); i++) {
                    invalid[i].incrementAndGet();
                }
                return false;
            }
            try {
                result = connection.query(batch, m == wanted ? want : Collections.emptyList());
            } catch (AvroRemoteException e) {
                for (int i = 0; i < batch.size(); i++) {
                    invalid[i].incrementAndGet();
                }
                log.warn("Error querying {} for {}", m, batch, e);
                return false;
            } finally {
                connection.close();
            }
            log.trace("queried: {} for: {} result: {}", m, batch.size(), result.getResult());
            dag.insertSerialized(result.getWanted(), System.currentTimeMillis());
            if (want.size() > 0 && metrics != null && m == wanted) {
                metrics.getSatisfiedRate().mark(want.size());
            }
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

    @SuppressWarnings("unused")
    private EntryProcessor resolve(String processor, ClassLoader resolver) {
        try {
            return (EntryProcessor) resolver.loadClass(processor).getConstructor(new Class[0]).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            log.warn("Unresolved processor configured: {} : {}", processor, e);
            return null;
        }
    }

    private void round() {
        try {
            query();
            generateNoOpTxns();
        } catch (Throwable t) {
            log.error("Error performing Avalanche batch round", t);
        }
    }

    /**
     * Timeout the pending transaction
     * 
     * @param key
     */
    private void timeout(HashKey key) {
        PendingTransaction pending = pendingTransactions.remove(key);
        if (pending == null) {
            return;
        }
        pending.timer.cancel(true);
        pending.pending.complete(null);
    }
}
