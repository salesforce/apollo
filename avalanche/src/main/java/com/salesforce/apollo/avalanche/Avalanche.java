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
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer.Context;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.proto.DagEntry;
import com.salesfoce.apollo.proto.DagEntry.Builder;
import com.salesfoce.apollo.proto.QueryResult;
import com.salesfoce.apollo.proto.QueryResult.Vote;
import com.salesforce.apollo.avalanche.WorkingSet.FinalizationData;
import com.salesforce.apollo.avalanche.communications.AvalancheClientCommunications;
import com.salesforce.apollo.avalanche.communications.AvalancheServerCommunications;
import com.salesforce.apollo.comm.CommonCommunications;
import com.salesforce.apollo.comm.Communications;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.fireflies.View;
import com.salesforce.apollo.membership.Member;
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

        public QueryResult onQuery(List<ByteBuffer> transactions, List<HashKey> wanted) {
            if (!running.get()) {
                ArrayList<Vote> results = new ArrayList<>();
                for (int i = 0; i < transactions.size(); i++) {
                    results.add(Vote.UNKNOWN);
                    if (metrics != null) {
                        metrics.getInboundQueryUnknownRate().mark();
                    }
                }
                return QueryResult.newBuilder().addAllResult(results).build();
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

            return QueryResult.newBuilder()
                              .addAllResult(queried)
                              .addAllWanted(dag.getEntries(wanted)
                                               .stream()
                                               .map(e -> ByteString.copyFrom(e))
                                               .collect(Collectors.toList()))
                              .build();
        }

        public List<ByteBuffer> requestDAG(List<HashKey> want) {
            return dag.getEntries(want);
        }
    }

    private final static Logger log = LoggerFactory.getLogger(Avalanche.class);

    private final CommonCommunications<AvalancheClientCommunications> comm;
    private final WorkingSet                                          dag;
    private final int                                                 invalidThreshold;
    private final AvalancheMetrics                                    metrics;
    private final AvalancheParameters                                 parameters;
    private final Deque<HashKey>                                      parentSample        = new LinkedBlockingDeque<>();
    private final ConcurrentMap<HashKey, PendingTransaction>          pendingTransactions = new ConcurrentSkipListMap<>();
    private volatile ScheduledFuture<?>                               queryFuture;
    private final AtomicLong                                          queryRounds         = new AtomicLong();
    private final int                                                 required;
    private final AtomicBoolean                                       running             = new AtomicBoolean();
    private volatile ScheduledFuture<?>                               scheduledNoOpsCull;
    private final Service                                             service             = new Service();
    private final View                                                view;

    public Avalanche(View view, Communications communications, AvalancheParameters p) {
        this(view, communications, p, null, null);
    }

    public Avalanche(View view, Communications communications, AvalancheParameters p, AvalancheMetrics metrics) {
        this(view, communications, p, metrics, null);
    }

    public Avalanche(View view, Communications communications, AvalancheParameters p, AvalancheMetrics metrics,
            ClassLoader resolver) {
        this.metrics = metrics;
        parameters = p;
        this.view = view;
        this.comm = communications.create(getNode(), AvalancheClientCommunications.getCreate(metrics),
                                          new AvalancheServerCommunications(service,
                                                  communications.getClientIdentityProvider(), metrics));
        this.dag = new WorkingSet(parameters, new DagWood(parameters.dagWood), metrics);

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
        DagEntry dagEntry = DagEntry.newBuilder()
                                    .setDescription(WellKnownDescriptions.GENESIS.toHash().toByteString())
                                    .setData(ByteString.copyFrom(data))
                                    .build();
        // genesis has no parents
        HashKey key = dag.insert(dagEntry, WellKnownDescriptions.GENESIS.toHash(), System.currentTimeMillis());
        log.info("Genesis added: {}", key);
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

    public void start(ScheduledExecutorService timer) {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        queryRounds.set(0);

        queryFuture = timer.schedule(() -> {
            if (running.get()) {
                round(timer);
            }
        }, 50, TimeUnit.MICROSECONDS);

        scheduledNoOpsCull = timer.scheduleWithFixedDelay(() -> dag.purgeNoOps(), parameters.noOpGenerationCullMillis,
                                                          parameters.noOpGenerationCullMillis, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        pendingTransactions.values().clear();
        ScheduledFuture<?> currenQuery = queryFuture;
        queryFuture = null;
        if (currenQuery != null) {
            currenQuery.cancel(true);
        }
        ScheduledFuture<?> current = scheduledNoOpsCull;
        scheduledNoOpsCull = null;
        if (current != null) {
            current.cancel(true);
        }
    }

    public HashKey submitTransaction(HashKey description, byte[] data) {
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
    public HashKey submitTransaction(HashKey description, byte[] data, Duration timeout,
                                     CompletableFuture<HashKey> future, ScheduledExecutorService scheduler) {
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

            Builder builder = DagEntry.newBuilder()
                                      .setDescription(description.toByteString())
                                      .setData(ByteString.copyFrom(data));
            parents.stream().map(e -> e.toByteString()).forEach(e -> builder.addLinks(e));
            DagEntry dagEntry = builder.build();

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
    public CompletableFuture<HashKey> submitTransaction(HashKey description, byte[] data, Duration timeout,
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
                assert key != null;
                PendingTransaction pending = pendingTransactions.remove(key);
                if (pending != null) {
                    pending.complete(key);
                }
            });

            finalized.deleted.forEach(key -> {
                PendingTransaction pending = pendingTransactions.remove(key);
                if (pending != null) {
                    pending.pending.completeExceptionally(new TransactionRejected(
                            "Transaction rejected due to conflict resolution"));
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
            byte[] dummy = new byte[4];
            getEntropy().nextBytes(dummy);
            Builder builder = DagEntry.newBuilder().setData(ByteString.copyFrom(dummy));
            parents.stream().map(e -> e.toByteString()).forEach(e -> builder.addLinks(e));
            DagEntry dagEntry = builder.build();
            assert dagEntry.getLinksCount() > 0 : "Whoopsie";
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
        return view.getParameters().entropy;
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
        Collection<? extends Member> sample = view.sample(parameters.core.k, getEntropy(), view.getNode());

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
            Set<HashKey> wanted = dag.getWanted();
            if (wanted.isEmpty()) {
                log.trace("no wanted DAG entries");
                return 0;
            }
            Member member = new ArrayList<Member>(sample).get(getEntropy().nextInt(sample.size()));
            AvalancheClientCommunications connection = comm.apply(member, getNode());
            if (connection == null) {
                log.info("No connection requesting DAG from {} for {} entries", member, wanted.size());
            }
            try {
                List<byte[]> entries = connection.requestDAG(wanted);
                dag.insertSerializedRaw(entries, System.currentTimeMillis());
                if (metrics != null) {
                    metrics.getWantedRate().mark(wanted.size());
                    metrics.getSatisfiedRate().mark(entries.size());
                }
            } catch (Exception e) {
                log.warn("Error requesting DAG {} for {}", member, wanted.size(), e);
            } finally {
                connection.release();
            }
            return 0;
        }
        List<ByteBuffer> query = dag.getQuerySerializedEntries(unqueried);

        for (ByteBuffer e : query) {
            assert e.hasRemaining() : "whoopsie";
        }
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
        ForkJoinPool.commonPool().execute(() -> {
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
    private List<Boolean> query(List<ByteBuffer> batch, Collection<? extends Member> sample) {
        long now = System.currentTimeMillis();
        AtomicInteger[] invalid = new AtomicInteger[batch.size()];
        AtomicInteger[] votes = new AtomicInteger[batch.size()];
        for (int i = 0; i < votes.length; i++) {
            invalid[i] = new AtomicInteger();
            votes[i] = new AtomicInteger();
        }
        Set<HashKey> want = dag.getWanted();
        if (want.size() > 0 && metrics != null) {
            metrics.getWantedRate().mark(want.size());
        }

        CompletionService<Boolean> frist = new ExecutorCompletionService<>(ForkJoinPool.commonPool());
        List<Future<Boolean>> futures;
        Member wanted = new ArrayList<Member>(sample).get(getEntropy().nextInt(sample.size()));
        futures = sample.stream().map(m -> frist.submit(() -> {
            QueryResult result;
            AvalancheClientCommunications connection = comm.apply(m, getNode());
            if (connection == null) {
                log.info("No connection querying {} for {} queries", m, batch.size());
                for (int i = 0; i < batch.size(); i++) {
                    invalid[i].incrementAndGet();
                }
                return false;
            }
            try {
                result = connection.query(batch, m == wanted ? want : Collections.emptyList());
            } catch (Exception e) {
                for (int i = 0; i < batch.size(); i++) {
                    invalid[i].incrementAndGet();
                }
                log.warn("Error querying {} for {}", m, batch, e);
                return false;
            } finally {
                connection.release();
            }
            log.trace("queried: {} for: {} result: {}", m, batch.size(), result.getResultList());
            dag.insertSerializedRaw(result.getWantedList()
                                          .stream()
                                          .map(e -> e.toByteArray())
                                          .collect(Collectors.toList()),
                                    System.currentTimeMillis());
            if (want.size() > 0 && metrics != null && m == wanted) {
                metrics.getSatisfiedRate().mark(want.size());
            }
            if (result.getResultList().isEmpty()) {
                for (int i = 0; i < batch.size(); i++) {
                    invalid[i].incrementAndGet();
                }
                return false;
            }
            for (int i = 0; i < batch.size(); i++) {
                switch (result.getResult(i)) {
                case FALSE:
                    break;
                case TRUE:
                    votes[i].incrementAndGet();
                    break;
                case UNKNOWN:
                case UNRECOGNIZED:
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

    private void round(ScheduledExecutorService timer) {
        try {
            query();
            generateNoOpTxns();
        } catch (Throwable t) {
            log.error("Error performing Avalanche batch round", t);
        } finally {
            queryFuture = timer.schedule(() -> {
                if (running.get()) {
                    round(timer);
                }
            }, 50, TimeUnit.MICROSECONDS);
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
        pending.pending.completeExceptionally(new TimeoutException("Transaction timeout"));
    }
}
