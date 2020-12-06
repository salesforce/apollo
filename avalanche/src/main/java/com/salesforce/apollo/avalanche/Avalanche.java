/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.proto.DagEntry;
import com.salesfoce.apollo.proto.DagEntry.Builder;
import com.salesfoce.apollo.proto.QueryResult;
import com.salesfoce.apollo.proto.QueryResult.Vote;
import com.salesforce.apollo.avalanche.WorkingSet.FinalizationData;
import com.salesforce.apollo.avalanche.communications.AvalancheClientCommunications;
import com.salesforce.apollo.avalanche.communications.AvalancheServerCommunications;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.fireflies.View;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.HashKey;

/**
 * Implementation of the Avalanche consensus protocol.
 * 
 * @author hal.hildebrand
 * @since 220
 */
public class Avalanche {

    public static class Finalized {
        public final DagEntry entry;
        public final HashKey  hash;

        public Finalized(HashKey hash, DagEntry dagEntry) {
            this.hash = hash;
            this.entry = dagEntry;
        }
    }

    public class Service {

        public QueryResult onQuery(List<ByteString> list, List<HashKey> wanted) {
            if (!running.get()) {
                ArrayList<Vote> results = new ArrayList<>();
                for (int i = 0; i < list.size(); i++) {
                    results.add(Vote.UNKNOWN);
                    if (metrics != null) {
                        metrics.getInboundQueryUnknownRate().mark();
                    }
                }
                return QueryResult.newBuilder().addAllResult(results).build();
            }
            long now = System.currentTimeMillis();
            Timer.Context timer = metrics == null ? null : metrics.getInboundQueryTimer().time();

            final List<HashKey> inserted = dag.insertSerialized(list, System.currentTimeMillis());
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
                metrics.getInboundQueryRate().mark(list.size());
            }
            assert queried.size() == list.size() : "on query results " + queried.size() + " != " + list.size();

            return QueryResult.newBuilder().addAllResult(queried).addAllWanted(dag.getEntries(wanted)).build();
        }

        public List<ByteString> requestDAG(List<HashKey> want) {
            return dag.getEntries(want);
        }
    }

    private final static Logger log = LoggerFactory.getLogger(Avalanche.class);

    private final CommonCommunications<AvalancheClientCommunications, Service> comm;
    private com.salesforce.apollo.membership.Context<? extends Member>         context;
    private final WorkingSet                                                   dag;
    private final SecureRandom                                                 entropy;
    private final int                                                          invalidThreshold;
    private final AvalancheMetrics                                             metrics;
    private final Node                                                         node;
    private final AvalancheParameters                                          parameters;
    private final Deque<HashKey>                                               parentSample = new LinkedBlockingDeque<>();
    private final Processor                                                    processor;
    private volatile ScheduledFuture<?>                                        queryFuture;
    private final AtomicLong                                                   queryRounds  = new AtomicLong();
    private final int                                                          required;
    private final AtomicBoolean                                                running      = new AtomicBoolean();
    private volatile ScheduledFuture<?>                                        scheduledNoOpsCull;
    private final Service                                                      service      = new Service();

    public Avalanche(Node node, Context<? extends Member> context, SecureRandom entropy, Router communications,
            AvalancheParameters p, AvalancheMetrics metrics, Processor processor) {
        this.metrics = metrics;
        parameters = p;
        this.node = node;
        this.context = context;
        this.entropy = entropy;
        this.comm = communications.create(node, context.getId(), service,
                                          r -> new AvalancheServerCommunications(
                                                  communications.getClientIdentityProvider(), metrics, r),
                                          AvalancheClientCommunications.getCreate(metrics));
        this.dag = new WorkingSet(processor, parameters, new DagWood(parameters.dagWood), metrics);

        required = (int) (parameters.core.k * parameters.core.alpha);
        invalidThreshold = parameters.core.k - required - 1;
        this.processor = processor;
    }

    public Avalanche(View view, Router communications, AvalancheParameters p, AvalancheMetrics metrics,
            Processor processor) {
        this(view.getNode(), view.getContext(), view.getParameters().entropy, communications, p, metrics, processor);
    }

    public Avalanche(View view, Router communications, AvalancheParameters p, Processor processor) {
        this(view, communications, p, null, processor);
    }

    public WorkingSet getDag() {
        return dag;
    }

    public DagDao getDagDao() {
        return new DagDao(dag);
    }

    public Node getNode() {
        return node;
    }

    public Service getService() {
        return service;
    }

    public void start(ScheduledExecutorService timer) {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        comm.register(context.getId(), service);
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
        comm.deregister(context.getId());
        ScheduledFuture<?> currentQuery = queryFuture;
        queryFuture = null;
        if (currentQuery != null) {
            currentQuery.cancel(true);
        }
        ScheduledFuture<?> current = scheduledNoOpsCull;
        scheduledNoOpsCull = null;
        if (current != null) {
            current.cancel(true);
        }
    }

    public HashKey submitGenesis(byte[] data) {
        return submit(WellKnownDescriptions.GENESIS.toHash(), ByteString.copyFrom(data),
                      WorkingSet.GENESIS_CONFLICT_SET, true);
    }

    /**
     * Submit a transaction to the group.
     * 
     * @param description - the transaction classification
     * @param data        - the transaction content
     * @return the HashKey of the transaction, null if invalid
     */
    public HashKey submitTransaction(HashKey description, byte[] data) {
        return submitTransaction(description, data, null);
    }

    /**
     * Submit a transaction to the group.
     * 
     * @param description - the transaction classification
     * @param data        - the transaction content
     * @param conflictSet - the conflict set key for this transaction
     * @return the HashKey of the transaction, null if invalid
     */
    public HashKey submitTransaction(HashKey description, byte[] data, HashKey conflictSet) {
        return submitTransaction(description, ByteString.copyFrom(data), conflictSet);
    }

    /**
     * Submit a transaction to the group.
     * 
     * @param description - the transaction classification
     * @param data        - the transaction content
     * @param conflictSet - the conflict set key for this transaction
     * @return the HashKey of the transaction, null if invalid
     */
    public HashKey submitTransaction(HashKey description, ByteString data, HashKey conflictSet) {
        return submit(description, data, conflictSet, false);
    }

    private void finalize(List<HashKey> preferings) {
        long then = System.currentTimeMillis();
        Timer.Context timer = metrics == null ? null : metrics.getFinalizeTimer().time();
        final FinalizationData finalized = dag.tryFinalize(preferings);
        if (timer != null) {
            timer.close();
        }
        if (metrics != null) {
            metrics.getFinalizerRate().mark(finalized.finalized.size());
        }
        ForkJoinPool.commonPool().execute(() -> {
            processor.finalize(finalized);
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
            parents.stream().map(e -> e.toID()).forEach(e -> builder.addLinks(e));
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
        return entropy;
    }

    private void prefer(List<HashKey> preferings) {
        Timer.Context timer = metrics == null ? null : metrics.getPreferTimer().time();
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
        Collection<? extends Member> sample = context.sample(parameters.core.k, getEntropy(), node.getId());

        if (sample.isEmpty()) {
            return 0;
        }
        if (sample.size() < parameters.core.k) {
            log.trace("not enough members in sample: {} < {}", sample.size(), parameters.core.k);
            return 0;
        }
        queryRounds.incrementAndGet();
        now = System.currentTimeMillis();
        Timer.Context timer = metrics == null ? null : metrics.getQueryTimer().time();
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
                List<ByteString> entries = connection.requestDAG(context.getId(), wanted);
                dag.insertSerialized(entries, System.currentTimeMillis());
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
        List<ByteString> query = dag.getQuerySerializedEntries(unqueried);
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

        if (metrics != null) {
            metrics.getFailedTxnQueryRate().mark(unpreferings.size());
        }
        if (!unpreferings.isEmpty()) {
            log.info("querying {} txns in {} ms failures: {}", unqueried.size(), System.currentTimeMillis() - start,
                     unpreferings.size());
        }
        log.trace("querying {} txns in {} ms ({} Query) ({} Sample)", unqueried.size(),
                  System.currentTimeMillis() - start, retrieveTime, sampleTime);

        if (running.get()) {
            prefer(preferings);
            finalize(preferings);
        }

        return results.size();
    }

    /**
     * Query the sample of members for their boolean opinion on the query of
     * transaction entries
     * 
     * @param query  - the batch of unqueried nodes
     * @param sample - the random sample of members to query
     * @return for each sampled member, the list of query results for the
     *         transaction batch, or NULL if there could not be a determination
     *         (such as comm failure) of the query for that member
     */
    private List<Boolean> query(List<ByteString> query, Collection<? extends Member> sample) {
        long now = System.currentTimeMillis();
        AtomicInteger[] invalid = new AtomicInteger[query.size()];
        AtomicInteger[] votes = new AtomicInteger[query.size()];
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
                log.info("No connection querying {} for {} queries", m, query.size());
                for (int i = 0; i < query.size(); i++) {
                    invalid[i].incrementAndGet();
                }
                return false;
            }
            try {
                result = connection.query(context.getId(), query, m == wanted ? want : Collections.emptyList());
            } catch (Exception e) {
                for (int i = 0; i < query.size(); i++) {
                    invalid[i].incrementAndGet();
                }
                log.debug("Error querying {} for {}", m, query, e);
                return false;
            } finally {
                connection.release();
            }
            log.trace("queried: {} for: {} result: {}", m, query.size(), result.getResultList());
            dag.insertSerialized(result.getWantedList(), System.currentTimeMillis());
            if (want.size() > 0 && metrics != null && m == wanted) {
                metrics.getSatisfiedRate().mark(want.size());
            }
            if (result.getResultList().isEmpty()) {
                for (int i = 0; i < query.size(); i++) {
                    invalid[i].incrementAndGet();
                }
                return false;
            }
            for (int i = 0; i < query.size(); i++) {
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
                                log.debug("exception querying {}", query.size(), e.getCause());
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
        for (int i = 0; i < query.size(); i++) {
            if ((invalidThreshold <= invalid[i].get())) {
                queryResults.add(null);
            } else {
                queryResults.add(votes[i].get() >= required);
            }
        }

        log.debug("query results: {} in: {} ms", queryResults.size(), System.currentTimeMillis() - now);

        return queryResults;
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

    private HashKey submit(HashKey description, ByteString data, HashKey conflictSet, boolean genesis) {
        if (!running.get()) {
            throw new IllegalStateException("Service is not running");
        }
        if (parentSample.isEmpty()) {
            dag.sampleParents(parentSample, getEntropy());
        }
        Set<HashKey> parents;

        if (genesis) {
            if (!WellKnownDescriptions.GENESIS.toHash().equals(description)) {
                throw new IllegalArgumentException("Must use GENESIS description for genesis block");
            }
            parents = Collections.emptySet();
        } else {
            parents = new HashSet<>();
            while (parents.size() < parameters.parentCount) {
                HashKey parent = parentSample.poll();
                if (parent == null) {
                    break;
                }
                parents.add(parent);
            }
            if (parents.isEmpty()) {
                log.error("No parents available for txn");
                throw new IllegalStateException("No parents avaialable for transaction");
            }
        }

        Timer.Context timer = metrics == null ? null : metrics.getSubmissionTimer().time();
        try {

            Builder builder = DagEntry.newBuilder().setDescription(description.toID()).setData(data);
            parents.stream().map(e -> e.toID()).forEach(e -> builder.addLinks(e));
            DagEntry dagEntry = builder.build();

            HashKey key = dag.insert(dagEntry, conflictSet, System.currentTimeMillis());
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
}
