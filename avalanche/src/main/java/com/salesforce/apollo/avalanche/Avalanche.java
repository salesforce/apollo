/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.commons.math3.util.Pair;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.WriteBuffer;
import org.h2.mvstore.type.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.salesfoce.apollo.messaging.proto.ByteMessage;
import com.salesfoce.apollo.proto.DagEntry;
import com.salesfoce.apollo.proto.DagEntry.EntryType;
import com.salesfoce.apollo.proto.QueryResult;
import com.salesfoce.apollo.proto.QueryResult.Vote;
import com.salesfoce.apollo.proto.SuppliedDagNodes;
import com.salesforce.apollo.avalanche.WorkingSet.FinalizationData;
import com.salesforce.apollo.avalanche.communications.AvalancheClient;
import com.salesforce.apollo.avalanche.communications.AvalancheServer;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.fireflies.View;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.utils.Utils;

/**
 * Implementation of the Avalanche consensus protocol.
 * 
 * @author hal.hildebrand
 * @since 220
 */
public class Avalanche {

    public static class DigestType implements DataType {

        @Override
        public int compare(Object a, Object b) {
            return ((Digest) a).compareTo(((Digest) b));
        }

        @Override
        public int getMemory(Object obj) {
            return ((Digest) obj).getAlgorithm().digestLength() + 1;
        }

        @Override
        public Digest read(ByteBuffer buff) {
            return new Digest(buff);
        }

        @Override
        public void read(ByteBuffer buff, Object[] obj, int len, boolean key) {
            for (int i = 0; i < len; i++) {
                obj[i] = read(buff);
            }
        }

        @Override
        public void write(WriteBuffer buff, Object obj) {
            Digest digest = (Digest) obj;
            buff.put(digest.getAlgorithm().digestCode());
            for (long l : digest.getLongs()) {
                buff.putLong(l);
            }
        }

        @Override
        public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
            for (int i = 0; i < len; i++) {
                write(buff, obj[i]);
            }
        }
    }

    public static class Finalized {
        public final DagEntry entry;
        public final Digest   hash;

        public Finalized(Digest hash, DagEntry dagEntry) {
            this.hash = hash;
            this.entry = dagEntry;
        }
    }

    public class Service {

        public QueryResult onQuery(List<ByteString> list, List<ByteString> txns, List<Digest> wanted) {
            if (!running.get()) {
                ArrayList<Vote> results = new ArrayList<>();
                for (int i = 0; i < txns.size(); i++) {
                    results.add(Vote.UNKNOWN);
                    if (metrics != null) {
                        metrics.getInboundQueryUnknownRate().mark();
                    }
                }
                return QueryResult.newBuilder().addAllResult(results).build();
            }
            long now = System.currentTimeMillis();
            Timer.Context timer = metrics == null ? null : metrics.getInboundQueryTimer().time();

            final List<Digest> inserted = dag.insertSerialized(list, txns, System.currentTimeMillis());
            List<Boolean> stronglyPreferred = dag.isStronglyPreferred(inserted);
            log.trace("onquery {} txn in {} ms", stronglyPreferred.size(), System.currentTimeMillis() - now);
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
                metrics.getInboundQueryRate().mark(txns.size());
            }
            assert queried.size() == txns.size() : "on query results " + queried.size() + " != " + txns.size();

            return QueryResult.newBuilder().addAllResult(queried).addAllWanted(dag.getEntries(wanted)).build();
        }

        public List<ByteString> requestDAG(List<Digest> want) {
            return dag.getEntries(want);
        }
    }

    private final static Logger log                = LoggerFactory.getLogger(Avalanche.class);
    private final static String STORE_MAP_TEMPLATE = "%s-%s-blocks";

    private final CommonCommunications<AvalancheClient, Service> comm;
    private final Context<? extends Member>                      context;
    private final WorkingSet                                     dag;
    private final int                                            invalidThreshold;
    private final AvalancheMetrics                               metrics;
    private final SigningMember                                  node;
    private final AvalancheParameters                            parameters;
    private final BlockingDeque<Digest>                          parentSample = new LinkedBlockingDeque<>();
    private final Processor                                      processor;
    private final Executor                                       queryExecutor;
    private volatile ScheduledFuture<?>                          queryFuture;
    private final AtomicLong                                     queryRounds  = new AtomicLong();
    private final int                                            required;
    private final AtomicBoolean                                  running      = new AtomicBoolean();
    private volatile ScheduledFuture<?>                          scheduledNoOpsCull;
    private final Service                                        service      = new Service();

    public Avalanche(SigningMember node, Context<? extends Member> context, Router communications,
            AvalancheParameters p, AvalancheMetrics metrics, Processor processor, MVStore store,
            Executor queryExecutor) {
        this.metrics = metrics;
        parameters = p;
        this.node = node;
        this.context = context;
        this.queryExecutor = queryExecutor;
        this.comm = communications.create(node, context.getId(), service,
                                          r -> new AvalancheServer(communications.getClientIdentityProvider(), metrics,
                                                  r),
                                          AvalancheClient.getCreate(metrics));
        MVMap.Builder<Digest, byte[]> builder = new MVMap.Builder<Digest, byte[]>().keyType(new DigestType());

        this.dag = new WorkingSet(processor, parameters,
                store.openMap(String.format(STORE_MAP_TEMPLATE, node.getId(), context.getId()), builder), metrics);

        required = (int) (parameters.core.k * parameters.core.alpha);
        invalidThreshold = parameters.core.k - required - 1;
        this.processor = processor;
    }

    public Avalanche(View view, Router communications, AvalancheParameters p, AvalancheMetrics metrics,
            Processor processor, MVStore store, Executor queryExecutor) {
        this(view.getNode(), view.getContext(), communications, p, metrics, processor, store, queryExecutor);
    }

    public Avalanche(View view, Router communications, AvalancheParameters p, Processor processor, MVStore store,
            Executor queryExecutor) {
        this(view, communications, p, null, processor, store, queryExecutor);
    }

    public WorkingSet getDag() {
        return dag;
    }

    public DagDao getDagDao() {
        return new DagDao(dag);
    }

    public SigningMember getNode() {
        return node;
    }

    public Service getService() {
        return service;
    }

    public void start(ScheduledExecutorService timer, Duration period) {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        comm.register(context.getId(), service);
        queryRounds.set(0);

        queryFuture = timer.schedule(() -> queryExecutor.execute(() -> {
            if (running.get()) {
                round(timer, period);
            }
        }), period.toMillis(), TimeUnit.MILLISECONDS);

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

    public Digest submitGenesis(Message data) {
        return submit(EntryType.GENSIS, data, WorkingSet.GENESIS_CONFLICT_SET);
    }

    /**
     * Submit a transaction to the group.
     * 
     * @param data - the transaction content
     * @return the Digest of the transaction, null if invalid
     */
    public Digest submitTransaction(Message data) {
        return submitTransaction(data, null);
    }

    /**
     * Submit a transaction to the group.
     * 
     * @param data        - the transaction content
     * @param conflictSet - the conflict set key for this transaction
     * @return the Digest of the transaction, null if invalid
     */
    public Digest submitTransaction(Message data, Digest conflictSet) {
        return submit(EntryType.USER, data, conflictSet);
    }

    private void finalize(List<Digest> preferings) {
        long then = System.currentTimeMillis();
        Timer.Context timer = metrics == null ? null : metrics.getFinalizeTimer().time();
        final FinalizationData finalized = dag.tryFinalize(preferings);
        if (timer != null) {
            timer.close();
        }
        if (metrics != null) {
            metrics.getFinalizerRate().mark(finalized.finalized.size());
        }
        queryExecutor.execute(() -> {
            processor.finalize(finalized);
        });
        log.debug("Finalizing: {}, deleting: {} in {} ms", finalized.finalized.size(), finalized.deleted.size(),
                  System.currentTimeMillis() - then);
    }

    /**
     * Periodically issue no op transactions for the neglected noOp transactions
     */
    private void generateNoOpTxns(boolean force) {
        if (!force && queryRounds.get() % parameters.noOpQueryFactor != 0) {
            return;
        }
        Deque<Digest> sample = dag.sampleNoOpParents(Utils.bitStreamEntropy(),
                                                     parameters.noOpsPerRound * parameters.maxNoOpParents);

        for (int i = 0; i < parameters.noOpsPerRound; i++) {
            TreeSet<Digest> parents = new TreeSet<>();
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
            Utils.bitStreamEntropy().nextBytes(dummy);
            DagEntry.Builder builder = DagEntry.newBuilder()
                                               .setDescription(EntryType.NO_OP)
                                               .setData(Any.pack(ByteMessage.newBuilder()
                                                                            .setContents(ByteString.copyFrom(dummy))
                                                                            .build()));
            parents.forEach(e -> builder.addLinks(e.toByteString()));
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

    private void getWanted(List<? extends Member> sample) {
        Collection<Digest> wanted = dag.getWanted(Utils.bitStreamEntropy(), parameters.maxWanted);
        if (wanted.isEmpty()) {
            log.trace("no wanted DAG entries");
            return;
        }
        Member member = new ArrayList<Member>(sample).get(Utils.bitStreamEntropy().nextInt(sample.size()));
        AvalancheClient connection = comm.apply(member, getNode());
        if (connection == null) {
            log.info("No connection requesting DAG from {} for {} entries", member, wanted.size());
        }
        ListenableFuture<SuppliedDagNodes> entries = connection.requestDAG(context.getId(), wanted);
        entries.addListener(() -> {
            SuppliedDagNodes suppliedDagNodes;
            try {
                suppliedDagNodes = entries.get();
            } catch (Exception e) {
                connection.release();
                log.trace("Error requesting DAG {} for {}", member, wanted.size(), e);
                return;
            }

            try {
                dag.insertSerialized(suppliedDagNodes.getEntriesList()
                                                     .stream()
                                                     .map(e -> DigestAlgorithm.DEFAULT.digest(e))
                                                     .map(e -> e.toByteString())
                                                     .collect(Collectors.toList()),
                                     suppliedDagNodes.getEntriesList(), System.currentTimeMillis());
                if (metrics != null) {
                    metrics.getWantedRate().mark(wanted.size());
                    metrics.getSatisfiedRate().mark(suppliedDagNodes.getEntriesList().size());
                }
            } finally {
                connection.release();
            }
        }, queryExecutor);
    }

    private void prefer(List<Digest> preferings) {
        Timer.Context timer = metrics == null ? null : metrics.getPreferTimer().time();
        dag.prefer(preferings);
        if (timer != null) {
            timer.close();
        }
        if (metrics != null) {
            metrics.getPreferRate().mark(preferings.size());
        }
    }

    private void process(List<Pair<Digest, ByteString>> query, AtomicInteger[] invalid, AtomicInteger[] votes,
                         Collection<Digest> want, Member wanted, Member m, QueryResult result) {
        log.trace("queried: {} for: {} result: {}", m, query.size(), result.getResultList().size());
        dag.insertSerialized(result.getWantedList()
                                   .stream()
                                   .map(e -> DigestAlgorithm.DEFAULT.digest(e))
                                   .map(e -> e.toByteString())
                                   .collect(Collectors.toList()),
                             result.getWantedList(), System.currentTimeMillis());
        if (want.size() > 0 && metrics != null && m == wanted) {
            metrics.getSatisfiedRate().mark(want.size());
        }
        if (result.getResultList().isEmpty()) {
            for (int i = 0; i < query.size(); i++) {
                invalid[i].incrementAndGet();
            }
            return;
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
    }

    private CompletableFuture<List<Boolean>> query(List<Pair<Digest, ByteString>> query,
                                                   List<? extends Member> sample) {
        AtomicInteger[] invalid = new AtomicInteger[query.size()];
        AtomicInteger[] votes = new AtomicInteger[query.size()];
        for (int i = 0; i < votes.length; i++) {
            invalid[i] = new AtomicInteger();
            votes[i] = new AtomicInteger();
        }

        Collection<Digest> want = dag.getWanted(Utils.bitStreamEntropy(), parameters.maxWanted);
        if (want.size() > 0 && metrics != null) {
            metrics.getWantedRate().mark(want.size());
        }

        CompletableFuture<List<Boolean>> futureSailor = new CompletableFuture<>();
        Member wanted = new ArrayList<Member>(sample).get(Utils.bitStreamEntropy().nextInt(sample.size()));
        AtomicInteger completed = new AtomicInteger(sample.size());
        List<Boolean> queryResults = new ArrayList<>();
        for (int i = 0; i < sample.size(); i++) {
            Member member = sample.get(i);
            queryExecutor.execute(() -> query(member, query, invalid, votes, futureSailor, completed, queryResults,
                                              want, wanted));
        }
        return futureSailor;
    }

    private void query(Member member, List<Pair<Digest, ByteString>> query, AtomicInteger[] invalid,
                       AtomicInteger[] votes, CompletableFuture<List<Boolean>> futureSailor, AtomicInteger completed,
                       List<Boolean> queryResults, Collection<Digest> want, Member wanted) {
        AvalancheClient connection = comm.apply(member, getNode());
        if (connection == null) {
            log.info("No connection querying {} for {} queries", member, query.size());
            for (int j = 0; j < query.size(); j++) {
                invalid[j].incrementAndGet();
            }
            return;
        }
        ListenableFuture<QueryResult> result;
        try {
            result = connection.query(context.getId(), query, member == wanted ? want : Collections.emptyList());
        } catch (Exception e) {
            for (int j = 0; j < query.size(); j++) {
                invalid[j].incrementAndGet();
            }
            log.debug("Error querying {} for {}", member, query, e);
            return;
        }
        result.addListener(() -> {
            QueryResult queryResult;
            try {
                queryResult = result.get();
                process(query, invalid, votes, want, wanted, member, queryResult);
            } catch (InterruptedException e) {
                for (int j = 0; j < query.size(); j++) {
                    invalid[j].incrementAndGet();
                }
                log.trace("Interrupted", e);
            } catch (ExecutionException e) {
                for (int j = 0; j < query.size(); j++) {
                    invalid[j].incrementAndGet();
                }
                log.trace("error querying {}", member, e.getCause());
            }
            if (completed.decrementAndGet() == 0) {
                for (int v = 0; v < query.size(); v++) {
                    if ((invalidThreshold <= invalid[v].get())) {
                        queryResults.add(null);
                    } else {
                        queryResults.add(votes[v].get() >= required);
                    }
                }
                futureSailor.complete(queryResults);
            }
        }, queryExecutor);
    }

    /**
     * Query a random sample of ye members for their votes on the next batch of
     * unqueried transactions for this node, determine confidence from the results
     * of the query results.
     */
    private int query(Runnable reschedule) {
        List<? extends Member> sample = context.sample(parameters.core.k, Utils.bitStreamEntropy(), node.getId());

        if (sample.isEmpty()) {
            reschedule.run();
            return 0;
        }
        if (sample.size() < parameters.core.k) {
            log.trace("not enough members in sample: {} < {}", sample.size(), parameters.core.k);
            reschedule.run();
            return 0;
        }
        queryRounds.incrementAndGet();
        Timer.Context timer = metrics == null ? null : metrics.getQueryTimer().time();

        getWanted(sample);
        List<Digest> unqueried = dag.query(parameters.queryBatchSize);
        if (unqueried.isEmpty()) {
            log.trace("no queries available");
            if (timer != null) {
                timer.close();
            }
            reschedule.run();
            return 0;
        }

        List<Pair<Digest, ByteString>> query = dag.getQuerySerializedEntries(unqueried);

        CompletableFuture<List<Boolean>> results = query(query, sample);
        results.whenComplete((queryResults, e) -> {
            try {
                if (timer != null) {
                    timer.close();
                }
                if (metrics != null) {
                    metrics.getQueryRate().mark(query.size());
                }
                List<Digest> unpreferings = new ArrayList<>();
                List<Digest> preferings = new ArrayList<>();
                for (int i = 0; i < queryResults.size(); i++) {
                    final Boolean result = queryResults.get(i);
                    Digest key = unqueried.get(i);
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
                    log.info("queried {} txns   failures: {}", unqueried.size(), unpreferings.size());
                }

                if (running.get()) {
                    prefer(preferings);
                    finalize(preferings);
                }
            } finally {
                reschedule.run();
            }
        });

        return query.size();
    }

    private void round(ScheduledExecutorService timer, Duration period) {
        queryExecutor.execute(() -> {
            try {
                generateNoOpTxns(query(() -> {
                    queryFuture = timer.schedule(() -> queryExecutor.execute(() -> {
                        if (running.get()) {
                            round(timer, period);
                        }
                    }), period.toMillis(), TimeUnit.MILLISECONDS);
                }) == 0);
            } catch (Throwable t) {
                log.error("Error performing Avalanche batch round", t);
            }
        });
    }

    private Digest submit(EntryType type, Message data, Digest conflictSet) {
        if (!running.get()) {
            throw new IllegalStateException("Service is not running");
        }
        Set<Digest> parents;

        if (EntryType.GENSIS == type) {
            parents = Collections.emptySet();
        } else {
            parents = new HashSet<>();
            if (parentSample.isEmpty()) {
                dag.sampleParents(parentSample, 1000, Utils.bitStreamEntropy());
            }
            if (parentSample.isEmpty()) {
                parents.addAll(dag.finalized(Utils.bitStreamEntropy(), parameters.parentCount));
            } else {
                parentSample.drainTo(parents, parameters.parentCount);
            }

            if (parents.isEmpty()) {
                log.error("No parents available for txn: {}", dag.finalizedCount());
                throw new IllegalStateException("No parents avaialable for transaction");
            }
        }

        Timer.Context timer = metrics == null ? null : metrics.getSubmissionTimer().time();
        try {

            DagEntry.Builder builder = DagEntry.newBuilder().setDescription(type).setData(Any.pack(data));
            parents.stream().map(e -> e.toByteString()).forEach(e -> builder.addLinks(e));
            DagEntry dagEntry = builder.build();

            Digest key = dag.insert(dagEntry, conflictSet, System.currentTimeMillis());
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
