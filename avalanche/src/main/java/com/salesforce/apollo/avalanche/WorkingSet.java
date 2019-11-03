/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.avalanche;

import static com.salesforce.apollo.dagwood.schema.Tables.DAG;
import static com.salesforce.apollo.dagwood.schema.Tables.UNFINALIZED;
import static com.salesforce.apollo.protocols.Conversion.hashOf;
import static com.salesforce.apollo.protocols.Conversion.manifestDag;
import static com.salesforce.apollo.protocols.Conversion.serialize;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.jooq.BatchBindStep;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.salesforce.apollo.avro.DagEntry;
import com.salesforce.apollo.avro.HASH;
import com.salesforce.apollo.protocols.HashKey;

/**
 * Manages the unfinalized, working set of the Apollo DAG.
 * 
 * @author hhildebrand
 *
 */
public class WorkingSet {

    public static class FinalizationData {
        public final Set<HashKey> deleted   = new TreeSet<>();
        public final Set<HashKey> finalized = new TreeSet<>();
    }

    public class KnownNode extends Node {
        private volatile boolean  chit       = false;
        private final Set<Node>   closure;
        private volatile int      confidence = 0;
        private final ConflictSet conflictSet;
        private final Set<Node>   dependents = new ConcurrentSkipListSet<>();
        private final DagEntry    entry;
        private volatile boolean  finalized  = false;
        private final Set<Node>   links;

        public KnownNode(HashKey key, DagEntry entry, Set<Node> links, HashKey cs, long discovered) {
            super(key, discovered);
            this.entry = entry;
            conflictSet = conflictSets.computeIfAbsent(cs, k -> new ConflictSet(k, this));
            conflictSet.add(this);
            this.links = links;
            this.closure = new TreeSet<>();

            for (Node node : links) {
                assert !node.isNoOp() : "Cannot have NoOps as parent links";
                node.addDependent(this);
                node.addClosureTo(closure);
            }
        }

        @Override
        public void addClosureTo(Set<Node> closureSet) {
            closureSet.addAll(links);
            closureSet.addAll(closure);
        }

        @Override
        public void addDependent(Node node) {
            dependents.add(node);
        }

        @Override
        public Set<Node> closure() {
            return closure;
        }

        @Override
        public void delete() {
            // TODO Auto-generated method stub

        }

        @Override
        public boolean finalized() {
            final boolean wasFinalized = finalized;
            finalized = true;
            return wasFinalized;
        }

        public boolean getChit() {
            return chit;
        }

        @Override
        public int getConfidence() {
            return confidence;
        }

        @Override
        public ConflictSet getConflictSet() {
            return conflictSet;
        }

        @Override
        public DagEntry getDagEntry() {
            return entry;
        }

        @Override
        public boolean isFinalized() {
            final boolean isFinalized = finalized;
            return isFinalized;
        }

        @Override
        public boolean isPreferred() {
            final boolean current = finalized;
            return current || conflictSet.getPreferred().equals(this);
        }

        @Override
        public boolean isPreferred(int maxConfidence) {
            final int current = confidence;
            return current <= maxConfidence && conflictSet.getPreferred().equals(this);
        }

        @Override
        public boolean isPreferredAndSingular(int maximumConfidence) {
            final boolean current = finalized;
            final int conf = confidence;
            return !current && conf <= maximumConfidence && conflictSet.getPreferred().equals(this)
                    && conflictSet.getCardinality() == 1;
        }

        @Override
        public boolean isStronglyPreferred() {
            if (conflictSet.getPreferred().equals(this)) {
                for (Node node : links) {
                    if (!node.isPreferred()) {
                        return false;
                    }
                }
                for (Node node : closure) {
                    if (!node.isPreferred()) {
                        return false;
                    }
                }
                return true;
            }
            return false;
        }

        @Override
        public boolean isUnfinalizedSingular() {
            final boolean current = finalized;
            return !current && conflictSet.getCardinality() == 1 && conflictSet.getPreferred().equals(this);
        }

        @Override
        public void markFinalized() {
            finalized = true;
        }

        @Override
        public void markPreferred() {
            confidence++;
            conflictSet.prefer(this);
        }

        @Override
        public void prefer() {
            chit = true;
            markPreferred();
            links.forEach(node -> node.markPreferred());
            closure.forEach(node -> node.markPreferred());
        }

        @Override
        public void replace(UnknownNode unknownNode, Node replacement) {
            if (links.remove(unknownNode)) {
                links.add(replacement);
                replacement.addDependent(this);
                replacement.addClosureTo(closure);
            }

        }

        @Override
        public void snip() {
            dependents.forEach(node -> {
                node.snip(Collections.singletonList(this));
                node.snip(links);
                node.snip(closure);
            });
            dependents.clear();
            unfinalized.remove(key);
            closure().forEach(node -> node.snip());
        }

        @Override
        public void snip(Collection<Node> nodes) {
            links.removeAll(nodes);
            closure.removeAll(nodes);
        }

        @Override
        public int sumChits() {
            return dependents.stream().mapToInt(node -> node.sumChits()).sum() + (chit ? 1 : 0);
        }

        @Override
        public boolean tryFinalize(Set<Node> finalizedSet, Set<Node> visited) {
            if (!visited.add(this)) {
                return finalized;
            }
            final boolean isFinalized = finalized;
            if (isFinalized) {
                return true;
            }
            final int currentConfidence = confidence;
            if (currentConfidence >= parameters.beta1 && conflictSet.getCardinality() == 1
                    && conflictSet.getPreferred().equals(this)) {
                if (links.stream()
                         .map(node -> node.tryFinalize(finalizedSet, visited))
                         .filter(success -> success)
                         .count() == links.size()) {
                    finalizedSet.add(this);
                    finalized = true;
                    return true;
                }
                final boolean current = finalized;
                return current;
            } else if (conflictSet.getCounter() >= parameters.beta2 && conflictSet.getPreferred().equals(this)) {
                finalized = true;
                links.forEach(node -> node.markFinalized());
                closure.forEach(node -> node.markFinalized());
                finalizedSet.add(this);
                finalizedSet.addAll(links);
                finalizedSet.addAll(closure);
                return true;
            } else {
                links.forEach(node -> node.tryFinalize(finalizedSet, visited));
                return false;
            }
        }
    }

    public static abstract class Node implements Comparable<Node> {
        protected final long    discovered;
        protected final HashKey key;

        public Node(HashKey key, long discovered) {
            this.key = key;
            this.discovered = discovered;
        }

        public abstract void addClosureTo(Set<Node> closure);

        abstract public void addDependent(Node node);

        abstract public Set<Node> closure();

        @Override
        public int compareTo(Node o) {
            return getKey().compareTo(o.getKey());
        }

        abstract public void delete();

        public abstract boolean finalized();

        public boolean getChit() {
            return false;
        }

        abstract public int getConfidence();

        public ConflictSet getConflictSet() {
            throw new IllegalStateException(getClass().getSimpleName() + " do not have conflict sets");
        }

        abstract public DagEntry getDagEntry();

        public long getDiscovered() {
            return discovered;
        }

        public HashKey getKey() {
            return key;
        }

        abstract public boolean isFinalized();

        public boolean isNoOp() {
            return false;
        }

        public boolean isPreferred() {
            return false;
        }

        public boolean isPreferred(int maxConfidence) {
            return false;
        }

        public boolean isPreferredAndSingular(int maximumConfidence) {
            return false;
        }

        public boolean isStronglyPreferred() {
            return false;
        }

        public boolean isUnfinalizedSingular() {
            return false;
        }

        abstract public void markFinalized();

        public void markPreferred() {
        }

        abstract public void prefer();

        public abstract void replace(UnknownNode unknownNode, Node replacement);

        public void snip() {
        }

        public void snip(Collection<Node> nodes) {
        }

        public int sumChits() {
            return 0;
        }

        abstract public boolean tryFinalize(Set<Node> finalizedSet, Set<Node> visited);
    }

    public static class NoOpNode extends Node {
        private volatile boolean chit = false;
        private final Set<Node>  closure;
        private final DagEntry   entry;

        public NoOpNode(HashKey key, DagEntry entry, Set<Node> links, long discovered) {
            super(key, discovered);
            this.entry = entry;
            closure = new TreeSet<>();
            links.forEach(node -> {
                if (closure.add(node)) {
                    closure.addAll(node.closure());
                }
            });
        }

        @Override
        public void addClosureTo(Set<Node> closure) {
            throw new IllegalStateException("NoOps can never be parents");
        }

        @Override
        public void addDependent(Node node) {
            throw new IllegalStateException("No ops cannot be parents");
        }

        @Override
        public Set<Node> closure() {
            return closure;
        }

        @Override
        public void delete() {
            // TODO Auto-generated method stub

        }

        @Override
        public boolean finalized() {
            throw new IllegalStateException("No op nodes cannot be finalized");
        }

        public boolean getChit() {
            return chit;
        }

        @Override
        public int getConfidence() {
            return 0;
        }

        @Override
        public DagEntry getDagEntry() {
            return entry;
        }

        @Override
        public boolean isFinalized() {
            return false;
        }

        @Override
        public boolean isNoOp() {
            return true;
        }

        @Override
        public boolean isStronglyPreferred() {
            for (Node node : closure) {
                if (!node.isPreferred()) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public void markFinalized() {
            throw new IllegalStateException("NoOps cannot be finalized");
        }

        @Override
        public void markPreferred() {
            throw new IllegalStateException("NoOps cannot be parents");
        }

        @Override
        public void prefer() {
            chit = true;
            closure.forEach(node -> node.markPreferred());
        }

        @Override
        public void replace(UnknownNode unknownNode, Node replacement) {
            closure.remove(unknownNode);
            replacement.addClosureTo(closure);
        }

        @Override
        public void snip(Collection<Node> nodes) {
            closure.removeAll(nodes);
        }

        @Override
        public int sumChits() {
            return chit ? 1 : 0;
        }

        @Override
        public boolean tryFinalize(Set<Node> finalizedSet, Set<Node> visited) {
            closure.forEach(node -> node.tryFinalize(finalizedSet, visited));
            return false;
        }
    }

    public class UnknownNode extends Node {

        private final Set<Node>  dependencies = new ConcurrentSkipListSet<>();
        private volatile boolean finalized;

        public UnknownNode(HashKey key, long discovered) {
            super(key, discovered);
        }

        @Override
        public void addClosureTo(Set<Node> closure) {
        }

        @Override
        public void addDependent(Node node) {
            dependencies.add(node);
        }

        @Override
        public Set<Node> closure() {
            return Collections.emptySet();
        }

        @Override
        public void delete() {
            // TODO Auto-generated method stub

        }

        @Override
        public boolean finalized() {
            return false;
        }

        @Override
        public int getConfidence() {
            return 0;
        }

        @Override
        public DagEntry getDagEntry() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public boolean isFinalized() {
            return finalized;
        }

        @Override
        public void markFinalized() {
            finalized = true;
        }

        @Override
        public void prefer() {
            throw new IllegalStateException("Unknown nodes cannot be preferred");
        }

        @Override
        public void replace(UnknownNode unknownNode, Node replacement) {
            throw new IllegalStateException("Unknown nodes do not have children");
        }

        public void replaceWith(Node replacement) {
            dependencies.forEach(node -> node.replace(this, replacement));
        }

        @Override
        public void snip() {
            dependencies.forEach(node -> {
                node.snip(Collections.singletonList(this));
            });
            unfinalized.remove(key);
        }

        @Override
        public boolean tryFinalize(Set<Node> finalizedSet, Set<Node> visited) {
            return finalized;
        }
    }

    public static final HashKey                      GENESIS_CONFLICT_SET = new HashKey(new byte[32]);
    private final NavigableMap<HashKey, ConflictSet> conflictSets         = new ConcurrentSkipListMap<>();
    private final Cache<HashKey, Boolean>            finalizedCache;
    private final AvalancheParameters                parameters;
    private final NavigableMap<HashKey, Node>        unfinalized          = new ConcurrentSkipListMap<>();
    private final Set<Node>                          unknown              = new ConcurrentSkipListSet<>();
    private final BlockingDeque<HashKey>             unqueried            = new LinkedBlockingDeque<>();

    public WorkingSet(AvalancheParameters parameters) {
        this.parameters = parameters;
        CacheBuilder<?, ?> builder = CacheBuilder.newBuilder();
        @SuppressWarnings("unchecked")
        CacheBuilder<HashKey, Boolean> cacheBuilder = (CacheBuilder<HashKey, Boolean>) builder;
        finalizedCache = cacheBuilder.build();
    }

    public List<HashKey> finalized(DSLContext context) {
        return context.select(DAG.KEY)
                      .from(DAG)
                      .orderBy(DSL.rand())
                      .limit(6)
                      .stream()
                      .map(r -> new HashKey(r.value1()))
                      .collect(Collectors.toList());
    }

    public List<HashKey> frontier(Random entropy) {
        List<HashKey> sample = unfinalized.values()
                                          .stream()
                                          .filter(node -> node.isPreferred(parameters.beta1 - 1))
                                          .map(node -> node.getKey())
                                          .collect(Collectors.toList());
        Collections.shuffle(sample, entropy);
        return sample;
    }

    public DagEntry get(HashKey key, DSLContext create) {
        Record1<byte[]> entry = create.select(UNFINALIZED.DATA)
                                      .from(UNFINALIZED)
                                      .where(UNFINALIZED.HASH.eq(key.bytes()))
                                      .fetchOne();
        if (entry == null) {
            entry = create.select(DAG.DATA).from(DAG).where(DAG.KEY.eq(key.bytes())).fetchOne();
        }
        if (entry == null) {
            return null;
        }
        return manifestDag(entry.value1());
    }

    public ConflictSet getConflictSet(HashKey key) {
        Node node = unfinalized.get(key);
        if (node == null) {
            return null;
        }
        return node.getConflictSet();
    }

    public NavigableMap<HashKey, ConflictSet> getConflictSets() {
        return conflictSets;
    }

    public List<DagEntry> getEntries(List<HASH> want, int queryBatchSize, DSLContext queryPool) {
        throw new IllegalStateException("Unknown nodes cannot be queried");
    }

    public AvalancheParameters getParameters() {
        return parameters;
    }

    public NavigableMap<HashKey, Node> getUnfinalized() {
        return unfinalized;
    }

    public List<DagEntry> getUnfinalized(List<HashKey> keys) {
        return keys.stream()
                   .map(key -> unfinalized.get(key))
                   .filter(e -> e != null)
                   .map(node -> node.getDagEntry())
                   .collect(Collectors.toList());
    }

    public Set<Node> getUnknown() {
        return unknown;
    }

    public BlockingDeque<HashKey> getUnqueried() {
        return unqueried;
    }

    public List<HashKey> getWanted(int max) {
        return Collections.emptyList();
    }

    public HashKey insert(DagEntry entry, HashKey conflictSet, long discovered, DSLContext context) {
        byte[] serialized = serialize(entry);
        HashKey key = new HashKey(hashOf(serialized));
        conflictSet = (entry.getLinks() == null || entry.getLinks().isEmpty()) ? GENESIS_CONFLICT_SET
                       : conflictSet == null ? key : conflictSet;
        insert(key, entry, serialized, entry.getDescription() == null, discovered, conflictSet, context);
        return key;
    }

    public HashKey insert(DagEntry entry, long discovered, DSLContext context) {
        return insert(entry, null, discovered, context);
    }

    public List<HashKey> insert(List<DagEntry> entries, long discovered, DSLContext context) {
        return entries.stream().map(entry -> insert(entry, discovered, context)).collect(Collectors.toList());
    }

    public boolean isFinalized(HashKey key, DSLContext context) {
        try {
            return finalizedCache.get(key, () -> context.fetchExists(DAG, DAG.KEY.eq(key.bytes())));
        } catch (ExecutionException e) {
            LoggerFactory.getLogger(WorkingSet.class).error("unable to query DAG", e);
            return false;
        }
    }

    public boolean isStronglyPreferred(HashKey key, DSLContext context) {
        return isStronglyPreferred(Collections.singletonList(key), context).get(0);
    }

    public List<Boolean> isStronglyPreferred(List<HashKey> keys, DSLContext context) {
        return keys.stream().map(key -> {
            Node node = unfinalized.get(key);
            return node != null ? node.isStronglyPreferred() : isFinalized(key, context);
        }).collect(Collectors.toList());
    }

    public void prefer(Collection<HashKey> keys) {
        keys.stream().map(key -> unfinalized.get(key)).filter(node -> node != null).forEach(node -> node.prefer());
    }

    public void prefer(HashKey key) {
        prefer(Collections.singletonList(key));
    }

    public List<HashKey> preferred(Random entropy) {
        List<HashKey> sample = unfinalized.values()
                                          .stream()
                                          .filter(node -> node.isPreferred())
                                          .map(node -> node.getKey())
                                          .collect(Collectors.toList());
        Collections.shuffle(sample, entropy);
        return sample;
    }

    public List<HashKey> query(int maxSize) {
        List<HashKey> query = new ArrayList<>();
        for (int i = 0; i < maxSize; i++) {
            HashKey key;
            try {
                key = i == 0 ? unqueried.poll(1, TimeUnit.MILLISECONDS) : unqueried.poll(200, TimeUnit.MICROSECONDS);
            } catch (InterruptedException e) {
                return query;
            }
            if (key == null) {
                break;
            }
            query.add(key);
        }
        return query;
    }

    public int sampleNoOpParents(Collection<HashKey> collector, Random entropy, DSLContext context) {
        List<HashKey> sample = singularFrontier(entropy);
        if (sample.isEmpty()) {
            sample = frontier(entropy);
        }
        if (sample.isEmpty()) {
            sample = unfinalizedSingular(entropy);
        }
        if (sample.isEmpty()) {
            sample = preferred(entropy);
        }
        sample.forEach(e -> collector.add(e));
        return sample.size();
    }

    public int sampleParents(Collection<HashKey> collector, Random entropy, DSLContext context) {
        List<HashKey> sample = singularFrontier(entropy);
        if (sample.isEmpty()) {
            sample = frontier(entropy);
        }
        if (sample.isEmpty()) {
            sample = unfinalizedSingular(entropy);
        }
        if (sample.isEmpty()) {
            sample = preferred(entropy);
        }
        if (sample.isEmpty()) {
            sample = finalized(context);
        }
        sample.forEach(e -> collector.add(e));
        return sample.size();
    }

    public List<HashKey> sampleParents(Random random, DSLContext context) {
        List<HashKey> collector = new ArrayList<>();
        sampleParents(collector, random, context);
        return collector;
    }

    public List<HashKey> singularFrontier(Random entropy) {
        List<HashKey> sample = unfinalized.values()
                                          .stream()
                                          .filter(node -> node.isPreferredAndSingular(parameters.beta1 / 1))
                                          .map(node -> node.getKey())
                                          .collect(Collectors.toList());
        Collections.shuffle(sample, entropy);
        return sample;
    }

    public FinalizationData tryFinalize(Collection<HashKey> keys, DSLContext context) {
        Set<Node> finalizedSet = new TreeSet<>();
        Set<Node> visited = new TreeSet<>();
        keys.stream()
            .map(key -> unfinalized.get(key))
            .filter(node -> node != null)
            .forEach(node -> node.tryFinalize(finalizedSet, visited));

        if (finalizedSet.isEmpty()) {
            return new FinalizationData();
        }

        Table<Record> allFinalized = DSL.table("FINALIZED");
        Field<byte[]> allFinalizedHash = DSL.field("HASH", byte[].class, allFinalized);
        context.execute("create cached local temporary table if not exists  FINALIZED(HASH binary(32) unique)");

        BatchBindStep batch = context.batch(context.insertInto(allFinalized, allFinalizedHash).values((byte[]) null));
        finalizedSet.forEach(node -> {
            final HashKey key = node.getKey();
            finalizedCache.put(key, true);
            unqueried.remove(key);
            batch.bind(key.bytes());
        });
        batch.execute();

        int finalized = context.mergeInto(DAG, DAG.KEY, DAG.DATA, DAG.LINKS)
                               .key(DAG.KEY)
                               .select(context.select(UNFINALIZED.HASH, UNFINALIZED.DATA, UNFINALIZED.LINKS)
                                              .from(UNFINALIZED)
                                              .join(allFinalized)
                                              .on((DSL.field("FINALIZED.HASH", byte[].class).eq(UNFINALIZED.HASH))))
                               .execute();

        context.deleteFrom(UNFINALIZED)
               .where(UNFINALIZED.HASH.in(context.select(allFinalizedHash).from(allFinalized)))
               .execute();

        context.deleteFrom(allFinalized).execute();

        if (finalized != finalizedSet.size()) {
            System.out.println("Finalization differs " + finalized + " != " + finalizedSet.size());
        }

        FinalizationData data = new FinalizationData();
        finalizedSet.stream().filter(node -> node instanceof KnownNode).forEach(node -> {
            final ConflictSet conflictSet = node.getConflictSet();
            conflictSets.remove(conflictSet.getKey());
            conflictSet.getLosers().forEach(loser -> {
                data.deleted.add(loser.getKey());
                loser.delete();
            });
            final HashKey key = node.getKey();
            data.finalized.add(key);
            unfinalized.remove(key);
        });
        return data;
    }

    public FinalizationData tryFinalize(HashKey key, DSLContext context) {
        return tryFinalize(Collections.singletonList(key), context);
    }

    public List<HashKey> unfinalizedSingular(Random entropy) {
        List<HashKey> sample = unfinalized.values()
                                          .stream()
                                          .filter(node -> node.isUnfinalizedSingular())
                                          .map(node -> node.getKey())
                                          .collect(Collectors.toList());
        Collections.shuffle(sample, entropy);
        return sample;
    }

    /** for testing **/
    void finalize(HashKey key, DSLContext context) {
        Node node = unfinalized.remove(key);
        if (node != null) {
            node.snip();
        }
        context.insertInto(DAG, DAG.KEY, DAG.DATA, DAG.LINKS)
               .select(context.select(UNFINALIZED.HASH, UNFINALIZED.DATA, UNFINALIZED.LINKS)
                              .from(UNFINALIZED)
                              .where((UNFINALIZED.HASH.eq(key.bytes()))))
               .execute();
        context.deleteFrom(UNFINALIZED).where(UNFINALIZED.HASH.eq(key.bytes())).execute();
    }

    /** for testing **/
    Node get(HashKey key) {
        return unfinalized.get(key);
    }

    void insert(HashKey key, byte[] entry, List<HASH> yeLinks, DSLContext context) {
        byte[] links = null;
        if (yeLinks != null) {
            ByteBuffer linkBuf = ByteBuffer.allocate(32 * yeLinks.size());
            for (HASH hash : yeLinks) {
                linkBuf.put(hash.bytes());
            }
            links = linkBuf.array();
        }

        context.mergeInto(UNFINALIZED, UNFINALIZED.HASH, UNFINALIZED.DATA, UNFINALIZED.LINKS)
               .key(UNFINALIZED.HASH)
               .values(key.bytes(), entry, links)
               .execute();
    }

    void insert(HashKey key, DagEntry entry, byte[] serialized, boolean noOp, long discovered, HashKey cs,
                DSLContext context) {
        unfinalized.compute(key, (k, v) -> {
            if (v != null) {
                if (v instanceof UnknownNode) {
                    Node replacement = nodeFor(k, entry, noOp, discovered, cs, context);
                    ((UnknownNode) v).replaceWith(replacement);
                    insert(key, serialize(entry), entry.getLinks(), context);
                    unqueried.add(k);
                    return replacement;
                }
                return v;
            }

            try {
                if (finalizedCache.get(k, () -> context.fetchExists(DAG, DAG.KEY.eq(k.bytes())))) {
                    return null;
                }
            } catch (ExecutionException e) {
                LoggerFactory.getLogger(WorkingSet.class).error("unable query DAG", e.getCause());
            }

            Node node = nodeFor(k, entry, noOp, discovered, cs, context);

            insert(key, serialize(entry), entry.getLinks(), context);
            unqueried.add(k);
            return node;
        });
    }

    Set<Node> linksOf(DagEntry entry, long discovered, DSLContext context) {
        List<HASH> links = entry.getLinks();
        return links == null ? Collections.emptySet()
                : links.stream()
                       .map(link -> new HashKey(link))
                       .map(link -> resolve(link, discovered, context))
                       .filter(node -> node != null)
                       .collect(Collectors.toCollection(ConcurrentSkipListSet::new));
    }

    Node nodeFor(HashKey k, DagEntry entry, boolean noOp, long discovered, HashKey cs, DSLContext context) {
        return noOp ? new NoOpNode(k, entry, linksOf(entry, discovered, context), discovered)
                : new KnownNode(k, entry, linksOf(entry, discovered, context), cs, discovered);
    }

    Node resolve(HashKey key, long discovered, DSLContext context) {
        return unfinalized.computeIfAbsent(key, k -> {
            try {
                if (finalizedCache.get(key, () -> context.fetchExists(DAG, DAG.KEY.eq(key.bytes())))) {
                    return null;
                }
            } catch (ExecutionException e) {
                LoggerFactory.getLogger(WorkingSet.class).error("error in finalization cache lookup", e.getCause());
            }
            Node node = new UnknownNode(k, discovered);
            unknown.add(node);
            return node;
        });
    }
}
