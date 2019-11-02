/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.avalanche;

import static com.salesforce.apollo.dagwood.schema.Tables.CONFLICTSET;
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
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jooq.BatchBindStep;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Table;
import org.jooq.impl.DSL;

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
        private volatile boolean  finalized  = false;
        private final Set<Node>   links;

        public KnownNode(HashKey key, Set<Node> links, HashKey cs, long discovered) {
            super(key, discovered);
            this.conflictSet = conflictSets.computeIfAbsent(cs, k -> new ConflictSet(this));
            this.links = links;
            this.closure = new TreeSet<>();

            for (Node node : links) {
                assert !node.isNoOp() : "Cannot have NoOps as parent links";
                node.addDependent(this);
                closure.addAll(node.closure());
            }
        }

        @Override
        public void addClosureTo(Set<Node> closureSet) {
            closure.add(this);
            closure.addAll(links);
            closure.addAll(closureSet);
        }

        @Override
        public Set<Node> closure() {
            return closure;
        }

        @Override
        public boolean finalized() {
            final boolean wasFinalized = finalized;
            finalized = true;
            return wasFinalized;
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
        public boolean isFinalized() {
            final boolean isFinalized = finalized;
            return isFinalized;
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
            closure.remove(unknownNode);
            replacement.addClosureTo(closure);
        }

        @Override
        public void snip() {
            dependents.forEach(node -> {
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
            final boolean isFinalized = finalized;
            if (isFinalized) {
                return true;
            }
            final int currentConfidence = confidence;
            if (currentConfidence >= parameters.beta1 && conflictSet.getCardinality() == 1
                    && conflictSet.getPreferred().equals(this)) {
                if (links.stream().map(node -> {
                    if (visited.add(node)) {
                        return node.tryFinalize(finalizedSet, visited);
                    }
                    return node.isFinalized();
                }).filter(success -> success).count() == links.size()) {
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
                return false;
            }
        }
    }

    public static abstract class Node implements Comparable<Node> {
        protected final Set<Node> dependents = new TreeSet<>();
        protected final long      discovered;
        protected final HashKey   key;

        public Node(HashKey key, long discovered) {
            this.key = key;
            this.discovered = discovered;
        }

        public abstract void addClosureTo(Set<Node> closure);

        public void addDependent(Node node) {
            dependents.add(this);
        }

        abstract public Set<Node> closure();

        @Override
        public int compareTo(Node o) {
            return getKey().compareTo(o.getKey());
        }

        public abstract boolean finalized();

        abstract public int getConfidence();

        public ConflictSet getConflictSet() {
            return null;
        }

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

        public NoOpNode(HashKey key, Set<Node> links, long discovered) {
            super(key, discovered);
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
        public Set<Node> closure() {
            return closure;
        }

        @Override
        public boolean finalized() {
            throw new IllegalStateException("No op nodes cannot be finalized");
        }

        @Override
        public int getConfidence() {
            return 0;
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

    public static class UnknownNode extends Node {

        private final Set<Node>  dependencies = new ConcurrentSkipListSet<>();
        private volatile boolean finalized;

        public UnknownNode(HashKey key, long discovered) {
            super(key, discovered);
        }

        @Override
        public void addClosureTo(Set<Node> closure) {
            throw new IllegalStateException("Unknown nodes are not replacements");
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
        public boolean finalized() {
            return false;
        }

        @Override
        public int getConfidence() {
            return 0;
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
        public boolean tryFinalize(Set<Node> finalizedSet, Set<Node> visited) {
            return finalized;
        }
    }

    public static final HashKey                      GENESIS_CONFLICT_SET = new HashKey(new byte[32]);
    private final NavigableMap<HashKey, ConflictSet> conflictSets         = new ConcurrentSkipListMap<>();
    private final AvalancheParameters                parameters;
    private final NavigableMap<HashKey, Node>        unfinalized          = new ConcurrentSkipListMap<>();

    private final Set<Node> unknown = new ConcurrentSkipListSet<>();

    private final BlockingDeque<HashKey> unqueried = new LinkedBlockingDeque<>();

    public WorkingSet(AvalancheParameters parameters) {
        this.parameters = parameters;
    }

    public Stream<HashKey> frontierSample(DSLContext create) {
        // TODO Auto-generated method stub
        return null;
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

    public Stream<HashKey> getNeglectedFrontier(DSLContext create) {
        // TODO Auto-generated method stub
        return null;
    }

    public Set<Node> getUnknown() {
        return unknown;
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

    public boolean isFinalized(HashKey key, DSLContext context) {
        return context.fetchExists(DAG, DAG.KEY.eq(key.bytes()));
    }

    public boolean isStronglyPreferred(HashKey hashKey, DSLContext using) {
        // TODO Auto-generated method stub
        return false;
    }

    public List<HashKey> isStronglyPreferred(List<HashKey> asList, DSLContext create) {
        // TODO Auto-generated method stub
        return null;
    }

    public void prefer(Collection<HashKey> keys) {
        keys.stream().map(key -> unfinalized.get(key)).filter(node -> node != null).forEach(node -> node.prefer());
    }

    public void prefer(HashKey key) {
        prefer(Collections.singletonList(key));
    }

    public List<HASH> query(int maxSize) {
        List<HASH> query = new ArrayList<>();
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
            query.add(key.toHash());
        }
        return query;
    }

    public int sampleParents(Collection<HashKey> collector, DSLContext context) {
        return 0;
    }

    public List<HashKey> sampleParents(DSLContext context) {
        List<HashKey> collector = new ArrayList<>();
        sampleParents(collector, context);
        return collector;
    }

    public List<Node> select(Predicate<Node> selector) {
        return unfinalized.values().parallelStream().filter(node -> selector.test(node)).collect(Collectors.toList());
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
            HashKey key = node.getKey();
            unqueried.remove(key);
            batch.bind(key.bytes());
        });
        batch.execute();

        context.insertInto(DAG, DAG.KEY, DAG.DATA, DAG.LINKS)
               .select(context.select(UNFINALIZED.HASH, UNFINALIZED.DATA, UNFINALIZED.LINKS)
                              .from(UNFINALIZED)
                              .join(allFinalized)
                              .on((DSL.field("FINALIZED.HASH", byte[].class).eq(UNFINALIZED.HASH))))
               .execute();

        context.deleteFrom(UNFINALIZED)
               .where(UNFINALIZED.HASH.in(context.select(allFinalizedHash).from(allFinalized)))
               .execute();

        context.deleteFrom(allFinalized).execute();
        FinalizationData data = new FinalizationData();
        finalizedSet.forEach(node -> data.finalized.add(node.getKey()));
        return data;
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

        context.deleteFrom(CONFLICTSET)
               .where(CONFLICTSET.NODE.in(context.select(UNFINALIZED.CONFLICTSET)
                                                 .from(UNFINALIZED)
                                                 .where((UNFINALIZED.HASH.eq(key.bytes())))))
               .execute();
        context.deleteFrom(UNFINALIZED).where(UNFINALIZED.HASH.eq(key.bytes())).execute();
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
        context.insertInto(UNFINALIZED, UNFINALIZED.HASH, UNFINALIZED.DATA, UNFINALIZED.LINKS)
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

            if (context.fetchExists(DAG, DAG.KEY.eq(k.bytes()))) {
                return null;
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
                       .collect(Collectors.toCollection(TreeSet::new));
    }

    Node nodeFor(HashKey k, DagEntry entry, boolean noOp, long discovered, HashKey cs, DSLContext context) {
        return noOp ? new NoOpNode(k, linksOf(entry, discovered, context), discovered)
                : new KnownNode(k, linksOf(entry, discovered, context), cs, discovered);
    }

    Node resolve(HashKey key, long discovered, DSLContext context) {
        if (context.fetchExists(DAG, DAG.KEY.eq(key.bytes()))) {
            return null;
        }
        return unfinalized.computeIfAbsent(key, k -> {
            Node node = new UnknownNode(k, discovered);
            unknown.add(node);
            return node;
        });
    }
}
