/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.avalanche;

import static com.salesforce.apollo.protocols.Conversion.hashOf;
import static com.salesforce.apollo.protocols.Conversion.manifestDag;
import static com.salesforce.apollo.protocols.Conversion.serialize;
import static java.util.concurrent.ConcurrentHashMap.newKeySet;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.proto.DagEntry;
import com.salesforce.apollo.avalanche.Avalanche.Finalized;
import com.salesforce.apollo.protocols.HashKey;

/**
 * Manages the unfinalized, working set of the Apollo DAG.
 * 
 * @author hhildebrand
 *
 */
public class WorkingSet {
    public static class DagInsert {
        public final HashKey  conflictSet;
        public final DagEntry dagEntry;
        public final byte[]   entry;
        public final HashKey  key;
        public final boolean  noOp;

        public DagInsert(HashKey key, DagEntry dagEntry, byte[] entry, HashKey conflictSet, boolean noOp) {
            this.key = key;
            this.dagEntry = dagEntry;
            this.entry = entry;
            this.conflictSet = conflictSet;
            this.noOp = noOp;
        }

        public void topologicalSort(Map<HashKey, DagInsert> set, Set<DagInsert> visited, List<DagInsert> stack) {
            if (!visited.add(this)) {
                return;
            }

            if (dagEntry.getLinksList() == null) {
                return;
            }

            dagEntry.getLinksList().forEach(link -> {
                DagInsert n = set.get(new HashKey(link.toByteArray()));
                if (n != null && !visited.contains(n)) {
                    n.topologicalSort(set, visited, stack);
                }
            });
            stack.add(this);
        }
    }

    public static class FinalizationData {
        public final Set<HashKey>   deleted   = new HashSet<>();
        public final Set<Finalized> finalized = new HashSet<>();
    }

    public class KnownNode extends MaterializedNode {
        private volatile int                 confidence = 0;
        private final ConflictSet            conflictSet;
        private final List<MaterializedNode> dependents = new ArrayList<>();
        private volatile boolean             finalized  = false;

        public KnownNode(HashKey key, byte[] entry, ArrayList<Node> links, HashKey cs, long discovered) {
            super(key, entry, links, discovered);
            conflictSet = conflictSets.computeIfAbsent(cs, k -> new ConflictSet(k, this));
            conflictSet.add(this);
        }

        @Override
        public void addDependent(MaterializedNode node) {
            synchronized (this) {
                dependents.add(node);
            }
        }

        @Override
        public Boolean calculateIsPreferred() {
            final boolean current = finalized;
            if (current) {
                return true;
            }
            final boolean preferred = conflictSet.getPreferred() == this;
            if (!preferred) {
                log.info("Not preferred conflict: {}  !=  {}", System.identityHashCode(conflictSet.getPreferred()),
                         System.identityHashCode(this));
            }
            return preferred;
        }

        @Override
        public Boolean calculateIsStronglyPreferred() {
            if (conflictSet.getPreferred() != this) {
                return false;
            }
            return super.calculateIsStronglyPreferred();
        }

        @Override
        public List<MaterializedNode> dependents() {
            return dependents;
        }

        @Override
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
        public boolean isFinalized() {
            final boolean isFinalized = finalized;
            return isFinalized;
        }

        @Override
        public boolean isFrontier() {
            return dependents.isEmpty();
        }

        @Override
        public boolean isPreferred(int maxConfidence) {
            final int current = confidence;
            return current <= maxConfidence && conflictSet.getPreferred() == this;
        }

        @Override
        public boolean isPreferredAndSingular(int maximumConfidence) {
            final boolean current = finalized;
            final int conf = confidence;
            return !current && conf <= maximumConfidence && conflictSet.getPreferred() == this
                    && conflictSet.getCardinality() == 1;
        }

        @Override
        public Boolean isStronglyPreferred() {
            final boolean current = finalized;
            if (current) {
                return true;
            }
            return super.isStronglyPreferred();
        }

        @Override
        public boolean isUnfinalizedSingular() {
            final boolean current = finalized;
            return !current && conflictSet.getCardinality() == 1 && conflictSet.getPreferred() == this;
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
        public Stream<Node> parents(int depth, Predicate<Node> filter) {
            if (depth <= 0) {
                throw new IllegalArgumentException("depth must be >= 1 : " + depth);
            }
            if (depth == 1) {
                return links().stream().filter(filter);
            }
            return links().stream().flatMap(node -> node.parents(depth - 1, filter));
        }

        @Override
        public void replace(UnknownNode node) {
            node.replaceWith(this);
            confidence = sumChits();
        }

        @Override
        public String toString() {
            return "Known [" + key + "]";
        }

        @Override
        public boolean tryFinalize(Set<Node> finalizedSet, Set<Node> visited) {
            final boolean isFinalized = finalized;
            if (!visited.add(this)) {
                return isFinalized;
            }
            if (isFinalized) {
                return true;
            }
            final int currentConfidence = confidence;
            final boolean preferred = conflictSet.getPreferred() == this;
            if (conflictSet.getCounter() >= parameters.core.beta2 && preferred) {
                if (!isComplete()) {
                    return false;
                }
                finalized = true;
                finalizedSet.add(this);
                traverseClosure(node -> {
                    node.markFinalized();
                    finalizedSet.add(node);
                    return true;
                });
                return true;
            } else if (currentConfidence >= parameters.core.beta1 && conflictSet.getCardinality() == 1 && preferred) {
                synchronized (this) {
                    if (links.stream()
                             .map(node -> node.tryFinalize(finalizedSet, visited))
                             .filter(success -> success)
                             .count() == links.size()) {
                        finalizedSet.add(this);
                        finalized = true;
                        return true;
                    }
                }
                final boolean current = finalized;
                return current;
            } else {
                synchronized (this) {
                    links.forEach(node -> node.tryFinalize(finalizedSet, visited));
                }
                return false;
            }
        }
    }

    abstract public class MaterializedNode extends Node {
        protected volatile boolean      chit = false;
        protected final ArrayList<Node> links;
        private final byte[]            entry;
        private volatile Result         isStronglyPreferred;

        public MaterializedNode(HashKey key, byte[] entry, ArrayList<Node> links, long discovered) {
            super(key, discovered);
            this.entry = entry;
            this.links = links;
            synchronized (this) {
                links.forEach(e -> e.addDependent(this));
            }
        }

        public Boolean calculateIsStronglyPreferred() {
            final Boolean test = traverseClosure(node -> node.isPreferred(), node -> node.markStronglyPreferred());
            if (test == null) {
                isStronglyPreferred = Result.UNKNOWN;
            } else {
                isStronglyPreferred = test ? Result.TRUE : Result.FALSE;
            }
            return test;
        }

        @Override
        public List<MaterializedNode> dependents() {
            return Collections.emptyList();
        }

        @Override
        public boolean getChit() {
            return chit;
        }

        @Override
        public byte[] getEntry() {
            return entry;
        }

        public void invalidate() {
            isStronglyPreferred = null;
            synchronized (this) {
                dependents().forEach(e -> e.invalidate());
            }
        }

        @Override
        public boolean isComplete() {
            return traverseClosure(node -> !node.isUnknown());
        }

        @Override
        public Boolean isPreferred() {
            final Result test = isStronglyPreferred;
            if (test != null) {
                return test.value();
            }
            return calculateIsPreferred();
        }

        @Override
        public Boolean isStronglyPreferred() {
            final Result test = isStronglyPreferred;
            if (test != null) {
                return test.value();
            }
            return calculateIsStronglyPreferred();
        }

        @Override
        public List<Node> links() {
            return links;
        }

        @Override
        public void markStronglyPreferred() {
            isStronglyPreferred = Result.TRUE;
        }

        @Override
        public void prefer() {
            chit = true;
            markPreferred();
            traverseClosure(node -> {
                node.markPreferred();
                return true;
            });
        }

        @Override
        public void replace(UnknownNode unknownNode, Node replacement) {
            synchronized (this) {
                log.trace("(found) replacing {} with {}", unknownNode.getKey(), replacement.getKey());
                if (links.remove(unknownNode)) {
                    links.add(replacement);
                    replacement.addDependent(this);
                }
            }
        }

        @Override
        public void snip() {
            finalized.put(key.bytes(), getEntry());
            unfinalized.remove(key);
            List<Node> deps;
            synchronized (this) {
                deps = new ArrayList<>(dependents());
                dependents().clear();
            }
            deps.forEach(e -> e.snip(this));
            traverseClosure(node -> {
                node.snip();
                return true;
            });
            synchronized (this) {
                links.clear();
            }
        }

        @Override
        public void snip(Node node) {
            synchronized (this) {
                links.remove(node);
            }
        }

        public int sumChits() {
            synchronized (this) {
                return dependents().stream().mapToInt(node -> node.sumChits()).sum() + (chit ? 1 : 0);
            }
        }

        public Boolean traverseClosure(Function<Node, Boolean> p) {
            return traverseClosure(p, null);
        }

        public Boolean traverseClosure(Function<Node, Boolean> test, Consumer<Node> post) {
            List<Node> stack = new ArrayList<>();
            stack.add(this);
            Set<Node> visited = Collections.newSetFromMap(new IdentityHashMap<>(2048));

            while (!stack.isEmpty()) {
                final Node node = stack.remove(stack.size() - 1);
                final List<Node> l;
                synchronized (node) {
                    l = new ArrayList<>(node.links());
                }
                for (Node e : l) {
                    if (visited.add(e)) {
                        Boolean result = test.apply(e);
                        if (result == null) {
                            return null;
                        }
                        if (!result) {
                            return false;
                        }
                        stack.add(e);
                    }
                }
                if (post != null) {
                    post.accept(node);
                }
            }
            return true;
        }

        @Override
        public boolean tryFinalize(Set<Node> finalizedSet, Set<Node> visited) {
            synchronized (this) {
                links.forEach(node -> node.tryFinalize(finalizedSet, visited));
            }
            return true;
        }

        protected abstract Boolean calculateIsPreferred();
    }

    public static abstract class Node {
        protected final long    discovered;
        protected final HashKey key;

        public Node(HashKey key, long discovered) {
            this.key = key;
            this.discovered = discovered;
        }

        abstract public void addDependent(MaterializedNode node);

        abstract public List<MaterializedNode> dependents();

        public boolean getChit() {
            return false;
        }

        abstract public int getConfidence();

        public ConflictSet getConflictSet() {
            throw new IllegalStateException(getClass().getSimpleName() + "s do not have conflict sets");
        }

        public long getDiscovered() {
            return discovered;
        }

        abstract public byte[] getEntry();

        public HashKey getKey() {
            return key;
        }

        abstract public boolean isComplete();

        abstract public boolean isFinalized();

        public boolean isFrontier() {
            return false;
        }

        public boolean isKnown() {
            return false;
        }

        public boolean isNoOp() {
            return false;
        }

        abstract public Boolean isPreferred();

        public boolean isPreferred(int maxConfidence) {
            return false;
        }

        public boolean isPreferredAndSingular(int maximumConfidence) {
            return false;
        }

        abstract public Boolean isStronglyPreferred();

        public boolean isUnfinalizedSingular() {
            return false;
        }

        public boolean isUnknown() {
            return false;
        }

        public List<Node> links() {
            return Collections.emptyList();
        }

        abstract public void markFinalized();

        public void markPreferred() {
        }

        abstract public void markStronglyPreferred();

        public Stream<Node> parents(int depth, Predicate<Node> filter) {
            throw new UnsupportedOperationException("not applicable to " + getClass().getSimpleName());
        }

        abstract public void prefer();

        abstract public void replace(UnknownNode node);

        public abstract void replace(UnknownNode unknownNode, Node replacement);

        abstract public void snip();

        abstract public void snip(Node node);

        abstract public boolean tryFinalize(Set<Node> finalizedSet, Set<Node> visited);
    }

    public class NoOpNode extends MaterializedNode {

        public NoOpNode(HashKey key, byte[] entry, ArrayList<Node> links, long discovered) {
            super(key, entry, links, discovered);
        }

        @Override
        public void addDependent(MaterializedNode node) {
            throw new IllegalStateException("No ops cannot be parents");
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
        public void replace(UnknownNode node) {
            node.replaceWith(this);
        }

        @Override
        public String toString() {
            return "NoOp [" + key + "]";
        }

        @Override
        protected Boolean calculateIsPreferred() {
            throw new IllegalStateException("Should never query NoOp in closure");
        }
    }

    public class UnknownNode extends Node {

        private final List<MaterializedNode> dependents = new ArrayList<>();

        public UnknownNode(HashKey key, long discovered) {
            super(key, discovered);
        }

        @Override
        public void addDependent(MaterializedNode node) {
            synchronized (this) {
                dependents.add(node);
            }
        }

        @Override
        public List<MaterializedNode> dependents() {
            return dependents;
        }

        @Override
        public int getConfidence() {
            return 0;
        }

        @Override
        public byte[] getEntry() {
            return null;
        }

        @Override
        public boolean isComplete() {
            return false;
        }

        @Override
        public boolean isFinalized() {
            return false;
        }

        @Override
        public Boolean isPreferred() {
            log.trace("node is unknown, not preferred");
            return null;
        }

        @Override
        public Boolean isStronglyPreferred() {
            log.info("{} failed to strongly prefer because querying unknown", key);
            return null;
        }

        @Override
        public boolean isUnknown() {
            return true;
        }

        @Override
        public void markFinalized() {
            throw new IllegalStateException("Unknown nodes cannot be finalized");
        }

        @Override
        public void markStronglyPreferred() {
        }

        @Override
        public void prefer() {
            throw new IllegalStateException("Unknown nodes cannot be preferred");
        }

        @Override
        public void replace(UnknownNode node) {
            throw new IllegalStateException("Unknown nodes cannot be replaced!");
        }

        @Override
        public void replace(UnknownNode unknownNode, Node replacement) {
            throw new IllegalStateException("Unknown nodes do not have children");
        }

        public void replaceWith(Node replacement) {
            log.trace("replacing: " + replacement.getKey());
            synchronized (this) {
                dependents.forEach(node -> {
                    node.replace(this, replacement);
                    node.invalidate();
                });
            }
        }

        @Override
        public void snip() {
            synchronized (this) {
                dependents.forEach(node -> {
                    node.snip(this);
                });
            }
        }

        @Override
        public void snip(Node node) {
            throw new IllegalStateException("Unknown nodes should never be dependents");
        }

        @Override
        public String toString() {
            return "Unknown [" + key + "]";
        }

        @Override
        public boolean tryFinalize(Set<Node> finalizedSet, Set<Node> visited) {
            return false;
        }
    }

    private static enum Result {
        FALSE {

            @Override
            Boolean value() {
                return Boolean.FALSE;
            }
        },
        TRUE {

            @Override
            Boolean value() {
                return Boolean.TRUE;
            }
        },
        UNKNOWN {

            @Override
            Boolean value() {
                return null;
            }
        };

        abstract Boolean value();
    }

    public static final HashKey          GENESIS_CONFLICT_SET = new HashKey(new byte[32]);
    public static Logger                 log                  = LoggerFactory.getLogger(WorkingSet.class);
    private static final ArrayList<Node> EMPTY_ARRAY_LIST     = new ArrayList<>();

    private final Map<HashKey, ConflictSet> conflictSets = new ConcurrentHashMap<>();
    private final DagWood                   finalized;
    private final ReentrantLock             lock         = new ReentrantLock(true);
    private final AvalancheMetrics          metrics;
    private final AvalancheParameters       parameters;
    private final Processor                 processor;
    private final Map<HashKey, Node>        unfinalized  = new ConcurrentHashMap<>();
    private final Set<HashKey>              unknown      = newKeySet();
    private final BlockingDeque<HashKey>    unqueried    = new LinkedBlockingDeque<>();

    public WorkingSet(Processor processor, AvalancheParameters parameters, DagWood wood, AvalancheMetrics metrics) {
        this.parameters = parameters;
        finalized = wood;
        this.metrics = metrics;
        this.processor = processor;
    }

    public List<HashKey> allFinalized() {
        return finalized.allFinalized();
    }

    public Collection<byte[]> finalized() {
        List<byte[]> l = new ArrayList<>();
        unfinalized.values()
                   .stream()
                   .filter(node -> node.isFinalized())
                   .filter(e -> l.size() < 100)
                   .map(node -> node.getKey())
                   .forEach(e -> l.add(e.bytes()));
        if (!l.isEmpty()) {
            return l;
        }
        for (byte[] key : finalized.keySet()) {
            l.add(key);
            if (l.size() > 100) {
                return l;
            }
        }
        return l;
    }

    public List<HashKey> frontier() {
        return unfinalized.values()
                          .stream()
                          .filter(node -> node.isFrontier())
                          .map(node -> node.getKey())
                          .collect(Collectors.toList());
    }

    public List<HashKey> frontier(Random entropy) {
        List<HashKey> sample = unfinalized.values()
                                          .stream()
                                          .filter(node -> node.isPreferred(parameters.core.beta1 - 1))
                                          .map(node -> node.getKey())
                                          .collect(Collectors.toList());
        Collections.shuffle(sample, entropy);
        return sample;
    }

    public List<HashKey> frontierForNoOp(Random entropy) {
        List<HashKey> sample = unfinalized.values()
                                          .stream()
                                          .filter(node -> node.isPreferred(parameters.core.beta2 - 1))
                                          .map(node -> node.getKey())
                                          .collect(Collectors.toList());
        Collections.shuffle(sample, entropy);
        return sample;
    }

    /** for testing **/
    public Node get(HashKey key) {
        return unfinalized.get(key);
    }

    public ConflictSet getConflictSet(HashKey key) {
        Node node = unfinalized.get(key);
        if (node == null) {
            return null;
        }
        return node.getConflictSet();
    }

    public DagEntry getDagEntry(HashKey key) {
        final Node node = unfinalized.get(key);
        if (node != null) {
            return manifestDag(node.getEntry());
        }
        byte[] entry = finalized.get(key.bytes());
        return entry == null ? null : manifestDag(entry);
    }

    public List<ByteBuffer> getEntries(List<HashKey> collect) {
        return collect.stream().map(key -> {
            Node n = unfinalized.get(key);
            if (n == null) {
                byte[] entry = finalized.get(key.bytes());
                return entry == null ? null : entry;
            }
            return n.getEntry();
        }).filter(n -> n != null).map(e -> ByteBuffer.wrap(e)).collect(Collectors.toList());
    }

    public DagWood getFinalized() {
        return finalized;
    }

    public AvalancheParameters getParameters() {
        return parameters;
    }

    public List<ByteBuffer> getQuerySerializedEntries(List<HashKey> keys) {
        List<ByteBuffer> entries = keys.stream()
                                       .map(key -> getBytes(key))
                                       .filter(entry -> entry != null)
                                       .map(entry -> ByteBuffer.wrap(entry))
                                       .collect(Collectors.toList());
        for (ByteBuffer entry : entries) {
            assert entry.hasRemaining() : "Whoops!";
        }
        return entries;
    }

    public Map<HashKey, Node> getUnfinalized() {
        return unfinalized;
    }

    public BlockingDeque<HashKey> getUnqueried() {
        return unqueried;
    }

    public Set<HashKey> getWanted() {
        return unknown;
    }

    public HashKey insert(DagEntry entry, HashKey conflictSet, long discovered) {
        byte[] serialized = serialize(entry);
        try {
            assert entry.equals(DagEntry.parseFrom(serialized)) : "Something lost in translation";
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException(e);
        }
        HashKey key = new HashKey(hashOf(serialized));
        conflictSet = entry.getLinksCount() == 0 ? GENESIS_CONFLICT_SET : conflictSet == null ? key : conflictSet;
        if (conflictSet.equals(GENESIS_CONFLICT_SET)) {
            assert new HashKey(entry.getDescription()).equals(GENESIS_CONFLICT_SET) : "Not in the genesis set";
        }
        insert(key, entry, serialized, entry.getDescription() == null, discovered, conflictSet);
        return key;
    }

    public HashKey insert(DagEntry entry, long discovered) {
        return insert(entry, null, discovered);
    }

    public List<HashKey> insert(List<DagEntry> entries, long discovered) {
        return entries.stream().map(entry -> insert(entry, discovered)).collect(Collectors.toList());
    }

    public List<HashKey> insertSerialized(List<ByteBuffer> transactions, long discovered) {
        return insertSerializedRaw(transactions.stream().map(e -> e.array()).collect(Collectors.toList()), discovered);
    }

    public List<HashKey> insertSerializedRaw(List<byte[]> transactions, long discovered) {
        return transactions.parallelStream().map(t -> {
            assert t.length > 0 : "whoopsie";
            HashKey key = new HashKey(hashOf(t));
            Node node = unfinalized.get(key);
            if (node == null || node.isUnknown()) {
                DagEntry entry = manifestDag(t);
                HashKey conflictSet = entry.getLinksCount() == 0 ? GENESIS_CONFLICT_SET
                        : processor.conflictSetOf(key, entry);
                if (conflictSet.equals(GENESIS_CONFLICT_SET)) {
                    assert new HashKey(entry.getDescription()).equals(GENESIS_CONFLICT_SET) : "Not in the genesis set";
                }
                insert(key, entry, t, entry.getDescription() == null, discovered, conflictSet);
            }
            return key;
        }).collect(Collectors.toList());
    }

    public boolean isFinalized(HashKey key) {
        return finalized.containsKey(key.bytes());
    }

    public Boolean isStronglyPreferred(HashKey key) {
        return isStronglyPreferred(Collections.singletonList(key)).get(0);
    }

    public List<Boolean> isStronglyPreferred(List<HashKey> keys) {
        return keys.stream().map((Function<? super HashKey, ? extends Boolean>) key -> {
            Node node = unfinalized.get(key);
            if (node == null) {
                final Boolean isFinalized = finalized.cacheContainsKey(key.bytes()) ? true : null;
                if (isFinalized == null) {
                    unknown.add(key);
                }
                return isFinalized;
            }
            return node.isStronglyPreferred();

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
                                          .filter(node -> node.isKnown())
                                          .filter(node -> node.isPreferred())
                                          .map(node -> node.getKey())
                                          .collect(Collectors.toList());
        Collections.shuffle(sample, entropy);
        return sample;
    }

    public void purgeNoOps() {
        unfinalized.values()
                   .stream()
                   .filter(e -> e.isNoOp())
                   .map(e -> (NoOpNode) e)
                   .filter(e -> e.links().isEmpty())
                   .forEach(e -> {
                       unfinalized.remove(e.getKey());
                       if (metrics != null) {
                           metrics.purgeNoOps().mark();
                       }
                   });
    }

    public List<HashKey> query(int maxSize) {
        List<HashKey> query = new ArrayList<>();
        for (int i = 0; i < maxSize; i++) {
            HashKey key;
            try {
                key = unqueried.poll(200, TimeUnit.MICROSECONDS);
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

    public void queueUnqueried(HashKey key) {
        unqueried.add(key);
    }

    public Deque<HashKey> sampleNoOpParents(Random entropy) {
        Deque<HashKey> sample = new ArrayDeque<>();

        sample.addAll(singularNoOpFrontier(entropy));

        if (sample.isEmpty()) {
            sample.addAll(unfinalizedSingular(entropy));
        }
        if (sample.isEmpty()) {
            sample.addAll(preferred(entropy));
        }
        return sample;
    }

    public int sampleParents(Collection<HashKey> collector, Random entropy) {
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
            sample = new ArrayList<>(finalized().stream().map(e -> new HashKey(e)).collect(Collectors.toList()));
        }
        collector.addAll(sample);
        return sample.size();
    }

    public List<HashKey> sampleParents(Random random) {
        List<HashKey> collector = new ArrayList<>();
        sampleParents(collector, random);
        return collector;
    }

    public List<HashKey> singularFrontier(Random entropy) {
        List<HashKey> sample = unfinalized.values()
                                          .stream()
                                          .filter(node -> node.isPreferredAndSingular(parameters.core.beta1 / 2 - 1))
                                          .map(node -> node.getKey())
                                          .collect(Collectors.toList());
        Collections.shuffle(sample, entropy);
        return sample;
    }

    public List<HashKey> singularNoOpFrontier(Random entropy) {
        List<HashKey> sample = unfinalized.values()
                                          .stream()
                                          .filter(node -> node.isPreferredAndSingular(parameters.core.beta2 - 1))
                                          .map(node -> node.getKey())
                                          .collect(Collectors.toList());
        Collections.shuffle(sample, entropy);
        return sample;
    }

    public void traverseAll(BiConsumer<HashKey, DagEntry> p) {
        unfinalized.entrySet()
                   .stream()
                   .filter(e -> !e.getValue().isFinalized())
                   .forEach(e -> p.accept(e.getKey(), manifestDag(e.getValue().getEntry())));
        finalized.keySet().stream().map(e -> new HashKey(e)).forEach(e -> p.accept(e, getDagEntry(e)));
    }

    public FinalizationData tryFinalize(Collection<HashKey> keys) {
        Set<Node> finalizedSet = new HashSet<>();
        Set<Node> visited = new HashSet<>();
        keys.stream()
            .map(key -> unfinalized.get(key))
            .filter(node -> node != null)
            .forEach(node -> node.tryFinalize(finalizedSet, visited));

        if (finalizedSet.isEmpty()) {
            return new FinalizationData();
        }
        FinalizationData data = new FinalizationData();
        finalizedSet.stream().forEach(node -> {
            finalize(node, data);
        });
        return data;
    }

    public FinalizationData tryFinalize(HashKey key) {
        return tryFinalize(Collections.singletonList(key));
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
    void finalize(HashKey key) {
        Node node = unfinalized.get(key);
        if (node == null) {
            return;
        }
        finalize(node, new FinalizationData());
    }

    void finalize(Node node, FinalizationData data) {
        node.snip();
        if (!node.isUnknown()) {
            final ConflictSet conflictSet = node.getConflictSet();
            conflictSets.remove(conflictSet.getKey());
            conflictSet.getLosers().forEach(loser -> {
                data.deleted.add(loser.getKey());
                unfinalized.remove(loser.getKey());
            });
            data.finalized.add(new Finalized(node.getKey(), node.getEntry()));
        }
    }

    void insert(HashKey key, DagEntry entry, byte[] serialized, boolean noOp, long discovered, HashKey cs) {
        final ReentrantLock l = lock;
        l.lock(); // sux, but without this, we get dup's which is teh bad.
        try {
            final Node found = unfinalized.get(key);
            if (found == null) {
                if (!finalized.containsKey(key.bytes())) {
                    Node node = nodeFor(key, serialized, entry, noOp, discovered, cs);
                    unfinalized.put(key, node);
                    unqueried.add(key);
                    if (unknown.remove(key)) {
                        if (metrics != null) {
                            metrics.getUnknownReplacementRate().mark();
                            metrics.getUnknown().decrementAndGet();
                        }
                    }
                    if (metrics != null) {
                        metrics.getInputRate().mark();
                    }
                }
            } else if (found.isUnknown()) {
                Node replacement = nodeFor(key, serialized, entry, noOp, discovered, cs);
                unfinalized.put(key, replacement);
                replacement.replace(((UnknownNode) found));
                unknown.remove(key);
                unqueried.add(key);
                if (metrics != null) {
                    metrics.getUnknownReplacementRate().mark();
                    metrics.getUnknown().decrementAndGet();
                }
            }
        } finally {
            l.unlock();
        }
    }

    ArrayList<Node> linksOf(DagEntry entry, long discovered) {
        return entry.getLinksCount() == 0 ? EMPTY_ARRAY_LIST
                : entry.getLinksList()
                       .stream()
                       .map(link -> new HashKey(link))
                       .map(link -> resolve(link, discovered))
                       .filter(node -> node != null)
                       .collect(Collectors.toCollection(ArrayList::new));
    }

    Node nodeFor(HashKey k, byte[] entry, DagEntry dagEntry, boolean noOp, long discovered, HashKey cs) {
        return noOp ? new NoOpNode(k, entry, linksOf(dagEntry, discovered), discovered)
                : new KnownNode(k, entry, linksOf(dagEntry, discovered), cs, discovered);
    }

    Node resolve(HashKey key, long discovered) {
        Node exist = unfinalized.get(key);
        if (exist == null) {
            if (finalized.containsKey(key.bytes())) {
                return null;
            }
            exist = new UnknownNode(key, discovered);
            unfinalized.put(key, exist);
            if (metrics != null) {
                metrics.getUnknownLinkRate().mark();
                metrics.getUnknown().incrementAndGet();
            }
            unknown.add(key);
        }
        return exist;
    }

    private byte[] getBytes(HashKey key) {
        final Node node = unfinalized.get(key);
        if (node != null) {
            if (!node.isComplete()) {
                queueUnqueried(node.getKey());
                return null;
            } else {
                return node.getEntry();
            }
        }
        byte[] bs = finalized.get(key.bytes());
        assert bs.length > 0 : "invalid stored for: " + key;
        return bs;
    }
}
