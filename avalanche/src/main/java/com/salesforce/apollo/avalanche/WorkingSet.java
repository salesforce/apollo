/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.avalanche;

import static com.salesforce.apollo.protocols.Conversion.hashOf;
import static com.salesforce.apollo.protocols.Conversion.manifestDag;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.math3.random.BitsStreamGenerator;
import org.apache.commons.math3.util.Pair;
import org.h2.mvstore.MVMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.proto.DagEntry;
import com.salesfoce.apollo.proto.DagEntry.EntryType;
import com.salesfoce.apollo.proto.ID;
import com.salesforce.apollo.avalanche.Avalanche.Finalized;
import com.salesforce.apollo.membership.ReservoirSampler;
import com.salesforce.apollo.protocols.Conversion;
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
                DagInsert n = set.get(new HashKey(link.toByteString()));
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

        public KnownNode(HashKey key, DagEntry entry, ArrayList<Node> links, HashKey cs, long discovered) {
            super(key, entry, links, discovered);
            conflictSet = conflictSets.computeIfAbsent(cs, k -> new ConflictSet(k, this));
            conflictSet.add(this);
        }

        @Override
        public void addDependent(MaterializedNode node) {
            dependents.add(node);
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
        public boolean tryFinalize(Set<Node> finalizedSet, List<Node> visited) {
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
            } else {
                links.forEach(node -> node.tryFinalize(finalizedSet, visited));
                return false;
            }
        }
    }

    abstract public class MaterializedNode extends Node {
        protected volatile boolean      chit = false;
        protected final ArrayList<Node> links;
        private final DagEntry          entry;
        private volatile Result         isStronglyPreferred;

        public MaterializedNode(HashKey key, DagEntry entry, ArrayList<Node> links, long discovered) {
            super(key, discovered);
            this.entry = entry;
            this.links = links;
            links.forEach(e -> e.addDependent(this));
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
        public DagEntry getEntry() {
            return entry;
        }

        public void invalidate() {
            isStronglyPreferred = null;
            dependents().forEach(e -> e.invalidate());
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
            log.trace("(found) replacing {} with {}", unknownNode.getKey(), replacement.getKey());
            if (links.remove(unknownNode)) {
                links.add(replacement);
                replacement.addDependent(this);
            }
        }

        @Override
        public void snip() {
            List<Node> stack = new ArrayList<>();
            List<Node> traversed = new ArrayList<>(1024);
            stack.add(this);
            traversed.add(this);
            read(() -> {
                while (!stack.isEmpty()) {
                    final Node node = stack.remove(stack.size() - 1);
                    List<Node> linkz = node.links();
                    for (int i = 0; i < linkz.size(); i++) {
                        Node e = linkz.get(i);
                        if (e.mark()) {
                            traversed.add(e);
                            stack.add(e);
                        }
                    }
                }
            });
            for (int i = 0; i < traversed.size(); i++) {
                Node node = traversed.get(i);
                write(() -> node.excise());
                if (!node.isUnknown()) {
                    finalized.put(key, node.getEntry().toByteArray());
                }
            }
        }

        @Override
        public void excise() {
            links.clear();
            unfinalized.remove(key);
            dependents().forEach(e -> e.snip(this));
            dependents().clear();
        }

        @Override
        public void snip(Node node) {
            links.remove(node);
        }

        public int sumChits() {
            return dependents().stream().mapToInt(node -> node.sumChits()).sum() + (chit ? 1 : 0);
        }

        public Boolean traverseClosure(Function<Node, Boolean> p) {
            return traverseClosure(p, null);
        }

        public Boolean traverseClosure(Function<Node, Boolean> test, Consumer<Node> post) {
            List<Node> stack = new ArrayList<>();
            List<Node> traversed = new ArrayList<>(1024);
            stack.add(this);
            try {
                while (!stack.isEmpty()) {
                    final Node node = stack.remove(stack.size() - 1);
                    List<Node> linkz = node.links();
                    for (int i = 0; i < linkz.size(); i++) {
                        Node e = linkz.get(i);
                        if (e.mark()) {
                            Boolean result = test.apply(e);
                            if (result == null) {
                                return null;
                            }
                            if (!result) {
                                return false;
                            }
                            traversed.add(e);
                            stack.add(e);
                        }

                    }
                    if (post != null) {
                        post.accept(node);
                    }
                }
                return true;
            } finally {
                for (int i = 0; i < traversed.size(); i++) {
                    traversed.get(i).unmark();
                }
            }
        }

        @Override
        public boolean tryFinalize(Set<Node> finalizedSet, List<Node> visited) {
            links.forEach(node -> node.tryFinalize(finalizedSet, visited));
            return true;
        }

        protected abstract Boolean calculateIsPreferred();
    }

    public static abstract class Node {
        protected final long    discovered;
        protected final HashKey key;
        private boolean         marked = false;

        public Node(HashKey key, long discovered) {
            this.key = key;
            this.discovered = discovered;
        }

        public void excise() {
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

        abstract public DagEntry getEntry();

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

        /**
         * Mark the receiver.
         * 
         * @return true if the receiver was unmarked, false if previously marked.
         */
        public boolean mark() {
            if (!marked) {
                marked = true;
                return true;
            }
            return false;
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

        abstract public boolean tryFinalize(Set<Node> finalizedSet, List<Node> visited);

        public void unmark() {
            marked = false;
        }
    }

    public class NoOpNode extends MaterializedNode {

        public NoOpNode(HashKey key, DagEntry entry, ArrayList<Node> links, long discovered) {
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
            dependents.add(node);
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
        public DagEntry getEntry() {
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
            dependents.forEach(node -> {
                node.replace(this, replacement);
                node.invalidate();
            });
        }

        @Override
        public void excise() {
            dependents.forEach(node -> {
                node.snip(this);
            });
        }

        @Override
        public void snip() {
            dependents.forEach(node -> {
                node.snip(this);
            });
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
        public boolean tryFinalize(Set<Node> finalizedSet, List<Node> visited) {
            return false;
        }
    }

    public static final HashKey          GENESIS_CONFLICT_SET = new HashKey(new byte[32]);
    public static Logger                 log                  = LoggerFactory.getLogger(WorkingSet.class);
    private static final ArrayList<Node> EMPTY_ARRAY_LIST     = new ArrayList<>();

    private final Map<HashKey, ConflictSet> conflictSets = new HashMap<>();
    private final MVMap<HashKey, byte[]>    finalized;
    private final AvalancheMetrics          metrics;
    private final AvalancheParameters       parameters;
    private final Processor                 processor;
    private final ReadWriteLock             rwLock       = new ReentrantReadWriteLock();
    private final Map<HashKey, Node>        unfinalized  = new HashMap<>();
    private final Set<HashKey>              unknown      = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final BlockingDeque<HashKey>    unqueried    = new LinkedBlockingDeque<>();

    public WorkingSet(Processor processor, AvalancheParameters parameters, MVMap<HashKey, byte[]> wood,
            AvalancheMetrics metrics) {
        this.parameters = parameters;
        finalized = wood;
        this.metrics = metrics;
        this.processor = processor;
    }

    public Iterator<HashKey> allFinalized() {
        return finalized.keyIterator(HashKey.ORIGIN);
    }

    public Collection<HashKey> finalized(BitsStreamGenerator entropy, int max) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(finalized.keyIterator(HashKey.ORIGIN),
                                                                        Spliterator.ORDERED),
                                    false)
                            .collect(new ReservoirSampler<>(null, max, entropy));
    }

    public int finalizedCount() {
        return finalized.size();
    }

    public List<HashKey> frontier() {
        return read(() -> {
            return unfinalized.values()
                              .stream()
                              .filter(node -> node.isFrontier())
                              .map(node -> node.getKey())
                              .collect(Collectors.toList());
        });
    }

    public List<HashKey> frontier(BitsStreamGenerator entropy, int max) {
        return read(() -> {
            List<HashKey> sample = unfinalized.values()
                                              .stream()
                                              .filter(node -> node.isPreferred(parameters.core.beta1 - 1))
                                              .map(node -> node.getKey())
                                              .collect(new ReservoirSampler<HashKey>(null, max, entropy));
            return sample;
        });
    }

    public List<HashKey> frontierForNoOp(BitsStreamGenerator entropy) {
        return read(() -> {
            List<HashKey> sample = unfinalized.values()
                                              .stream()
                                              .filter(node -> node.isPreferred(parameters.core.beta2 - 1))
                                              .map(node -> node.getKey())
                                              .collect(new ReservoirSampler<HashKey>(null, 100, entropy));
            return sample;
        });
    }

    /** for testing **/
    public Node get(HashKey key) {
        return read(() -> {
            return unfinalized.get(key);
        });
    }

    public ConflictSet getConflictSet(HashKey key) {
        return read(() -> {
            Node node = unfinalized.get(key);
            if (node == null) {
                return null;
            }
            return node.getConflictSet();
        });
    }

    public DagEntry getDagEntry(HashKey key) {
        return read(() -> {
            final Node node = unfinalized.get(key);
            if (node != null) {
                return node.getEntry();
            }
            byte[] entry = finalized.get(key);
            return entry == null ? null : manifestDag(entry);
        });
    }

    public List<ByteString> getEntries(List<HashKey> collect) {
        return read(() -> {
            return collect.stream().map(key -> {
                Node n = unfinalized.get(key);
                if (n == null) {
                    byte[] entry = finalized.get(key);
                    return entry == null ? null : ByteString.copyFrom(entry);
                }
                DagEntry entry = n.getEntry();
                return entry == null ? null : entry.toByteString();
            }).filter(n -> n != null).collect(Collectors.toList());
        });
    }

    public int getFinalizedCount() {
        return read(() -> {
            return finalized.size();
        });
    }

    public AvalancheParameters getParameters() {
        return parameters;
    }

    public List<Pair<HashKey, ByteString>> getQuerySerializedEntries(List<HashKey> keys) {
        return keys.stream().map(key -> {
            ByteString bytes = read(() -> getBytes(key));
            return bytes == null ? null : new Pair<>(key, bytes);
        }).filter(entry -> entry != null).collect(Collectors.toList());
    }

    public Map<HashKey, Node> getUnfinalized() {
        return unfinalized;
    }

    public BlockingDeque<HashKey> getUnqueried() {
        return unqueried;
    }

    public Collection<HashKey> getWanted(BitsStreamGenerator secureRandom, int max) {
        return unknown.stream().collect(new ReservoirSampler<>(null, max, secureRandom));
    }

    public HashKey insert(DagEntry entry, HashKey cs, long discovered) {
        HashKey key = new HashKey(hashOf(entry.toByteString()));
        HashKey conflictSet = entry.getLinksCount() == 0 ? GENESIS_CONFLICT_SET : cs == null ? key : cs;
        if (conflictSet.equals(GENESIS_CONFLICT_SET)) {
            assert entry.getDescription().equals(EntryType.GENSIS) : "Not in the genesis set: " + key + " links: "
                    + entry.getLinksCount() + " description: " + entry.getDescription() + " calculated: " + conflictSet
                    + " supplied: " + cs;
        }
        insert(key, entry, entry.getDescription() == EntryType.NO_OP, discovered, conflictSet);
        return key;
    }

    public HashKey insert(DagEntry entry, long discovered) {
        return insert(entry, null, discovered);
    }

    public List<HashKey> insert(List<DagEntry> entries, long discovered) {
        return entries.stream().map(entry -> insert(entry, discovered)).collect(Collectors.toList());
    }

    public List<HashKey> insertSerialized(List<ID> hashes, List<ByteString> transactions, long discovered) {
        List<HashKey> keys = new ArrayList<>();
        for (int i = 0; i < hashes.size(); i++) {
            HashKey key = new HashKey(hashes.get(i));
            keys.add(key);
            Node node = read(() -> unfinalized.get(key));
            if (node == null || node.isUnknown()) {
                ByteString t = transactions.get(i);
                DagEntry entry = manifestDag(t);
                boolean isNoOp = entry.getDescription() == EntryType.NO_OP;
                HashKey conflictSet = isNoOp ? key : entry.getLinksCount() == 0 ? GENESIS_CONFLICT_SET
                                             : processor.validate(key, entry);
                if (conflictSet.equals(GENESIS_CONFLICT_SET)) {
                    assert entry.getDescription() == EntryType.GENSIS : "Not in the genesis set";
                }
                insert(key, entry, isNoOp, discovered, conflictSet);
            }
        }
        return keys;
    }

    public boolean isFinalized(HashKey key) {
        return finalized.containsKey(key);
    }

    public Boolean isNoOp(HashKey key) {
        Node node = read(() -> unfinalized.get(key));
        return node == null ? null : node.isNoOp();
    }

    public Boolean isStronglyPreferred(HashKey key) {
        return isStronglyPreferred(Collections.singletonList(key)).get(0);
    }

    public List<Boolean> isStronglyPreferred(List<HashKey> keys) {
        return keys.stream().map((Function<? super HashKey, ? extends Boolean>) key -> {
            Node node = read(() -> unfinalized.get(key));
            if (node == null) {
                final Boolean isFinalized = finalized.containsKey(key) ? true : null;
                if (isFinalized == null) {
                    unknown.add(key);
                }
                return isFinalized;
            }
            return read(() -> node.isStronglyPreferred());
        }).collect(Collectors.toList());

    }

    public void prefer(Collection<HashKey> keys) {
        keys.stream()
            .map(key -> read(() -> unfinalized.get(key)))
            .filter(node -> node != null)
            .forEach(node -> write(() -> node.prefer()));
    }

    public void prefer(HashKey key) {
        prefer(Collections.singletonList(key));
    }

    public List<HashKey> preferred(BitsStreamGenerator entropy, int max) {
        return read(() -> {
            List<HashKey> sample = unfinalized.values()
                                              .stream()
                                              .filter(node -> node.isKnown())
                                              .filter(node -> node.isPreferred())
                                              .map(node -> node.getKey())
                                              .collect(new ReservoirSampler<HashKey>(null, max, entropy));
            return sample;
        });
    }

    public void purgeNoOps() {
        long cutoff = System.currentTimeMillis() - (5 * 1000);
        write(() -> {
            unfinalized.values()
                       .stream()
                       .filter(e -> e.isNoOp())
                       .map(e -> (NoOpNode) e)
                       .filter(e -> e.discovered <= cutoff)
                       .map(e -> e.getKey())
                       .collect(Collectors.toList())
                       .forEach(e -> {
                           unfinalized.remove(e);
                           if (metrics != null) {
                               metrics.purgeNoOps().mark();
                           }
                       });
        });
    }

    public List<HashKey> query(int maxSize) {
        List<HashKey> query = new ArrayList<>();
        unqueried.drainTo(query, maxSize);
        return query;
    }

    public void queueUnqueried(HashKey key) {
        unqueried.add(key);
    }

    public Deque<HashKey> sampleNoOpParents(BitsStreamGenerator entropy, int want) {
        return read(() -> {
            Deque<HashKey> sample = new ArrayDeque<>();

            sample.addAll(singularNoOpFrontier(entropy, want));
            log.trace("Sampled no op frontier: {}", sample.size());

            if (sample.size() < want) {
                List<HashKey> s = unfinalizedSingular(entropy, want - sample.size());
                log.trace("Sampled no op unfinalized singular: {}", s.size());
                sample.addAll(s);
            }
            if (sample.size() < want) {
                List<HashKey> s = preferred(entropy, want - sample.size());
                log.trace("Sampled no op preferred: {}", s.size());
                sample.addAll(s);
            }
            return sample;
        });
    }

    public List<HashKey> sampleParents(BitsStreamGenerator random, int max) {
        List<HashKey> collector = new ArrayList<>();
        sampleParents(collector, max, random);
        return collector;
    }

    public int sampleParents(Collection<HashKey> collector, int max, BitsStreamGenerator entropy) {
        return read(() -> {
            List<HashKey> sample = singularFrontier(entropy, max);
            if (sample.size() < max) {
                sample.addAll(frontier(entropy, max - sample.size()));
            }
            if (sample.size() < max) {
                sample.addAll(unfinalizedSingular(entropy, max - sample.size()));
            }
            if (sample.size() < max) {
                sample.addAll(preferred(entropy, max - sample.size()));
            }
            Collections.shuffle(sample);
            collector.addAll(sample);
            return sample.size();
        });
    }

    public List<HashKey> singularFrontier(BitsStreamGenerator entropy, int max) {
        return read(() -> {
            List<HashKey> sample = unfinalized.values()
                                              .stream()
                                              .filter(n -> !n.isNoOp())
                                              .filter(node -> node.isPreferredAndSingular(parameters.core.beta1 / 2
                                                      - 1))
                                              .map(node -> node.getKey())
                                              .collect(new ReservoirSampler<HashKey>(null, max, entropy));
            return sample;
        });
    }

    public List<HashKey> singularNoOpFrontier(BitsStreamGenerator entropy, int want) {
        return read(() -> {
            List<HashKey> sample = unfinalized.values()
                                              .stream()
                                              .filter(n -> !n.isNoOp())
                                              .filter(node -> node.isPreferredAndSingular(parameters.core.beta2 - 1))
                                              .map(node -> node.getKey())
                                              .collect(new ReservoirSampler<HashKey>(null, want, entropy));
            return sample;
        });
    }

    public void traverseAll(BiConsumer<HashKey, DagEntry> p) {
        read(() -> {
            unfinalized.entrySet()
                       .stream()
                       .filter(e -> !e.getValue().isFinalized())
                       .forEach(e -> p.accept(e.getKey(), e.getValue().getEntry()));
            finalized.keySet().forEach(e -> p.accept(e, getDagEntry(e)));
        });
    }

    public FinalizationData tryFinalize(Collection<HashKey> keys) {
        Set<Node> finalizedSet = new HashSet<>();
        List<Node> visited = new ArrayList<>();
        keys.stream()
            .map(key -> read(() -> unfinalized.get(key)))
            .filter(node -> node != null)
            .forEach(node -> read(() -> node.tryFinalize(finalizedSet, visited)));

        if (finalizedSet.isEmpty()) {
            return new FinalizationData();
        }
        FinalizationData data = new FinalizationData();
        finalizedSet.forEach(node -> finalize(node, data));
        return data;
    }

    public FinalizationData tryFinalize(HashKey key) {
        return tryFinalize(Collections.singletonList(key));
    }

    public List<HashKey> unfinalizedSingular(BitsStreamGenerator entropy, int max) {
        return read(() -> {
            List<HashKey> sample = unfinalized.values()
                                              .stream()
                                              .filter(node -> node.isUnfinalizedSingular())
                                              .map(node -> node.getKey())
                                              .collect(new ReservoirSampler<HashKey>(null, max, entropy));
            return sample;
        });
    }

    /** for testing **/
    void finalize(HashKey key) {
        Node node = unfinalized.get(key);
        if (node == null) {
            return;
        }
        write(() -> {
            finalize(node, new FinalizationData());
        });
    }

    void finalize(Node node, FinalizationData data) {
        if (!node.isUnknown()) {
            finalized.put(node.getKey(), node.getEntry().toByteArray());
            write(() -> {
                node.excise();
                final ConflictSet conflictSet = node.getConflictSet();
                conflictSets.remove(conflictSet.getKey());
                conflictSet.getLosers().forEach(loser -> {
                    data.deleted.add(loser.getKey());
                    unfinalized.remove(loser.getKey());
                });
            });
            data.finalized.add(new Finalized(node.getKey(), node.getEntry()));
        }
    }

    boolean insert(HashKey key, DagEntry entry, boolean noOp, long discovered, HashKey cs) {
        Node existing = read(() -> unfinalized.get(key));
        if (existing != null && !existing.isUnknown()) {
            return true;
        }

        HashKey derived = new HashKey(Conversion.hashOf(entry.toByteString()));
        if (!key.equals(derived)) {
            log.error("Key {} does not match hash {} of entry", key, derived);
            return false;
        }
        return write(() -> {
            Node found = unfinalized.get(key);
            if (found == null) {
                if (!finalized.containsKey(key)) {
                    if (unfinalized.get(key) == null) {
                        unfinalized.put(key, nodeFor(key, entry, noOp, discovered, cs));
                        unqueried.add(key);
                    }
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
                Node replacement = nodeFor(key, entry, noOp, discovered, cs);
                unfinalized.put(key, replacement);
                replacement.replace(((UnknownNode) found));
                unknown.remove(key);
                unqueried.add(key);
                if (metrics != null) {
                    metrics.getUnknownReplacementRate().mark();
                    metrics.getUnknown().decrementAndGet();
                }
            }
            return true;
        });
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

    Node nodeFor(HashKey k, DagEntry dagEntry, boolean noOp, long discovered, HashKey cs) {
        return noOp ? new NoOpNode(k, dagEntry, linksOf(dagEntry, discovered), discovered)
                : new KnownNode(k, dagEntry, linksOf(dagEntry, discovered), cs, discovered);
    }

    Node resolve(HashKey key, long discovered) {
        Node exist = unfinalized.get(key);
        if (exist == null) {
            if (finalized.containsKey(key)) {
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

    private ByteString getBytes(HashKey key) {
        final Node node = unfinalized.get(key);
        if (node != null) {
            if (!node.isComplete()) {
                queueUnqueried(node.getKey());
                return null;
            } else {
                return node.getEntry().toByteString();
            }
        }
        byte[] bs = finalized.get(key);
        return bs == null ? null : ByteString.copyFrom(bs);
    }

    private <T> T read(Callable<T> call) {
        final Lock l = rwLock.readLock();
        l.lock();
        try {
            return call.call();
        } catch (Exception e) {
            throw new IllegalStateException("Error in read lock", e);
        } finally {
            l.unlock();
        }
    }

    private void read(Runnable r) {
        final Lock l = rwLock.readLock();
        l.lock();
        try {
            r.run();
        } catch (Exception e) {
            throw new IllegalStateException("Error in read lock", e);
        } finally {
            l.unlock();
        }
    }

    @SuppressWarnings("unused")
    private <T> T write(Callable<T> call) {
        final Lock l = rwLock.writeLock();
        l.lock();
        try {
            return call.call();
        } catch (Exception e) {
            throw new IllegalStateException("Error in write lock", e);
        } finally {
            l.unlock();
        }
    }

    private void write(Runnable r) {
        final Lock l = rwLock.writeLock();
        l.lock();
        try {
            r.run();
        } catch (Exception e) {
            throw new IllegalStateException("Error in write lock", e);
        } finally {
            l.unlock();
        }
    }
}
