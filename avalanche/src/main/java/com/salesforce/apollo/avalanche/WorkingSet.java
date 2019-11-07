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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        public void topologicalSort(Map<HashKey, DagInsert> set, Set<DagInsert> visited, Stack<DagInsert> stack) {
            if (!visited.add(this)) {
                return;
            }

            if (dagEntry.getLinks() == null) {
                return;
            }

            dagEntry.getLinks().forEach(link -> {
                DagInsert n = set.get(new HashKey(link.bytes()));
                if (n != null && !visited.contains(n)) {
                    n.topologicalSort(set, visited, stack);
                }
            });
            stack.push(this);
        }
    }

    public static class FinalizationData {
        public final Set<HashKey> deleted   = new TreeSet<>();
        public final Set<HashKey> finalized = new TreeSet<>();
    }

    public class KnownNode extends MaterializedNode {
        private volatile int                confidence = 0;
        private final ConflictSet           conflictSet;
        private final Set<MaterializedNode> dependents = ConcurrentHashMap.newKeySet();
        private volatile boolean            finalized  = false;

        public KnownNode(HashKey key, byte[] entry, Set<Node> links, HashKey cs, long discovered) {
            super(key, entry, links, discovered);
            conflictSet = conflictSets.computeIfAbsent(cs, k -> new ConflictSet(k, this));
            conflictSet.add(this);
        }

        @Override
        public void addDependent(MaterializedNode node) {
            dependents.add(node);
        }

        @Override
        public void delete() {
            // TODO Auto-generated method stub

        }

        @Override
        public Collection<MaterializedNode> dependents() {
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
        public boolean isComplete() {
            return traverseClosure(node -> !node.isUnknown());
        }

        @Override
        public boolean isFinalized() {
            final boolean isFinalized = finalized;
            return isFinalized;
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
        public Boolean calculateIsStronglyPreferred() {
            if (conflictSet.getPreferred() != this) {
                return false;
            }
            return super.calculateIsStronglyPreferred();
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

        public void replace(UnknownNode node) {
            node.replaceWith(this);
            confidence = sumChits();
        }

        @Override
        public void snip() {
            unfinalized.remove(key);
            dependents.forEach(node -> {
                node.snip(this);
            });
            super.snip();
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
            if (currentConfidence >= parameters.beta1 && conflictSet.getCardinality() == 1 && preferred) {
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
            } else if (conflictSet.getCounter() >= parameters.beta2 && preferred) {
                finalized = true;
                finalizedSet.add(this);
                traverseClosure(node -> {
                    node.markFinalized();
                    finalizedSet.add(node);
                    return true;
                });
                return true;
            } else {
                links.forEach(node -> node.tryFinalize(finalizedSet, visited));
                return false;
            }
        }

        @Override
        public void invalidateCachedIsp() {
            super.invalidateCachedIsp();
            dependents.forEach(node -> node.invalidateCachedIsp());
        }
    }

    abstract public static class MaterializedNode extends Node {
        private volatile boolean   cachedSP            = false;
        protected volatile boolean chit                = false;
        private volatile Boolean   isStronglyPreferred = null;
        protected final Set<Node>  links;
        private final byte[]       entry;

        public MaterializedNode(HashKey key, byte[] entry, Set<Node> links, long discovered) {
            super(key, discovered);
            this.entry = entry;
            this.links = links;
        }

        public void invalidateCachedIsp() {
            cachedSP = false;
            isStronglyPreferred = null;
        }

        abstract public void addDependent(MaterializedNode node);

        @Override
        public void delete() {
            // TODO Auto-generated method stub

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

        public abstract Collection<MaterializedNode> dependents();

        @Override
        public boolean getChit() {
            return chit;
        }

        @Override
        public byte[] getEntry() {
            return entry;
        }

        @Override
        public boolean isComplete() {
            return traverseClosure(node -> !node.isUnknown());
        }

        @Override
        public Set<Node> links() {
            return links;
        }

        @Override
        public void replace(UnknownNode unknownNode, Node replacement) {
            links.remove(unknownNode);
            links.add(replacement);
            replacement.addDependent(this);
        }

        @Override
        public void snip() {
            traverseClosure(node -> {
                node.snip();
                return true;
            });
            links.clear();
        }

        @Override
        public Boolean isPreferred() {
            boolean current = cachedSP;
            if (current) {
                Boolean currentIsp = isStronglyPreferred;
                return currentIsp;
            }
            return calculateIsPreferred();
        }

        protected abstract Boolean calculateIsPreferred();

        @Override
        public Boolean isStronglyPreferred() {
            boolean current = cachedSP;
            if (current) {
                Boolean currentIsp = isStronglyPreferred;
                return currentIsp;
            }
            synchronized (this) {
                Boolean stronglyPreferred = calculateIsStronglyPreferred();
                isStronglyPreferred = stronglyPreferred;
                cachedSP = true;
                return stronglyPreferred;
            }
        }

        public Boolean calculateIsStronglyPreferred() {
            return traverseClosure(node -> node.isPreferred(), node -> markStronglyPreferred());
        }

        private void markStronglyPreferred() {
            synchronized (this) {
                isStronglyPreferred = true;
                cachedSP = true;
            }
        }

        @Override
        public int sumChits() {
            return dependents().stream().mapToInt(node -> node.sumChits()).sum() + (chit ? 1 : 0);
        }

        @Override
        public boolean tryFinalize(Set<Node> finalizedSet, Set<Node> visited) {
            links.forEach(node -> node.tryFinalize(finalizedSet, visited));
            return true;
        }
    }

    public static abstract class Node {
        protected final long    discovered;
        protected final HashKey key;

        public Node(HashKey key, long discovered) {
            this.key = key;
            this.discovered = discovered;
        }

        abstract public void delete();

        public boolean getChit() {
            return false;
        }

        abstract public void addDependent(MaterializedNode node);

        abstract public int getConfidence();

        public ConflictSet getConflictSet() {
            throw new IllegalStateException(getClass().getSimpleName() + " do not have conflict sets");
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

        public Set<Node> links() {
            return Collections.emptySet();
        }

        abstract public void markFinalized();

        public void markPreferred() {
        }

        abstract public void prefer();

        abstract public void replace(UnknownNode node);

        public abstract void replace(UnknownNode unknownNode, Node replacement);

        abstract public void snip();

        public void snip(Node node) {
            links().remove(node);
        }

        public int sumChits() {
            return 0;
        }

        public Boolean traverseClosure(Function<Node, Boolean> p) {
            return traverseClosure(p, null);
        }

        public Boolean traverseClosure(Function<Node, Boolean> p, Consumer<Node> post) {
            return traverseClosure(p, post, new HashSet<>());
        }

        public Boolean traverseClosure(Function<Node, Boolean> p, Consumer<Node> post, Set<Node> visited) {
            Stack<Node> stack = new Stack<>();
            stack.push(this);

            for (Node node : links()) {
                if (visited.add(node)) {
                    Boolean result = p.apply(node);
                    if (result == null) {
                        return null;
                    }
                    if (!result) {
                        return false;
                    }
                    node.traverseClosure(p, post, visited);
                }
            }
            if (post != null) {
                post.accept(this);
            }
            return true;
        }

        abstract public boolean tryFinalize(Set<Node> finalizedSet, Set<Node> visited);
    }

    public static class NoOpNode extends MaterializedNode {

        public NoOpNode(HashKey key, byte[] entry, Set<Node> links, long discovered) {
            super(key, entry, links, discovered);
        }

        @Override
        public void addDependent(MaterializedNode node) {
            throw new IllegalStateException("No ops cannot be parents");
        }

        @Override
        public void delete() {
            // TODO Auto-generated method stub

        }

        @Override
        public Collection<MaterializedNode> dependents() {
            return Collections.emptySet();
        }

        @Override
        public int getConfidence() {
            return 0;
        }

        @Override
        public boolean isComplete() {
            return traverseClosure(node -> !node.isUnknown());
        }

        @Override
        protected Boolean calculateIsPreferred() {
            throw new IllegalStateException("Should never query NoOp in closure");
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
        public Set<Node> links() {
            return links;
        }

        @Override
        public void markFinalized() {
            throw new IllegalStateException("NoOps cannot be finalized");
        }

        @Override
        public void replace(UnknownNode node) {
            // no dependents
        }

        @Override
        public void replace(UnknownNode unknownNode, Node replacement) {
            links.remove(unknownNode);
            links.add(replacement);
        }

        @Override
        public String toString() {
            return "NoOp [" + key + "]";
        }
    }

    public class UnknownNode extends Node {

        private final Set<MaterializedNode> dependencies = ConcurrentHashMap.newKeySet();

        private volatile boolean finalized;

        public UnknownNode(HashKey key, long discovered) {
            super(key, discovered);
        }

        @Override
        public void addDependent(MaterializedNode node) {
            dependencies.add(node);
        }

        @Override
        public void delete() {
            // TODO Auto-generated method stub

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
            return finalized;
        }

        @Override
        public Boolean isPreferred() {
            boolean current = finalized;
            if (current) {
                return true;
            }
            log.trace("failed to prefer because unknown in closure");
            return null;
        }

        @Override
        public Boolean isStronglyPreferred() {
            boolean current = finalized;
            if (current) {
                return true;
            }
            log.info("failed to prefer because querying unknown");
            return null;
        }

        @Override
        public boolean isUnknown() {
            return true;
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
        public void replace(UnknownNode node) {
            throw new IllegalStateException("Unknown nodes cannot be replaced!");
        }

        @Override
        public void replace(UnknownNode unknownNode, Node replacement) {
            throw new IllegalStateException("Unknown nodes do not have children");
        }

        public void replaceWith(Node replacement) {
            log.trace("replacing: " + replacement.getKey());
            dependencies.forEach(node -> node.replace(this, replacement));
        }

        @Override
        public void snip() {
            unfinalized.remove(key);
            dependencies.forEach(node -> {
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
        public boolean tryFinalize(Set<Node> finalizedSet, Set<Node> visited) {
            return finalized;
        }
    }

    public static final HashKey GENESIS_CONFLICT_SET = new HashKey(new byte[32]);

    public static Logger                             log          = LoggerFactory.getLogger(WorkingSet.class);
    private final NavigableMap<HashKey, ConflictSet> conflictSets = new ConcurrentSkipListMap<>();
    private final DagWood                            finalized;
    private final ReentrantLock                      lock         = new ReentrantLock(true);
    private final AvaMetrics                         metrics;
    private final AvalancheParameters                parameters;
    private final NavigableMap<HashKey, Node>        unfinalized  = new ConcurrentSkipListMap<>();
    private final Set<HashKey>                       unknown      = new ConcurrentSkipListSet<>();
    private final BlockingDeque<HashKey>             unqueried    = new LinkedBlockingDeque<>();

    public WorkingSet(AvalancheParameters parameters, DagWood wood, AvaMetrics metrics) {
        this.parameters = parameters;
        finalized = wood;
        this.metrics = metrics;
    }

    public Set<byte[]> finalized() {
        return finalized.keySet();
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

    public DagEntry getDagEntry(HashKey key) {
        final Node node = unfinalized.get(key);
        if (node != null) {
            return manifestDag(node.getEntry());
        }
        return manifestDag(finalized.get(key.bytes()));
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
        return keys.stream().map(key -> unfinalized.get(key)).filter(node -> node != null).filter(node -> {
            if (node.isComplete()) {
                return true;
            }
            unqueried.add(node.getKey());
            return false;
        }).map(node -> ByteBuffer.wrap(node.getEntry())).collect(Collectors.toList());
    }

    public NavigableMap<HashKey, Node> getUnfinalized() {
        return unfinalized;
    }

    public Set<HashKey> getUnknown() {
        return unknown;
    }

    public BlockingDeque<HashKey> getUnqueried() {
        return unqueried;
    }

    public Set<HashKey> getWanted() {
        return unknown;
    }

    public List<HashKey> getWanted(Random entropy) {
        List<HashKey> wanted = new ArrayList<>(unknown);
        Collections.shuffle(wanted, entropy);
        return wanted;
    }

    public HashKey insert(DagEntry entry, HashKey conflictSet, long discovered) {
        byte[] serialized = serialize(entry);
        HashKey key = new HashKey(hashOf(serialized));
        conflictSet = (entry.getLinks() == null || entry.getLinks().isEmpty()) ? GENESIS_CONFLICT_SET
                       : conflictSet == null ? key : conflictSet;
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
        return transactions.stream().map(e -> e.array()).map(t -> {
            HashKey key = new HashKey(hashOf(t));
            DagEntry entry = manifestDag(t);
            HashKey conflictSet = (entry.getLinks() == null || entry.getLinks().isEmpty()) ? GENESIS_CONFLICT_SET : key;
            insert(key, entry, t, entry.getDescription() == null, discovered, conflictSet);
            return key;
        }).collect(Collectors.toList());
    }

    public List<HashKey> insertSerializedRaw(List<byte[]> transactions, long discovered) {
        return transactions.stream().map(t -> {
            HashKey key = new HashKey(hashOf(t));
            DagEntry entry = manifestDag(t);
            HashKey conflictSet = (entry.getLinks() == null || entry.getLinks().isEmpty()) ? GENESIS_CONFLICT_SET : key;
            insert(key, entry, t, entry.getDescription() == null, discovered, conflictSet);
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
                return finalized.cacheContainsKey(key.bytes()) ? true : null;
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

    public void queueUnqueried(HashKey key) {
        unqueried.add(key);
    }

    public int sampleNoOpParents(Collection<HashKey> collector, Random entropy) {
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
        sample.forEach(e -> collector.add(e));
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

                                          .filter(node -> node.isPreferredAndSingular(parameters.beta1 / 1))
                                          .map(node -> node.getKey())
                                          .collect(Collectors.toList());
        Collections.shuffle(sample, entropy);
        return sample;
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
        lock.lock();
        try {
            finalizedSet.stream().forEach(node -> {
                finalize(node, data);
            });
        } finally {
            lock.unlock();
        }
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
        HashKey key = node.getKey();
        final ReentrantLock l = lock;
        l.lock();
        final ConflictSet conflictSet = node.getConflictSet();
        try {
            conflictSets.remove(conflictSet.getKey());
            finalized.put(key.bytes(), node.getEntry());
            node.snip();
        } finally {
            l.unlock();
        }
        conflictSet.getLosers().forEach(loser -> {
            data.deleted.add(loser.getKey());
            loser.delete();
        });
        data.finalized.add(key);
    }

    /** for testing **/
    Node get(HashKey key) {
        return unfinalized.get(key);
    }

    void insert(HashKey key, DagEntry entry, byte[] serialized, boolean noOp, long discovered, HashKey cs) {
        log.trace("inserting: {}", key);
        final ReentrantLock l = lock;
        l.lock(); // sux, but without this, we get dup's which is teh bad.
        try {
            final Node found = unfinalized.get(key);
            if (found == null) {
                if (finalized.containsKey(key.bytes())) {
                    return;
                }
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
                return;

            }
            if (found.isUnknown()) {
                Node replacement = nodeFor(key, serialized, entry, noOp, discovered, cs);
                unfinalized.put(key, replacement);
                replacement.replace(((UnknownNode) found));
                unqueried.add(key);
                unknown.remove(key);
                if (metrics != null) {
                    metrics.getUnknownReplacementRate().mark();
                    metrics.getUnknown().decrementAndGet();
                }
            }
        } finally {
            l.unlock();
        }
    }

    Set<Node> linksOf(DagEntry entry, long discovered) {
        List<HASH> links = entry.getLinks();
        return links == null ? Collections.emptySet()
                : links.stream()
                       .map(link -> new HashKey(link))
                       .map(link -> resolve(link, discovered))
                       .filter(node -> node != null)
                       .collect(Collectors.toCollection(() -> ConcurrentHashMap.newKeySet()));
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
}
