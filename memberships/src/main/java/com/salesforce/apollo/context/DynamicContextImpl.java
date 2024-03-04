/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.context;

import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.ReservoirSampler;
import org.apache.commons.math3.random.BitsStreamGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.salesforce.apollo.context.Context.minMajority;

/**
 * Provides a dynamic Context for Membership. Members can be active or inactive.  Context membership is dynamic and
 * members can be added or removed at any time.
 *
 * @author hal.hildebrand
 */
public class DynamicContextImpl<T extends Member> implements DynamicContext<T> {

    private static final Logger log = LoggerFactory.getLogger(DynamicContext.class);

    private final    int                              bias;
    private final    double                           epsilon;
    private final    Digest                           id;
    private final    Map<Digest, Tracked<T>>          members             = new ConcurrentSkipListMap<>();
    private final    Map<UUID, MembershipListener<T>> membershipListeners = new ConcurrentHashMap<>();
    private final    double                           pByz;
    private final    List<Ring<T>>                    rings               = new ArrayList<>();
    private volatile int                              cardinality;

    public DynamicContextImpl(Digest id, int cardinality, double pbyz, int bias) {
        this(id, cardinality, pbyz, bias, DEFAULT_EPSILON);
    }

    public DynamicContextImpl(Digest id, int cardinality, double pbyz, int bias, double epsilon) {
        this.pByz = pbyz;
        this.id = id;
        this.bias = bias;
        this.cardinality = cardinality;
        this.epsilon = epsilon;
        for (int i = 0; i < (minMajority(pByz, cardinality, epsilon, bias) * bias) + 1; i++) {
            rings.add(new Ring<>(i, this));
        }
    }

    @Override
    public void activate(Collection<T> activeMembers) {
        activeMembers.forEach(this::activate);
    }

    /**
     * Mark a member as active in the context
     */
    @Override
    public boolean activate(T m) {
        if (tracking(m).activate()) {
            membershipListeners.values().forEach(l -> {
                try {
                    l.active(m);
                } catch (Throwable e) {
                    log.error("error recoving member in listener: " + l, e);
                }
            });
            return true;
        }
        return false;
    }

    /**
     * Mark a member identified by the digest ID as active in the context
     *
     * @return true if the member was previously inactive, false if currently active
     */
    public boolean activate(Digest id) {
        var m = members.get(id);
        if (m == null) {
            throw new NoSuchElementException("No member known: " + id);
        }
        if (m.activate()) {
            membershipListeners.values().forEach(l -> {
                try {
                    l.active(m.member);
                } catch (Throwable e) {
                    log.error("error recovering member in listener: " + l, e);
                }
            });
            return true;
        }
        return false;
    }

    /**
     * Mark a member as active in the context
     */
    @Override
    public boolean activateIfMember(T m) {
        var member = members.get(m.getId());
        if (member != null && member.activate()) {
            membershipListeners.values().forEach(l -> {
                try {
                    l.active(m);
                } catch (Throwable e) {
                    log.error("error recovering member in listener: " + l, e);
                }
            });
            return true;
        }
        return false;
    }

    @Override
    public Stream<T> active() {
        return members.values().stream().filter(Tracked::isActive).map(Tracked::member);
    }

    @Override
    public int activeCount() {
        return (int) members.values().stream().filter(Tracked::isActive).count();
    }

    @Override
    public List<T> activeMembers() {
        return members.values().stream().filter(Tracked::isActive).map(Tracked::member).toList();
    }

    @Override
    public <Q extends T> void add(Collection<Q> members) {
        members.forEach(this::add);
    }

    @Override
    public boolean add(T m) {
        if (members.containsKey(m.getId())) {
            return false;
        }
        tracking(m);
        return true;
    }

    @Override
    public Stream<T> allMembers() {
        return members.values().stream().map(Tracked::member);
    }

    @Override
    public Iterable<T> betweenPredecessors(int ring, T start, T stop) {
        return ring(ring).betweenPredecessors(start, stop);
    }

    @Override
    public Iterable<T> betweenSuccessor(int ring, T start, T stop) {
        return ring(ring).betweenSuccessor(start, stop);
    }

    @Override
    public int cardinality() {
        final var c = cardinality;
        return c;
    }

    @Override
    public void clear() {
        for (Ring<T> ring : rings) {
            ring.clear();
        }
        members.clear();
    }

    @Override
    public void deregister(UUID id) {
        membershipListeners.remove(id);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if ((obj == null) || (getClass() != obj.getClass()))
            return false;
        Context<?> other = (Context<?>) obj;
        if (id == null) {
            return other.getId() == null;
        } else
            return id.equals(other.getId());
    }

    @Override
    public T findPredecessor(int ring, Digest d, Function<T, IterateResult> predicate) {
        return ring(ring).findPredecessor(d, predicate);
    }

    @Override
    public T findPredecessor(int ring, T m, Function<T, IterateResult> predicate) {
        return ring(ring).findPredecessor(m, predicate);
    }

    @Override
    public T findSuccessor(int ring, Digest d, Function<T, IterateResult> predicate) {
        return ring(ring).findSuccessor(d, predicate);
    }

    @Override
    public T findSuccessor(int ring, T m, Function<T, IterateResult> predicate) {
        return ring(ring).findSuccessor(m, predicate);
    }

    @Override
    public T getActiveMember(Digest memberID) {
        var tracked = members.get(memberID);
        return tracked == null ? null : tracked.member();
    }

    @Override
    public List<T> getAllMembers() {
        return allMembers().toList();
    }

    @Override
    public int getBias() {
        return bias;
    }

    @Override
    public double getEpsilon() {
        return epsilon;
    }

    @Override
    public Digest getId() {
        return id;
    }

    @Override
    public T getMember(Digest memberID) {
        var tracked = members.get(memberID);
        return tracked == null ? null : tracked.member();
    }

    @Override
    public Collection<T> getOffline() {
        return members.values().stream().filter(e -> !e.isActive()).map(Tracked::member).toList();
    }

    @Override
    public double getProbabilityByzantine() {
        return pByz;
    }

    @Override
    public short getRingCount() {
        return (short) rings.size();
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public Digest hashFor(T m, int ring) {
        assert ring >= 0 && ring < rings.size() : "Invalid ring: " + ring + " max: " + (rings.size() - 1);
        var tracked = members.get(m.getId());
        if (tracked == null) {
            return hashFor(m.getId(), ring);
        }
        return tracked.hash(ring);
    }

    @Override
    public boolean isActive(Digest id) {
        assert id != null;
        var member = members.get(id);
        if (member == null) {
            return false;
        }
        return member.isActive();
    }

    @Override
    public boolean isActive(T m) {
        assert m != null;
        return isActive(m.getId());
    }

    @Override
    public boolean isBetween(int ring, T predecessor, T item, T successor) {
        return ring(ring).isBetween(predecessor, item, successor);
    }

    @Override
    public boolean isMember(Digest digest) {
        return members.containsKey(digest);
    }

    @Override
    public boolean isMember(T m) {
        return members.containsKey(m.getId());
    }

    @Override
    public boolean isOffline(Digest digest) {
        assert digest != null;
        var member = members.get(digest);
        if (member == null) {
            return true;
        }
        return !member.isActive();
    }

    @Override
    public boolean isOffline(T m) {
        assert m != null;
        return isOffline(m.getId());
    }

    @Override
    public boolean isSuccessorOf(T m, Digest digest) {
        for (Ring<T> ring : rings) {
            if (m.equals(ring.successor(m))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int majority(boolean bootstrapped) {
        var majority = getRingCount() - toleranceLevel();
        if (bootstrapped) {
            return switch (totalCount()) {
                case 1, 2 -> 1;
                case 3 -> 2;
                case 4 -> 3;
                default -> majority;
            };
        } else {
            return majority;
        }
    }

    @Override
    public int memberCount() {
        return members.size();
    }

    @Override
    public <Q extends T> void offline(Collection<Q> members) {
        members.forEach(this::offline);
    }

    /**
     * Take a member offline
     */
    @Override
    public boolean offline(T m) {
        if (tracking(m).offline()) {
            membershipListeners.values().forEach(l -> {
                try {
                    l.offline(m);
                } catch (Throwable e) {
                    log.error("error sending fail to listener: " + l, e);
                }
            });
            return true;
        }
        return false;
    }

    @Override
    public int offlineCount() {
        return (int) members.values().stream().filter(e -> !e.isActive()).count();
    }

    /**
     * Take a member offline if already a member
     */
    @Override
    public void offlineIfMember(T m) {
        var member = members.get(m.getId());
        if (member != null && member.offline()) {
            membershipListeners.values().forEach(l -> {
                try {
                    l.offline(m);
                } catch (Throwable e) {
                    log.error("error offlining member in listener: " + l, e);
                }
            });
        }
    }

    @Override
    public T predecessor(int ring, Digest location) {
        return ring(ring).predecessor(location);
    }

    @Override
    public T predecessor(int ring, Digest location, Predicate<T> predicate) {
        return ring(ring).predecessor(location, predicate);
    }

    @Override
    public T predecessor(int ring, T m) {
        return ring(ring).predecessor(m);
    }

    @Override
    public T predecessor(int ring, T m, Predicate<T> predicate) {
        return ring(ring).predecessor(m, predicate);
    }

    /**
     * @return the predecessor on each ring for the provided key
     */
    @Override
    public List<T> predecessors(Digest key) {
        return predecessors(key, t -> true);
    }

    /**
     * @return the predecessor on each ring for the provided key that pass the provided predicate
     */
    @Override
    public List<T> predecessors(Digest key, Predicate<T> test) {
        List<T> predecessors = new ArrayList<>();
        for (Ring<T> ring : rings) {
            T predecessor = ring.predecessor(key, test);
            if (predecessor != null) {
                predecessors.add(predecessor);
            }
        }
        return predecessors;
    }

    /**
     * @return the predecessor on each ring for the provided key
     */
    @Override
    public List<T> predecessors(T key) {
        return predecessors(key, t -> true);
    }

    /**
     * @return the predecessor on each ring for the provided key that pass the provided predicate
     */
    @Override
    public List<T> predecessors(T key, Predicate<T> test) {
        List<T> predecessors = new ArrayList<>();
        for (Ring<T> ring : rings) {
            T predecessor = ring.predecessor(key, test);
            if (predecessor != null) {
                predecessors.add(predecessor);
            }
        }
        return predecessors;
    }

    @Override
    public Iterable<T> predecessors(int ring, Digest location) {
        return ring(ring).predecessors(location);
    }

    @Override
    public Iterable<T> predecessors(int ring, Digest location, Predicate<T> predicate) {
        return ring(ring).predecessors(location, predicate);
    }

    @Override
    public Iterable<T> predecessors(int ring, T start) {
        return ring(ring).predecessors(start);
    }

    @Override
    public Iterable<T> predecessors(int ring, T start, Predicate<T> predicate) {
        return null;
    }

    @Override
    public int rank(int ring, Digest item, Digest dest) {
        return ring(ring).rank(item, dest);
    }

    @Override
    public int rank(int ring, Digest item, T dest) {
        return ring(ring).rank(item, dest);
    }

    @Override
    public int rank(int ring, T item, T dest) {
        return ring(ring).rank(item, dest);
    }

    @Override
    public void rebalance() {
        rebalance(members.size());
    }

    @Override
    public void rebalance(int newCardinality) {
        this.cardinality = Math.max(bias + 1, newCardinality);
        final var ringCount = minMajority(pByz, cardinality, epsilon, bias) * bias + 1;
        members.values().forEach(t -> t.rebalance(ringCount, this));
        final var currentCount = rings.size();
        if (ringCount < currentCount) {
            for (int i = 0; i < currentCount - ringCount; i++) {
                var removed = rings.removeLast();
                removed.clear();
            }
        } else if (ringCount > currentCount) {
            final var added = new ArrayList<Ring<T>>();
            for (int i = currentCount; i < ringCount; i++) {
                final var ring = new Ring<>(i, this);
                rings.add(ring);
                added.add(ring);
            }
            assert rings.size() == ringCount : "Whoops: " + rings.size() + " != " + ringCount;
            members.values().forEach(t -> {
                for (var ring : added) {
                    ring.insert(t.member);
                }
            });
        }
        assert rings.size() == ringCount : "Ring count: " + rings.size() + " does not match: " + ringCount;
        log.debug("Rebalanced: {} from: {} to: {} tolerance: {}", id, currentCount, rings.size(), toleranceLevel());
    }

    @Override
    public UUID register(MembershipListener<T> listener) {
        var uuid = UUID.randomUUID();
        membershipListeners.put(uuid, listener);
        return uuid;
    }

    @Override
    public <Q extends T> void remove(Collection<Q> members) {
        members.forEach(this::remove);
    }

    @Override
    public void remove(Digest id) {
        var removed = members.remove(id);
        if (removed != null) {
            for (Ring<T> ring : rings) {
                ring.delete(removed.member);
            }
        }
    }

    /**
     * remove a member from the receiving DynamicContext
     */
    @Override
    public void remove(T m) {
        remove(m.getId());
    }

    /**
     * @return the Stream of rings managed by the context
     */
    @Override
    public Stream<Ring<T>> rings() {
        return rings.stream();
    }

    /**
     * Answer a random sample of at least range size from the active members of the context
     *
     * @param range    - the desired range
     * @param entropy  - source o randomness
     * @param excluded - predicate to test for exclusion
     * @return a random sample set of the view's live members. May be limited by the number of active members.
     */
    @Override
    public <N extends T> List<T> sample(int range, BitsStreamGenerator entropy, Predicate<T> excluded) {
        return rings.get(entropy.nextInt(rings.size()))
                    .stream()
                    .collect(new ReservoirSampler<>(excluded, range, entropy));
    }

    /**
     * Answer a random sample of at least range size from the active members of the context
     *
     * @param range   - the desired range
     * @param entropy - source o randomness
     * @param exc     - the member to exclude from sample
     * @return a random sample set of the view's live members. May be limited by the number of active members.
     */
    @Override
    public <N extends T> List<T> sample(int range, BitsStreamGenerator entropy, Digest exc) {
        Member excluded = exc == null ? null : getMember(exc);
        return rings.get(entropy.nextInt(rings.size()))
                    .stream()
                    .collect(new ReservoirSampler<T>(excluded, range, entropy));
    }

    @Override
    public int size() {
        return members.size();
    }

    @Override
    public Stream<T> stream(int ring) {
        return ring(ring).stream();
    }

    @Override
    public Stream<T> streamPredecessors(int ring, Digest location, Predicate<T> predicate) {
        return ring(ring).streamPredecessors(location, predicate);
    }

    @Override
    public Stream<T> streamPredecessors(int ring, T m, Predicate<T> predicate) {
        return ring(ring).streamPredecessors(m, predicate);
    }

    @Override
    public Stream<T> streamSuccessors(int ring, Digest location, Predicate<T> predicate) {
        return ring(ring).streamSuccessors(location, predicate);
    }

    @Override
    public Stream<T> streamSuccessors(int ring, T m, Predicate<T> predicate) {
        return ring(ring).streamSuccessors(m, predicate);
    }

    @Override
    public T successor(int ring, Digest hash) {
        return ring(ring).successor(hash);
    }

    @Override
    public T successor(int ring, Digest hash, Predicate<T> predicate) {
        return ring(ring).successor(hash, predicate);
    }

    @Override
    public T successor(int ring, T m) {
        return ring(ring).successor(m);
    }

    @Override
    public T successor(int ring, T m, Predicate<T> predicate) {
        return ring(ring).successor(m, predicate);
    }

    /**
     * @return the list of successors to the key on each ring
     */
    @Override
    public List<T> successors(Digest key) {
        return successors(key, t -> true);
    }

    /**
     * @return the list of successor to the key on each ring that pass the provided predicate test
     */
    @Override
    public List<T> successors(Digest key, Predicate<T> test) {
        List<T> successors = new ArrayList<>();
        for (Ring<T> ring : rings) {
            T successor = ring.successor(key, test);
            if (successor != null) {
                successors.add(successor);
            }
        }
        return successors;
    }

    /**
     * @return the list of successors to the key on each ring
     */
    @Override
    public List<T> successors(T key) {
        return successors(key, t -> true);
    }

    /**
     * @return the list of successor to the key on each ring that pass the provided predicate test
     */
    @Override
    public List<T> successors(T key, Predicate<T> test) {
        List<T> successors = new ArrayList<>();
        for (Ring<T> ring : rings) {
            T successor = ring.successor(key, test);
            if (successor != null) {
                successors.add(successor);
            }
        }
        return successors;
    }

    @Override
    public Iterable<T> successors(int ring, Digest location) {
        return ring(ring).successors(location);
    }

    @Override
    public Iterable<T> successors(int ring, Digest location, Predicate<T> predicate) {
        return ring(ring).successors(location, predicate);
    }

    @Override
    public Iterable<T> successors(int ring, T m, Predicate<T> predicate) {
        return ring(ring).successors(m, predicate);
    }

    /**
     * The number of iterations until a given message has been distributed to all members in the context, using the
     * rings of the receiver as a gossip graph
     */
    @Override
    public int timeToLive() {
        return (rings.size() * diameter()) + 1;
    }

    @Override
    public String toString() {
        return "DynamicContext [" + id + "]";
    }

    /**
     * Answer the tolerance level of the context to byzantine members, assuming this context has been constructed from
     * FF parameters
     */
    @Override
    public int toleranceLevel() {
        return (rings.size() - 1) / bias;
    }

    @Override
    public int totalCount() {
        return members.size();
    }

    @Override
    public Iterable<T> traverse(int ring, T member) {
        return ring(ring).traverse(member);
    }

    @Override
    public boolean validRing(int ring) {
        return ring >= 0 && ring < rings.size();
    }

    private Digest[] hashesFor(T m) {
        Digest key = m.getId();
        Digest[] s = new Digest[rings.size()];
        for (int ring = 0; ring < rings.size(); ring++) {
            s[ring] = hashFor(key, ring);
        }
        return s;
    }

    /**
     * @return the indexed Ring<T>
     */
    private Ring<T> ring(int index) {
        if (index < 0 || index >= rings.size()) {
            throw new IllegalArgumentException("Not a valid ring #: " + index + " max: " + (rings.size() - 1));
        }
        return rings.get(index);
    }

    private Tracked<T> tracking(T m) {
        return members.computeIfAbsent(m.getId(), id1 -> {
            for (var ring : rings) {
                ring.insert(m);
            }
            return new Tracked<>(m, () -> hashesFor(m));
        });
    }

    /**
     * @author hal.hildebrand
     **/
    public static class Tracked<M extends Member> {
        private static final Logger log = LoggerFactory.getLogger(Tracked.class);

        final AtomicBoolean active = new AtomicBoolean(false);
        final M             member;
        Digest[] hashes;

        public Tracked(M member, Supplier<Digest[]> hashes) {
            this.member = member;
            this.hashes = hashes.get();
        }

        public boolean activate() {
            var activated = active.compareAndSet(false, true);
            if (activated) {
                log.trace("Activated: {}", member.getId());
            }
            return activated;
        }

        public Digest hash(int index) {
            return hashes[index];
        }

        public boolean isActive() {
            return active.get();
        }

        public M member() {
            return member;
        }

        public boolean offline() {
            var offlined = active.compareAndSet(true, false);
            if (offlined) {
                log.trace("Offlined: {}", member.getId());
            }
            return offlined;
        }

        @Override
        public String toString() {
            return String.format("%s:%s %s", member, active.get(), Arrays.asList(hashes));
        }

        void rebalance(int ringCount, Context<M> context) {
            final var newHashes = new Digest[ringCount];
            System.arraycopy(hashes, 0, newHashes, 0, Math.min(ringCount, hashes.length));
            for (int i = Math.min(ringCount, hashes.length); i < newHashes.length; i++) {
                newHashes[i] = context.hashFor(member.getId(), i);
            }
            hashes = newHashes;
        }
    }

    /**
     * A ring of members. Also, too, addressable functions by Digest, for ring operations to obtain members.
     *
     * @author hal.hildebrand
     * @since 220
     */
    public static class Ring<T extends Member> implements Iterable<T> {
        private final DynamicContextImpl<T>   context;
        private final int                     index;
        private final NavigableMap<Digest, T> ring = new ConcurrentSkipListMap<>();

        public Ring(int index, DynamicContextImpl<T> context) {
            this.index = index;
            this.context = context;
        }

        /**
         * @param start
         * @param stop
         * @return Return all counter-clockwise items between (but not including) start and stop
         */
        public Iterable<T> betweenPredecessors(T start, T stop) {
            if (start.equals(stop)) {
                return Collections.emptyList();
            }
            Digest startHash = hash(start);
            Digest stopHash = hash(stop);

            if (startHash.compareTo(stopHash) < 0) {
                Iterator<T> head = ring.headMap(startHash, false).descendingMap().values().iterator();
                Iterator<T> tail = ring.tailMap(stopHash, false).descendingMap().values().iterator();
                return new Iterable<T>() {

                    @Override
                    public Iterator<T> iterator() {
                        return new Iterator<T>() {

                            @Override
                            public boolean hasNext() {
                                return tail.hasNext() || head.hasNext();
                            }

                            @Override
                            public T next() {
                                return head.hasNext() ? head.next() : tail.next();
                            }
                        };
                    }
                };
            }
            return ring.subMap(stopHash, false, startHash, false).descendingMap().values();
        }

        /**
         * @param start
         * @param stop
         * @return all clockwise items between (but not including) start item and stop item.
         */
        public Iterable<T> betweenSuccessor(T start, T stop) {
            if (start.equals(stop)) {
                return Collections.emptyList();
            }
            Digest startHash = hash(start);
            Digest stopHash = hash(stop);
            if (startHash.compareTo(stopHash) < 0) {
                return ring.subMap(startHash, false, stopHash, false).values();
            }

            NavigableMap<Digest, T> headMap = ring.headMap(stopHash, false);
            NavigableMap<Digest, T> tailMap = ring.tailMap(startHash, false);

            Iterator<T> head = headMap.values().iterator();
            Iterator<T> tail = tailMap.values().iterator();
            return new Iterable<T>() {

                @Override
                public Iterator<T> iterator() {
                    return new Iterator<T>() {

                        @Override
                        public boolean hasNext() {
                            return tail.hasNext() || head.hasNext();
                        }

                        @Override
                        public T next() {
                            return tail.hasNext() ? tail.next() : head.next();
                        }
                    };
                }
            };
        }

        public void clear() {
            ring.clear();
        }

        public boolean contains(Digest id) {
            return ring.containsKey(hash(id));
        }

        public boolean contains(T member) {
            return ring.containsKey(hash(member));
        }

        public void delete(T m) {
            ring.remove(hash(m));
        }

        /**
         * @param d         - the digest
         * @param predicate - the test function.
         * @return the first successor of d for which function evaluates to SUCCESS. Answer null if function evaluates
         * to FAIL.
         */
        public T findPredecessor(Digest d, Function<T, IterateResult> predicate) {
            return pred(hash(d), predicate);
        }

        /**
         * @param m         - the member
         * @param predicate - the test function.
         * @return the first successor of m for which function evaluates to SUCCESS. Answer null if function evaluates
         * to FAIL.
         */
        public T findPredecessor(T m, Function<T, IterateResult> predicate) {
            return pred(hash(m), predicate);
        }

        /**
         * @param d         - the digest
         * @param predicate - the test function.
         * @return the first successor of d for which function evaluates to SUCCESS. Answer null if function evaluates
         * to FAIL.
         */
        public T findSuccessor(Digest d, Function<T, IterateResult> predicate) {
            return succ(hash(d), predicate);
        }

        /**
         * @param m         - the member
         * @param predicate - the test function.
         * @return the first successor of m for which function evaluates to SUCCESS. Answer null if function evaluates
         * to FAIL.
         */
        public T findSuccessor(T m, Function<T, IterateResult> predicate) {
            return succ(hash(m), predicate);
        }

        /**
         * Answer the nth member on the ring. Wrap arounds permitted - i.e. this cycles.
         */
        public Member get(int m) {
            if (m < 0) {
                throw new IllegalArgumentException("Must be greater than 0: " + m);
            }
            int index = m % ring.size();
            for (Member member : ring.values()) {
                if (index == 0) {
                    return member;
                }
                index--;
            }
            throw new NoSuchElementException("empty ring");
        }

        public int getIndex() {
            return index;
        }

        /**
         * for testing
         *
         * @return
         */
        public Map<Digest, T> getRing() {
            return ring;
        }

        public Digest hash(Digest d) {
            return context.hashFor(d, index);
        }

        public Digest hash(T m) {
            return context.hashFor(m, index);
        }

        public T insert(T m) {
            LoggerFactory.getLogger(getClass()).trace("Adding: {} to ring: {}", m.getId(), index);
            return ring.put(hash(m), m);
        }

        /**
         * <pre>
         *
         *    - An item lies between itself. That is, if pred == itm == succ, True is
         *    returned.
         *
         *    - Everything lies between an item and item and itself. That is, if pred == succ, then
         *    this method always returns true.
         *
         *    - An item is always between itself and any other item. That is, if
         *    pred == item, or succ == item, this method returns True.
         * </pre>
         *
         * @param predecessor - the asserted predecessor on the ring
         * @param item        - the item to test
         * @param successor   - the asserted successor on the ring
         * @return true if the member item is between the pred and succ members on the ring
         */
        public boolean isBetween(T predecessor, T item, T successor) {
            if (predecessor.equals(item) || successor.equals(item)) {
                return true;
            }
            Digest predHash = hash(predecessor);
            Digest memberHash = hash(item);
            Digest succHash = hash(successor);
            return predHash.compareTo(memberHash) < 0 & memberHash.compareTo(succHash) < 0;
        }

        @Override
        public Iterator<T> iterator() {
            return ring.values().iterator();
        }

        public Collection<T> members() {
            return ring.values();
        }

        public T predecessor(Digest location) {
            return predecessor(location, m -> true);
        }

        /**
         * @param location  - the target
         * @param predicate - the test predicate
         * @return the first predecessor of m for which predicate evaluates to True. m is never evaluated.
         */
        public T predecessor(Digest location, Predicate<T> predicate) {
            return pred(hash(location), predicate);
        }

        /**
         * @param m - the member
         * @return the predecessor of the member
         */
        public T predecessor(T m) {
            return predecessor(m, e -> true);
        }

        /**
         * @param m         - the member
         * @param predicate - the test predicate
         * @return the first predecessor of m for which predicate evaluates to True. m is never evaluated.
         */
        public T predecessor(T m, Predicate<T> predicate) {
            return pred(hash(m), predicate);
        }

        public Iterable<T> predecessors(Digest location) {
            return predecessors(location, m -> true);
        }

        /**
         * @param location
         * @param predicate
         * @return an Iterable of all items counter-clock wise in the ring from (but excluding) start location to (but
         * excluding) the first item where predicate(item) evaluates to True.
         */
        public Iterable<T> predecessors(Digest location, Predicate<T> predicate) {
            return preds(hash(location), predicate);
        }

        public Iterable<T> predecessors(T start) {
            return predecessors(start, m -> true);
        }

        /**
         * @param start
         * @param predicate
         * @return an Iterable of all items counter-clock wise in the ring from (but excluding) start location to (but
         * excluding) the first item where predicate(item) evaluates to True.
         */
        public Iterable<T> predecessors(T start, Predicate<T> predicate) {
            return preds(hash(start), predicate);
        }

        /**
         * @return the number of items between item and dest
         */
        public int rank(Digest item, Digest dest) {
            return rankBetween(hash(item), hash(dest));
        }

        /**
         * @return the number of items between item and dest
         */
        public int rank(Digest item, T dest) {
            return rankBetween(hash(item), hash(dest));
        }

        /**
         * @return the number of items between item and dest
         */
        public int rank(T item, T dest) {
            return rankBetween(hash(item), hash(dest));
        }

        public int size() {
            return ring.size();
        }

        public Stream<T> stream() {
            return ring.values().stream();
        }

        /**
         * @param start
         * @param predicate
         * @return a Stream of all items counter-clock wise in the ring from (but excluding) start location to (but
         * excluding) the first item where predicate(item) evaluates to True.
         */
        public Stream<T> streamPredecessors(Digest location, Predicate<T> predicate) {
            return StreamSupport.stream(predecessors(location, predicate).spliterator(), false);
        }

        /**
         * @param start
         * @param predicate
         * @return a list of all items counter-clock wise in the ring from (but excluding) start item to (but excluding)
         * the first item where predicate(item) evaluates to True.
         */
        public Stream<T> streamPredecessors(T m, Predicate<T> predicate) {
            return StreamSupport.stream(predecessors(m, predicate).spliterator(), false);
        }

        /**
         * @param start
         * @param predicate
         * @return a Stream of all items counter-clock wise in the ring from (but excluding) start location to (but
         * excluding) the first item where predicate(item) evaluates to True.
         */
        public Stream<T> streamSuccessors(Digest location, Predicate<T> predicate) {
            return StreamSupport.stream(successors(location, predicate).spliterator(), false);
        }

        /**
         * @param start
         * @param predicate
         * @return a Stream of all items counter-clock wise in the ring from (but excluding) start item to (but
         * excluding) the first item where predicate(item) evaluates to True.
         */
        public Stream<T> streamSuccessors(T m, Predicate<T> predicate) {
            return StreamSupport.stream(successors(m, predicate).spliterator(), false);
        }

        /**
         * @param start
         * @param predicate
         * @return a iterable of all items counter-clock wise in the ring from (but excluding) start location to (but
         * excluding) the first item where predicate(item) evaluates to True.
         */
        public T successor(Digest hash) {
            return successor(hash, e -> true);
        }

        /**
         * @param hash      - the location to start on the ring
         * @param predicate - the test predicate
         * @return the first successor of m for which predicate evaluates to True. m is never evaluated..
         */
        public T successor(Digest hash, Predicate<T> predicate) {
            return succ(hash, predicate);
        }

        /**
         * @param m - the member
         * @return the successor of the member
         */
        public T successor(T m) {
            return successor(m, e -> true);
        }

        /**
         * @param m         - the member
         * @param predicate - the test predicate
         * @return the first successor of m for which predicate evaluates to True. m is never evaluated..
         */
        public T successor(T m, Predicate<T> predicate) {
            return succ(hash(m), predicate);
        }

        public Iterable<T> successors(Digest location) {
            return successors(location, m -> true);
        }

        /**
         * @param start
         * @param predicate
         * @return an Iterable of all items counter-clock wise in the ring from (but excluding) start location to (but
         * excluding) the first item where predicate(item) evaluates to True.
         */
        public Iterable<T> successors(Digest location, Predicate<T> predicate) {
            return succs(hash(location), predicate);
        }

        /**
         * @param start
         * @param predicate
         * @return an Iterable of all items counter-clock wise in the ring from (but excluding) start item to (but
         * excluding) the first item where predicate(item) evaluates to True.
         */
        public Iterable<T> successors(T m, Predicate<T> predicate) {
            return succs(hash(m), predicate);
        }

        @Override
        public String toString() {
            return "Ring[" + index + "] : " + ring.keySet();
        }

        /**
         * @param member
         * @return the iteratator to traverse the ring starting at the member
         */
        public Iterable<T> traverse(T member) {
            Digest hash = hash(member);
            Iterator<T> head = ring.headMap(hash, false).values().iterator();
            Iterator<T> tail = ring.tailMap(hash, false).values().iterator();
            return new Iterable<T>() {

                @Override
                public Iterator<T> iterator() {
                    return new Iterator<T>() {

                        @Override
                        public boolean hasNext() {
                            return tail.hasNext() || head.hasNext();
                        }

                        @Override
                        public T next() {
                            return tail.hasNext() ? tail.next() : head.next();
                        }
                    };
                }
            };
        }

        private T pred(Digest hash, Function<T, IterateResult> predicate) {
            for (T member : ring.headMap(hash, false).descendingMap().values()) {
                switch (predicate.apply(member)) {
                case CONTINUE:
                    continue;
                case FAIL:
                    return null;
                case SUCCESS:
                    return member;
                default:
                    throw new IllegalStateException();
                }
            }
            for (T member : ring.tailMap(hash, false).descendingMap().values()) {
                switch (predicate.apply(member)) {
                case CONTINUE:
                    continue;
                case FAIL:
                    return null;
                case SUCCESS:
                    return member;
                default:
                    throw new IllegalStateException();
                }
            }
            return null;
        }

        private T pred(Digest hash, Predicate<T> predicate) {
            for (T member : ring.headMap(hash, false).descendingMap().values()) {
                if (predicate.test(member)) {
                    return member;
                }
            }
            for (T member : ring.tailMap(hash, false).descendingMap().values()) {
                if (predicate.test(member)) {
                    return member;
                }
            }
            return null;
        }

        private Iterable<T> preds(Digest hash, Predicate<T> predicate) {
            Iterator<T> tail = ring.tailMap(hash, false).descendingMap().values().iterator();
            Iterator<T> head = ring.headMap(hash, false).descendingMap().values().iterator();

            Iterator<T> iterator = new Iterator<T>() {
                private T next = nextMember();

                @Override
                public boolean hasNext() {
                    return next != null;
                }

                @Override
                public T next() {
                    if (next == null) {
                        throw new NoSuchElementException();
                    }
                    T current = next;
                    next = nextMember();
                    return current;
                }

                private T nextMember() {
                    while (head.hasNext()) {
                        T next = head.next();
                        return predicate.test(next) ? null : next;
                    }
                    while (tail.hasNext()) {
                        T next = tail.next();
                        return predicate.test(next) ? null : next;
                    }
                    return null;
                }
            };
            return new Iterable<T>() {

                @Override
                public Iterator<T> iterator() {
                    return iterator;
                }
            };
        }

        /**
         * @return the number of items between item and dest
         */
        private int rankBetween(Digest item, Digest dest) {
            if (item.compareTo(dest) < 0) {
                return ring.subMap(item, false, dest, false).size();
            }
            return ring.tailMap(item, false).size() + ring.headMap(dest, false).size();
        }

        private T succ(Digest hash, Function<T, IterateResult> predicate) {
            if (hash == null) {
                return null;
            }
            for (T member : ring.tailMap(hash, false).values()) {
                switch (predicate.apply(member)) {
                case CONTINUE:
                    continue;
                case FAIL:
                    return null;
                case SUCCESS:
                    return member;
                default:
                    throw new IllegalStateException();

                }
            }
            for (T member : ring.headMap(hash, false).values()) {
                switch (predicate.apply(member)) {
                case CONTINUE:
                    continue;
                case FAIL:
                    return null;
                case SUCCESS:
                    return member;
                default:
                    throw new IllegalStateException();

                }
            }
            return null;
        }

        private T succ(Digest hash, Predicate<T> predicate) {
            if (hash == null) {
                return null;
            }
            for (T member : ring.tailMap(hash, false).values()) {
                if (predicate.test(member)) {
                    return member;
                }
            }
            for (T member : ring.headMap(hash, false).values()) {
                if (predicate.test(member)) {
                    return member;
                }
            }
            return null;
        }

        private Iterable<T> succs(Digest digest, Predicate<T> predicate) {
            Iterator<T> tail = ring.tailMap(digest, false).values().iterator();
            Iterator<T> head = ring.headMap(digest, false).values().iterator();

            Iterator<T> iterator = new Iterator<T>() {
                private T next = nextMember();

                @Override
                public boolean hasNext() {
                    return next != null;
                }

                @Override
                public T next() {
                    if (next == null) {
                        throw new NoSuchElementException();
                    }
                    T current = next;
                    next = nextMember();
                    return current;
                }

                private T nextMember() {
                    while (tail.hasNext()) {
                        T next = tail.next();
                        return predicate.test(next) ? null : next;
                    }
                    while (head.hasNext()) {
                        T next = head.next();
                        return predicate.test(next) ? null : next;
                    }
                    return null;
                }
            };
            return new Iterable<T>() {

                @Override
                public Iterator<T> iterator() {
                    return iterator;
                }
            };
        }

    }
}
