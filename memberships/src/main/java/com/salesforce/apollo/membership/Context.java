/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership;

import static java.util.concurrent.ForkJoinPool.commonPool;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.protocols.HashKey;

/**
 * Provides a Context for Membership and is uniquely identified by a HashKey;.
 * Members may be either active or offline. The Context maintains a number of
 * Rings (may be zero) that the Context provides for Firefly type ordering
 * operators. Each ring has a unique hash of each individual member, and thus
 * each ring has a different ring order of the same membership set. Hashes for
 * Context level operators include the ID of the ring. Hashes computed for each
 * member, per ring include the ID of the enclosing Context.
 * 
 * @author hal.hildebrand
 *
 */
public class Context<T extends Member> {

    public static class Counter {
        private Integer       current = 0;
        private List<Integer> indices;

        public Counter(Set<Integer> indices) {
            this.indices = new ArrayList<>(indices);
            Collections.sort(this.indices);
        }

        public boolean accept() {
            boolean accepted = indices.isEmpty() ? false : current.equals(indices.get(0));
            if (accepted) {
                indices = indices.subList(1, indices.size());
            }
            current = current + 1;
            return accepted;
        }
    }

    public interface MembershipListener<T> {

        /**
         * A member has failed
         * 
         * @param member
         */
        default void fail(T member) {
        };

        /**
         * A new member has recovered and is now live
         * 
         * @param member
         */
        default void recover(T member) {
        };
    }

    private static class ReservoirSampler<T> implements Collector<T, List<T>, List<T>> {

        private int                c = 0;
        private Object             exclude;
        private final SecureRandom rand;
        private final int          sz;

        public ReservoirSampler(Object excluded, int size, SecureRandom entropy) {
            this.exclude = excluded;
            this.sz = size;
            rand = entropy;
        }

        @Override
        public BiConsumer<List<T>, T> accumulator() {
            return this::addIt;
        }

        @Override
        public Set<java.util.stream.Collector.Characteristics> characteristics() {
            return EnumSet.of(Collector.Characteristics.UNORDERED, Collector.Characteristics.IDENTITY_FINISH);
        }

        @Override
        public BinaryOperator<List<T>> combiner() {
            return (left, right) -> {
                left.addAll(right);
                return left;
            };
        }

        @Override
        public Function<List<T>, List<T>> finisher() {
            return (i) -> i;
        }

        @Override
        public Supplier<List<T>> supplier() {
            return ArrayList::new;
        }

        private void addIt(final List<T> in, T s) {
            if (exclude.equals(s)) {
                return;
            }
            if (in.size() < sz) {
                in.add(s);
            } else {
                int replaceInIndex = (int) (rand.nextDouble() * (sz + (c++) + 1));
                if (replaceInIndex < sz) {
                    in.set(replaceInIndex, s);
                }
            }
        }

    }

    public static final ThreadLocal<MessageDigest> DIGEST_CACHE = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance(Context.SHA_256);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    });

    public static final String SHA_256 = "sha-256";

    private static final String CONTEXT_HASH_TEMPLATE = "%s-%s";
    private static final String RING_HASH_TEMPLATE    = "%s-%s-%s";

    /**
     * @return the minimum t such that the probability of more than t out of 2t+1
     *         monitors are correct with probability e/size given the uniform
     *         probability pByz that a monitor is Byzantine.
     */
    public static int minMajority(double pByz, double faultToleranceLevel) {
        for (int t = 1; t <= 10000; t++) {
            double pf = 1.0 - Util.binomialc(t, 2 * t + 1, pByz);
            if (faultToleranceLevel >= pf) {
                return t;
            }
        }
        throw new IllegalArgumentException("Cannot compute number if rings from pByz=" + pByz);
    }

    private final Map<HashKey, T> active = new ConcurrentHashMap<>();

    private BiFunction<T, Integer, HashKey>     hasher              = (m, ring) -> hashFor(m, ring);
    private final Map<HashKey, HashKey[]>       hashes              = new ConcurrentHashMap<>();
    private final HashKey                       id;
    private Logger                              log                 = LoggerFactory.getLogger(Context.class);
    private final List<MembershipListener<T>>   membershipListeners = new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<HashKey, T> offline             = new ConcurrentHashMap<>();

    private final Ring<T>[] rings;

    public Context(HashKey id) {
        this(id, 0);
    }

    @SuppressWarnings("unchecked")
    public Context(HashKey id, int r) {
        this.id = id;
        this.rings = new Ring[r];
        for (int i = 0; i < r; i++) {
            rings[i] = new Ring<T>(i, hasher);
        }
    }

    /**
     * Mark a member as active in the context
     */
    public void activate(T m) {
        active.computeIfAbsent(m.getId(), id -> m);
        offline.remove(m.getId());
        for (Ring<T> ring : rings) {
            ring.insert(m);
        }
        membershipListeners.stream().forEach(l -> {
            commonPool().execute(() -> {
                try {
                    l.recover(m);
                } catch (Throwable e) {
                    log.error("error recoving member in listener: " + l, e);
                }
            });
        });
    }

    public boolean activateIfOffline(HashKey memberID) {
        T offlined = offline.remove(memberID);
        if (offlined == null) {
            return false;
        }
        activate(offlined);
        return true;
    }

    public void add(T m) {
        offline(m);
    }

    public Stream<T> allMembers() {
        return Arrays.asList(active.values(), offline.values()).stream().flatMap(c -> c.stream());
    }

    public int cardinality() {
        return active.size() + offline.size();
    }

    public void clear() {
        for (Ring<T> ring : rings) {
            ring.clear();
        }
        hashes.clear();
    }

    /**
     * Answer the aproximate diameter of the receiver, assuming the rings were built
     * with FF parameters, with the rings forming random graph connections segments.
     */
    public int diameter() {
        return (cardinality());
    }

    /**
     * Answer the aproximate diameter of the receiver, assuming the rings were built
     * with FF parameters, with the rings forming random graph connections segments
     * with the supplied cardinality
     */
    public int diameter(int c) {
        double pN = ((double) (2 * toleranceLevel())) / ((double) c);
        double logN = Math.log(c);
        return (int) (logN / Math.log(c * pN));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Context<?> other = (Context<?>) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        return true;
    }

    public Collection<T> getActive() {
        return active.values();
    }

    public T getActiveMember(HashKey memberID) {
        return active.get(memberID);
    }

    public HashKey getId() {
        return id;
    }

    public T getMember(HashKey memberID) {
        T member = active.get(memberID);
        if (member == null) {
            member = offline.get(memberID);
        }
        return member;
    }

    public Collection<T> getOffline() {
        return offline.values();
    }

    public int getRingCount() {
        return rings.length;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    public boolean isActive(T m) {
        assert m != null;
        return active.containsKey(m.getId());
    }

    public boolean isOffline(HashKey hashKey) {
        return offline.containsKey(hashKey);
    }

    public boolean isOffline(T m) {
        return offline.containsKey(m.getId());
    }

    /**
     * Take a member offline
     */
    public void offline(T m) {
        active.remove(m.getId());
        offline.computeIfAbsent(m.getId(), id -> m);
        for (Ring<T> ring : rings) {
            ring.delete(m);
        }
        membershipListeners.stream().forEach(l -> {
            commonPool().execute(() -> {
                try {
                    l.fail(m);
                } catch (Throwable e) {
                    log.error("error sending fail to listener: " + l, e);
                }
            });
        });
    }

    public boolean offlineIfActive(HashKey memberID) {
        T activated = active.remove(memberID);
        if (activated == null) {
            return false;
        }
        offline(activated);
        return true;
    }

    /**
     * @return the predecessor on each ring for the provided key
     */
    public List<T> predecessors(HashKey key) {
        return predecessors(key, t -> true);
    }

    /**
     * @return the predecessor on each ring for the provided key that pass the
     *         provided predicate
     */
    public List<T> predecessors(HashKey key, Predicate<T> test) {
        List<T> predecessors = new ArrayList<>();
        for (Ring<T> ring : rings) {
            T predecessor = ring.predecessor(contextHash(key, ring.getIndex()), test);
            if (predecessor != null) {
                predecessors.add(predecessor);
            }
        }
        return predecessors;
    }

    public void register(MembershipListener<T> listener) {
        membershipListeners.add(listener);
    }

    /**
     * remove a member from the receiving Context
     */
    public void remove(T m) {
        HashKey[] s = hashes.remove(m.getId());
        if (s == null) {
            return;
        }
        active.remove(m.getId());
        offline.remove(m.getId());
        for (int i = 0; i < s.length; i++) {
            rings[i].delete(m);
        }
    }

    /**
     * @return the indexed Ring<T>
     */
    public Ring<T> ring(int index) {
        if (index < 0 || index >= rings.length) {
            throw new IllegalArgumentException("Not a valid ring #: " + index);
        }
        return rings[index];
    }

    /**
     * @return the Stream of rings managed by the context
     */
    public Stream<Ring<T>> rings() {
        return Arrays.asList(rings).stream();
    }

    /**
     * Answer a random sample of at least range size from the active members of the
     * context
     * 
     * @param range    - the desired range
     * @param entropy  - source o randomness
     * @param excluded - the member to exclude from sample
     * @return a random sample set of the view's live members. May be limited by the
     *         number of active members.
     */
    public <N extends T> List<T> sample(int range, SecureRandom entropy, HashKey excluded) {
        return rings[entropy.nextInt(rings.length)].stream().collect(new ReservoirSampler<T>(excluded, range, entropy));
    }

    /**
     * @return the list of successors to the key on each ring
     */
    public List<T> successors(HashKey key) {
        return successors(key, t -> true);
    }

    /**
     * @return the list of successor to the key on each ring that pass the provided
     *         predicate test
     */
    public List<T> successors(HashKey key, Predicate<T> test) {
        List<T> successors = new ArrayList<>();
        for (Ring<T> ring : rings) {
            T successor = ring.successor(contextHash(key, ring.getIndex()), test);
            if (successor != null) {
                successors.add(successor);
            }
        }
        return successors;
    }

    /**
     * The number of iterations until a given message has been distributed to all
     * members in the context, using the rings of the receiver as a gossip graph
     */
    public int timeToLive() {
        return toleranceLevel() * diameter() + 1;
    }

    /**
     * Answer the tolerance level of the context to byzantine members, assuming this
     * context has been constructed from FF parameters
     */
    public int toleranceLevel() {
        return (rings.length - 1) / 2;
    }

    @Override
    public String toString() {
        return "Context [id=" + id + " " + ring(0) + "]";
    }

    HashKey hashFor(T m, int index) {
        HashKey[] hSet = hashes.computeIfAbsent(m.getId(), k -> {
            HashKey[] s = new HashKey[rings.length];
            MessageDigest md = DIGEST_CACHE.get();
            for (int ring = 0; ring < rings.length; ring++) {
                md.reset();
                md.update(String.format(RING_HASH_TEMPLATE, id, m.getId(), ring).getBytes());
                s[ring] = new HashKey(md.digest());
            }
            return s;
        });
        if (hSet == null) {
            throw new IllegalArgumentException("T " + m.getId() + " is not part of this group " + id);
        }
        return hSet[index];
    }

    private HashKey contextHash(HashKey key, int ring) {
        MessageDigest md = DIGEST_CACHE.get();
        md.reset();
        md.update(String.format(CONTEXT_HASH_TEMPLATE, id, ring).getBytes());
        return new HashKey(md.digest());
    }
}
