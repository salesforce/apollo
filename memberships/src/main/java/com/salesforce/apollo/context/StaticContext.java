/*
 * Copyright (c) 2024, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.context;

import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.MockMember;
import com.salesforce.apollo.membership.ReservoirSampler;
import org.apache.commons.math3.random.BitsStreamGenerator;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.salesforce.apollo.context.Context.minMajority;

/**
 * Static Context implementation
 *
 * @author hal.hildebrand
 */
public class StaticContext<T extends Member> implements Context<T> {

    private final Digest       id;
    private final Tracked<T>[] members;
    private final int[][]      ringMap;
    private final Digest[][]   rings;
    private final int          bias;
    private final double       epsilon;
    private final double       pByz;
    private final int          cardinality;

    public StaticContext(Context<T> of) {
        this(of.getId(), of.getProbabilityByzantine(), of.getBias(), of.allMembers().toList(), of.getEpsilon(),
             of.cardinality());
    }

    public StaticContext(Digest id, double pByz, int bias, Collection<T> members, double epsilon, int cardinality) {
        this(id, members, (short) ((minMajority(pByz, Math.max(bias + 1, cardinality), epsilon, bias) * bias) + 1),
             bias, epsilon, pByz, cardinality);
    }

    public StaticContext(Digest id, double pbyz, Collection<T> members, int bias) {
        this(id, pbyz, bias, members, Context.DEFAULT_EPSILON, members.size());
    }

    public StaticContext(Digest id, Collection<T> members, short rings, int bias, double epsilon, double pByz,
                         int cardinality) {
        this.id = id;
        this.members = newArray(members.size());
        this.rings = new Digest[rings][];
        this.bias = bias;
        this.epsilon = epsilon;
        this.pByz = pByz;
        this.cardinality = Math.max(bias + 1, cardinality);
        for (int j = 0; j < rings; j++) {
            this.rings[j] = new Digest[members.size()];
        }
        this.ringMap = new int[rings][];
        for (int j = 0; j < rings; j++) {
            this.ringMap[j] = new int[members.size()];
        }
        initialize(members);
    }

    @SafeVarargs
    static <E> E[] newArray(int length, E... array) {
        return Arrays.copyOf(array, length);
    }

    @Override
    public Stream<T> allMembers() {
        return Arrays.stream(members).map(t -> t.member);
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
        return cardinality;
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
        int index = Arrays.binarySearch(members, new MockMember(memberID));
        return index >= 0 && (members[index].member.getId().equals(memberID)) ? members[index].member : null;
    }

    @Override
    public T getMember(int i, int r) {
        i = i % size();
        var ring = new StaticRing(r);
        return ring.get(i);
    }

    @Override
    public double getProbabilityByzantine() {
        return pByz;
    }

    public short getRingCount() {
        return (short) rings.length;
    }

    @Override
    public Digest hashFor(T m, int r) {
        assert r >= 0 && r < rings.length : "Invalid ring: " + r + " max: " + (rings.length - 1);
        var ring = ring(r);
        int index = Arrays.binarySearch(members, m);
        var tracked = members[index];
        if (tracked == null) {
            return hashFor(m.getId(), r);
        }
        return tracked.hash(r);
    }

    @Override
    public boolean isBetween(int ring, T predecessor, T item, T successor) {
        return ring(ring).isBetween(predecessor, item, successor);
    }

    @Override
    public boolean isMember(Digest digest) {
        int index = Arrays.binarySearch(members, new MockMember(digest));
        return index >= 0 && (members[index].member.getId().equals(digest));
    }

    @Override
    public boolean isMember(T m) {
        int index = Arrays.binarySearch(members, m);
        return index >= 0 && (members[index].member.getId().equals(m.getId()));
    }

    @Override
    public boolean isSuccessorOf(T m, Digest digest) {
        for (int r = 0; r < rings.length; r++) {
            if (m.equals(ring(r).successor(m))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int memberCount() {
        return members.length;
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
        for (int r = 0; r < rings.length; r++) {
            var ring = new StaticRing(r);
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
        for (int r = 0; r < rings.length; r++) {
            var ring = new StaticRing(r);
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
        return ring(ring).predecessors(start, predicate);
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
        return ring(entropy.nextInt(rings.length)).stream().collect(new ReservoirSampler<>(range, excluded));
    }

    /**
     * Answer a random sample of at least range size from the active members of the context
     *
     * @param range - the desired range
     * @param exc   - the member to exclude from sample
     * @return a random sample set of the view's live members. May be limited by the number of active members.
     */
    @Override
    public <N extends T> List<T> sample(int range, BitsStreamGenerator entropy, Digest exc) {
        Member excluded = exc == null ? null : getMember(exc);
        return ring(entropy.nextInt(rings.length)).stream().collect(new ReservoirSampler<>(range, (T) excluded));
    }

    @Override
    public int size() {
        return members.length;
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
     * @return the list of successor to the key on each ring that passes the provided predicate test
     */
    @Override
    public List<T> successors(Digest key, Predicate<T> test) {
        List<T> successors = new ArrayList<>();
        for (int r = 0; r < rings.length; r++) {
            var ring = new StaticRing(r);
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
        for (int r = 0; r < rings.length; r++) {
            var ring = ring(r);
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

    @Override
    public Iterable<T> traverse(int ring, T member) {
        return ring(ring).traverse(member);
    }

    @Override
    public void uniqueSuccessors(Digest key, Predicate<T> test, Set<T> collector) {
        for (int ring = 0; ring < rings.length; ring++) {
            StaticRing r = ring(ring);
            T successor = r.successor(key, m -> !collector.contains(m) && test.test(m));
            if (successor != null) {
                collector.add(successor);
            }
        }
    }

    @Override
    public boolean validRing(int ring) {
        return ring >= 0 && ring < rings.length;
    }

    private Digest[] hashesFor(T m) {
        Digest key = m.getId();
        Digest[] s = new Digest[rings.length];
        for (int ring = 0; ring < rings.length; ring++) {
            s[ring] = hashFor(key, ring);
        }
        return s;
    }

    private void initialize(Collection<T> members) {
        record ringMapping<T extends Member>(Tracked<T> m, int i) {
        }
        {
            var i = 0;
            for (var m : members) {
                this.members[i++] = new Tracked<>(m, hashesFor(m));
            }
        }
        Arrays.sort(this.members, Comparator.comparing(t -> t.member));
        for (int j = 0; j < rings.length; j++) {
            var mapped = new TreeMap<Digest, ringMapping<T>>();
            for (var i = 0; i < this.members.length; i++) {
                var m = this.members[i];
                mapped.put(Context.hashFor(id, j, m.member.getId()), new ringMapping<>(m, i));
            }
            short index = 0;
            for (var e : mapped.entrySet()) {
                rings[j][index] = e.getKey();
                ringMap[j][index] = e.getValue().i;
                index++;
            }
        }
    }

    private StaticRing ring(int index) {
        if (index < 0 || index >= rings.length) {
            throw new IndexOutOfBoundsException(index);
        }
        return new StaticRing(index);
    }

    private record Tracked<T extends Member>(T member, Digest[] hashes) implements Comparable<Member> {
        @Override
        public int compareTo(Member m) {
            return member.compareTo(m);
        }

        public Digest hash(int ring) {
            return hashes[ring];
        }
    }

    public class StaticRing {

        private final int index;

        private StaticRing(int index) {
            this.index = index;
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
            var ring = ring();
            Digest startHash = hashFor(start);
            Digest stopHash = hashFor(stop);
            int startIndex = Arrays.binarySearch(ring.rehashed, startHash);
            int endIndex = Arrays.binarySearch(ring.rehashed, stopHash);
            return () -> new HeadIntervalIterator(startIndex, t -> true, endIndex);
        }

        /**
         * @return all clockwise items between (but not including) start item and stop item.
         */
        public Iterable<T> betweenSuccessor(T start, T stop) {
            var ring = ring();
            Digest startHash = hashFor(start);
            Digest stopHash = hashFor(stop);
            int startIndex = Arrays.binarySearch(ring.rehashed, startHash);
            int endIndex = Arrays.binarySearch(ring.rehashed, stopHash);
            return () -> new TailIntervalIterator(startIndex, t -> true, endIndex);
        }

        /**
         * @param d         - the digest
         * @param predicate - the test function.
         * @return the first successor of d for which function evaluates to SUCCESS. Answer null if function evaluates
         * to FAIL.
         */
        public T findPredecessor(Digest d, Function<T, IterateResult> predicate) {
            return pred(hashFor(d), predicate);
        }

        /**
         * @param m         - the member
         * @param predicate - the test function.
         * @return the first successor of m for which function evaluates to SUCCESS. Answer null if function evaluates
         * to FAIL.
         */
        public T findPredecessor(T m, Function<T, IterateResult> predicate) {
            return pred(hashFor(m), predicate);
        }

        /**
         * @param d         - the digest
         * @param predicate - the test function.
         * @return the first successor of d for which function evaluates to SUCCESS. Answer null if function evaluates
         * to FAIL.
         */
        public T findSuccessor(Digest d, Function<T, IterateResult> predicate) {
            return succ(hashFor(d), predicate);
        }

        /**
         * @param m         - the member
         * @param predicate - the test function.
         * @return the first successor of m for which function evaluates to SUCCESS. Answer null if function evaluates
         * to FAIL.
         */
        public T findSuccessor(T m, Function<T, IterateResult> predicate) {
            return succ(hashFor(m), predicate);
        }

        public T get(int i) {
            return ring().get(i, members).member;
        }

        public Digest hashFor(Digest d) {
            return Context.hashFor(id, index, d);
        }

        public Digest hashFor(T d) {
            return StaticContext.this.hashFor(d, index);
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
            Digest predHash = hashFor(predecessor);
            Digest memberHash = hashFor(item);
            Digest succHash = hashFor(successor);
            return predHash.compareTo(memberHash) < 0 & memberHash.compareTo(succHash) < 0;
        }

        public T predecessor(Digest digest) {
            return pred(Context.hashFor(id, index, digest), (Predicate<T>) d -> true);
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
            return pred(hashFor(m), predicate);
        }

        public T predecessor(Digest digest, Predicate<T> test) {
            return pred(Context.hashFor(id, index, digest), test);
        }

        public Iterable<T> predecessors(Digest location) {
            return predecessors(location, m -> true);
        }

        public Iterable<T> predecessors(T digest) {
            return preds(Context.hashFor(id, index, digest.getId()), d -> true);
        }

        /**
         * @return an Iterable of all items counter-clock wise in the ring from (but excluding) start location to (but
         * excluding) the first item where predicate(item) evaluates to True.
         */
        public Iterable<T> predecessors(T start, Predicate<T> predicate) {
            return preds(hashFor(start), predicate);
        }

        /**
         * @return an Iterable of all items counter-clock wise in the ring from (but excluding) start location to (but
         * excluding) the first item where predicate(item) evaluates to True.
         */
        public Iterable<T> predecessors(Digest location, Predicate<T> predicate) {
            return preds(hashFor(location), predicate);
        }

        /**
         * @return the number of items between item and dest
         */
        public int rank(Digest item, Digest dest) {
            return rankBetween(hashFor(item), hashFor(dest));
        }

        /**
         * @return the number of items between item and dest
         */
        public int rank(Digest item, T dest) {
            return rankBetween(hashFor(item), hashFor(dest));
        }

        /**
         * @return the number of items between item and dest
         */
        public int rank(T item, T dest) {
            return rankBetween(hashFor(item), hashFor(dest));
        }

        public Stream<T> stream() {
            Iterable<T> iterable = new Iterable<T>() {

                @Override
                public Iterator<T> iterator() {
                    return new Iterator<T>() {
                        private final Ring<T> ring    = ring();
                        private       int     current = 0;

                        @Override
                        public boolean hasNext() {
                            return current < members.length;
                        }

                        @Override
                        public T next() {
                            if (current >= members.length) {
                                throw new NoSuchElementException();
                            }
                            var digest = ring.get(current, members);
                            current++;
                            return digest.member;
                        }
                    };
                }
            };
            return StreamSupport.stream(iterable.spliterator(), false);
        }

        /**
         * @param location
         * @param predicate
         * @return a Stream of all items counter-clock wise in the ring from (but excluding) start location to (but
         * excluding) the first item where predicate(item) evaluates to True.
         */
        public Stream<T> streamPredecessors(Digest location, Predicate<T> predicate) {
            return StreamSupport.stream(predecessors(location, predicate).spliterator(), false);
        }

        /**
         * @param m
         * @param predicate
         * @return a list of all items counter-clock wise in the ring from (but excluding) start item to (but excluding)
         * the first item where predicate(item) evaluates to True.
         */
        public Stream<T> streamPredecessors(T m, Predicate<T> predicate) {
            return StreamSupport.stream(predecessors(m, predicate).spliterator(), false);
        }

        /**
         * @param location
         * @param predicate
         * @return a Stream of all items counter-clock wise in the ring from (but excluding) start location to (but
         * excluding) the first item where predicate(item) evaluates to True.
         */
        public Stream<T> streamSuccessors(Digest location, Predicate<T> predicate) {
            return StreamSupport.stream(successors(location, predicate).spliterator(), false);
        }

        /**
         * @param m
         * @param predicate
         * @return a Stream of all items counter-clock wise in the ring from (but excluding) start item to (but
         * excluding) the first item where predicate(item) evaluates to True.
         */
        public Stream<T> streamSuccessors(T m, Predicate<T> predicate) {
            return StreamSupport.stream(successors(m, predicate).spliterator(), false);
        }

        public T successor(Digest digest) {
            return succ(Context.hashFor(id, index, digest), (Predicate<T>) d -> true);
        }

        public T successor(Digest digest, Predicate<T> test) {
            return succ(Context.hashFor(id, index, digest), test);
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
            return succ(hashFor(m), predicate);
        }

        public Iterable<T> successors(Digest digest) {
            return succs(Context.hashFor(id, index, digest), d -> true);
        }

        /**
         * @param location
         * @param predicate
         * @return an Iterable of all items counter-clock wise in the ring from (but excluding) start location to (but
         * excluding) the first item where predicate(item) evaluates to True.
         */
        public Iterable<T> successors(Digest location, Predicate<T> predicate) {
            return succs(hashFor(location), predicate);
        }

        /**
         * @param m
         * @param predicate
         * @return an Iterable of all items counter-clock wise in the ring from (but excluding) start item to (but
         * excluding) the first item where predicate(item) evaluates to True.
         */
        public Iterable<T> successors(T m, Predicate<T> predicate) {
            return succs(hashFor(m), predicate);
        }

        public Iterable<T> traverse(T member) {
            var ring = ring();
            var digest = hashFor(member);
            int startIndex = Arrays.binarySearch(ring.rehashed, digest);
            final Iterator<T> iterator = new TailIterator(startIndex, t -> true);
            return () -> iterator;
        }

        private T pred(Digest digest, Predicate<T> test) {
            var ring = ring();
            short startIndex = (short) Arrays.binarySearch(ring.rehashed, digest);
            if (startIndex < 0) {
                for (short i = (short) (ring.rehashed.length - 1); i >= 0; i--) {
                    final var tested = ring.get(i, members);
                    if (test.test(tested.member)) {
                        return tested.member;
                    }
                }
                return null;
            }
            for (short i = (short) (startIndex - 1); i >= 0; i--) {
                final var tested = ring.get(i, members);
                if (test.test(tested.member)) {
                    return tested.member;
                }
            }
            for (short i = (short) (ring.rehashed.length - 1); i > startIndex; i--) {
                final var tested = ring.get(i, members);
                if (test.test(tested.member)) {
                    return tested.member;
                }
            }
            return null;
        }

        private T pred(Digest digest, Function<T, IterateResult> predicate) {
            var ring = ring();
            short startIndex = (short) Arrays.binarySearch(ring.rehashed, digest);
            if (startIndex < 0) {
                for (short i = (short) (ring.rehashed.length - 1); i >= 0; i--) {
                    final var member = ring.get(i, members);
                    switch (predicate.apply(member.member)) {
                    case CONTINUE:
                        continue;
                    case FAIL:
                        return null;
                    case SUCCESS:
                        return member.member;
                    default:
                        throw new IllegalStateException();
                    }
                }
                return null;
            }
            for (short i = (short) (startIndex - 1); i >= 0; i--) {
                final var member = ring.get(i, members);
                switch (predicate.apply(member.member)) {
                case CONTINUE:
                    continue;
                case FAIL:
                    return null;
                case SUCCESS:
                    return member.member;
                default:
                    throw new IllegalStateException();
                }
            }
            for (short i = (short) (ring.rehashed.length - 1); i > startIndex; i--) {
                final var member = ring.get(i, members);
                switch (predicate.apply(member.member)) {
                case CONTINUE:
                    continue;
                case FAIL:
                    return null;
                case SUCCESS:
                    return member.member;
                default:
                    throw new IllegalStateException();
                }
            }
            return null;
        }

        private Iterable<T> preds(Digest hash, Predicate<T> predicate) {
            var ring = ring();
            int startIndex = Arrays.binarySearch(ring.rehashed, hash);
            final var iterator = new HeadIterator(startIndex, predicate);
            return () -> iterator;
        }

        /**
         * @return the number of items between item and dest
         */
        private int rankBetween(Digest item, Digest dest) {
            var ring = ring();
            int startIndex = Arrays.binarySearch(ring.rehashed, item);
            int endIndex = Arrays.binarySearch(ring.rehashed, dest);
            int count = 0;
            var i = new TailIntervalIterator(startIndex, t -> false, endIndex);
            while (i.hasNext()) {
                i.next();
                count++;
            }
            return count;
        }

        private Ring<T> ring() {
            return new Ring<T>(rings[index], ringMap[index]);
        }

        private T succ(Digest digest, Predicate<T> test) {
            var ring = ring();
            short startIndex = (short) Arrays.binarySearch(ring.rehashed, digest);
            if (startIndex < 0) {
                for (short i = 0; i < ring.rehashed.length; i++) {
                    final var tested = ring.get(i, members);
                    if (test.test(tested.member)) {
                        return tested.member;
                    }
                }
                return null;
            }
            for (short i = (short) (startIndex + 1); i < ring.rehashed.length; i++) {
                final var tested = ring.get(i, members);
                if (test.test(tested.member)) {
                    return tested.member;
                }
            }
            for (short i = 0; i < startIndex; i++) {
                final var tested = ring.get(i, members);
                if (test.test(tested.member)) {
                    return tested.member;
                }
            }
            return null;
        }

        private T succ(Digest hash, Function<T, IterateResult> predicate) {
            var ring = ring();
            short startIndex = (short) Arrays.binarySearch(ring.rehashed, hash);
            if (startIndex < 0) {
                for (short i = 0; i < ring.rehashed.length; i++) {
                    final var member = ring.get(i, members);
                    switch (predicate.apply(member.member)) {
                    case CONTINUE:
                        continue;
                    case FAIL:
                        return null;
                    case SUCCESS:
                        return member.member;
                    default:
                        throw new IllegalStateException();

                    }
                }
                return null;
            }
            for (short i = (short) (startIndex + 1); i < ring.rehashed.length; i++) {
                final var member = ring.get(i, members);
                switch (predicate.apply(member.member)) {
                case CONTINUE:
                    continue;
                case FAIL:
                    return null;
                case SUCCESS:
                    return member.member;
                default:
                    throw new IllegalStateException();

                }
            }
            for (short i = 0; i < startIndex; i++) {
                final var member = ring.get(i, members);
                switch (predicate.apply(member.member)) {
                case CONTINUE:
                    continue;
                case FAIL:
                    return null;
                case SUCCESS:
                    return member.member;
                default:
                    throw new IllegalStateException();

                }
            }
            return null;
        }

        private Iterable<T> succs(Digest digest, Predicate<T> test) {
            var ring = ring();
            int startIndex = Arrays.binarySearch(ring.rehashed, digest);
            final Iterator<T> iterator = new TailIterator(startIndex, test);
            return () -> iterator;
        }

        // A Ring is a list of rehashed ids and a map from these ids to the original id
        private record Ring<T extends Member>(Digest[] rehashed, int[] mapping) {
            private Tracked<T> get(int i, Tracked<T>[] members) {
                return members[mapping[i]];
            }
        }

        private class HeadIntervalIterator extends HeadIterator {
            final int endExclusive;

            private HeadIntervalIterator(int start, Predicate<T> test, int endExclusive) {
                super(start, test);
                this.endExclusive = endExclusive;
            }

            @Override
            public boolean hasNext() {
                return current != endExclusive && super.hasNext();
            }

            @Override
            public T next() {
                if (current != endExclusive) {
                    throw new NoSuchElementException();
                }
                return super.next();
            }
        }

        private class HeadIterator implements Iterator<T> {
            private final int          start;
            private final Predicate<T> test;
            int current;
            T   next;

            private HeadIterator(int start, Predicate<T> test) {
                assert start >= 0 && start < members.length && test != null;
                this.start = start;
                this.test = test;
                if (!test.test(ring().get(start, members).member)) {
                    next = ring().get(start, members).member();
                }
                current = (start - 1) % members.length;
                if (current < 0) {
                    current = members.length + current;
                }
            }

            @Override
            public boolean hasNext() {
                return current != start && next != null;
            }

            @Override
            public T next() {
                if (next == null) {
                    throw new NoSuchElementException();
                }
                var returned = next;
                next = null;
                current = (current - 1) % members.length;
                if (current < 0) {
                    current = members.length + current;
                }
                if (current != start && !test.test(ring().get(current, members).member)) {
                    next = ring().get(current, members).member();
                }
                return returned;
            }
        }

        private class TailIntervalIterator extends TailIterator {
            final int endExclusive;

            private TailIntervalIterator(int start, Predicate<T> test, int endExclusive) {
                super(start, test);
                this.endExclusive = endExclusive;
            }

            @Override
            public boolean hasNext() {
                return current != endExclusive && super.hasNext();
            }

            @Override
            public T next() {
                if (current == endExclusive) {
                    throw new NoSuchElementException();
                }
                return super.next();
            }
        }

        private class TailIterator implements Iterator<T> {
            private final int          start;
            private final Predicate<T> test;
            int current;
            T   next;

            private TailIterator(int start, Predicate<T> test) {
                assert start >= 0 && start < members.length && test != null;
                this.start = start;
                this.test = test;
                if (!test.test(ring().get(start, members).member)) {
                    next = ring().get(start, members).member();
                }
                current = (start + 1) % members.length;
            }

            @Override
            public boolean hasNext() {
                return current != start && next != null;
            }

            @Override
            public T next() {
                if (next == null) {
                    throw new NoSuchElementException();
                }
                var returned = next;
                next = null;
                current = (current + 1) % members.length;
                if (current != start && !test.test(ring().get(current, members).member)) {
                    next = ring().get(current, members).member();
                }
                return returned;
            }
        }
    }

}
