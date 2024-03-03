/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership;

import com.salesforce.apollo.cryptography.Digest;
import org.apache.commons.math3.random.BitsStreamGenerator;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.salesforce.apollo.membership.Context.minMajority;

/**
 * Static DynamicContext implementation
 *
 * @author hal.hildebrand
 */
public class StaticContext<T extends Member> implements Context<T> {

    private final Digest  id;
    private final T[]     members;
    private final int[][] ringMap;
    private final T[][]   rings;

    public StaticContext(Context<T> of) {
        this(of.getId(), of.allMembers().toList(), of.getRingCount());
    }

    public StaticContext(Digest id, int cardinality, double pByz, int bias, List<T> members, double epsilon) {
        this(id, members, (short) ((minMajority(pByz, cardinality, epsilon, bias) * bias) + 1));
    }

    @SuppressWarnings("unchecked")
    public StaticContext(Digest id, Collection<T> members, short rings) {
        this.id = id;
        this.members = (T[]) new Object[members.size()];
        this.rings = (T[][]) new Object[rings][];
        for (int j = 0; j < rings; j++) {
            this.rings[j] = (T[]) new Object[members.size()];
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
        return null;
    }

    @Override
    public Iterable<T> betweenPredecessors(int ring, T start, T stop) {
        return null;
    }

    @Override
    public Iterable<T> betweenSuccessor(int ring, T start, T stop) {
        return null;
    }

    @Override
    public int cardinality() {
        return 0;
    }

    @Override
    public int diameter() {
        return 0;
    }

    @Override
    public T findPredecessor(int ring, Digest d, Function<T, Ring.IterateResult> predicate) {
        return null;
    }

    @Override
    public T findPredecessor(int ring, T m, Function<T, Ring.IterateResult> predicate) {
        return null;
    }

    @Override
    public T findSuccessor(int ring, Digest d, Function<T, Ring.IterateResult> predicate) {
        return null;
    }

    @Override
    public T findSuccessor(int ring, T m, Function<T, Ring.IterateResult> predicate) {
        return null;
    }

    @Override
    public List<T> getAllMembers() {
        return null;
    }

    @Override
    public int getBias() {
        return 0;
    }

    @Override
    public double getEpsilon() {
        return 0;
    }

    @Override
    public Digest getId() {
        return null;
    }

    @Override
    public T getMember(Digest memberID) {
        return null;
    }

    @Override
    public double getProbabilityByzantine() {
        return 0;
    }

    public short getRingCount() {
        return (short) rings.length;
    }

    @Override
    public Digest hashFor(T m, int ring) {
        return null;
    }

    @Override
    public boolean isBetween(int ring, T predecessor, T item, T successor) {
        return false;
    }

    @Override
    public boolean isMember(Digest digest) {
        return false;
    }

    @Override
    public boolean isMember(T m) {
        return false;
    }

    @Override
    public boolean isSuccessorOf(T m, Digest digest) {
        return false;
    }

    @Override
    public int majority(boolean bootstrapped) {
        return 0;
    }

    @Override
    public int memberCount() {
        return 0;
    }

    @Override
    public T predecessor(int ring, Digest location) {
        return null;
    }

    @Override
    public T predecessor(int ring, Digest location, Predicate<T> predicate) {
        return null;
    }

    @Override
    public T predecessor(int ring, T m) {
        return null;
    }

    @Override
    public T predecessor(int ring, T m, Predicate<T> predicate) {
        return null;
    }

    @Override
    public List<T> predecessors(Digest key) {
        return null;
    }

    public List<T> predecessors(T m) {
        var predecessors = new ArrayList<T>();
        for (var i = 0; i < rings.length; i++) {
            predecessors.add(new StaticRing(i).predecessor(m.getId()));
        }
        return predecessors;
    }

    @Override
    public List<T> predecessors(T key, Predicate<T> test) {
        return null;
    }

    @Override
    public Iterable<T> predecessors(int ring, Digest location) {
        return null;
    }

    @Override
    public Iterable<T> predecessors(int ring, Digest location, Predicate<T> predicate) {
        return null;
    }

    @Override
    public Iterable<T> predecessors(int ring, T start) {
        return null;
    }

    @Override
    public Iterable<T> predecessors(int ring, T start, Predicate<T> predicate) {
        return null;
    }

    public List<T> predecessors(Digest digest, Predicate<T> test) {
        var predecessors = new ArrayList<T>();
        for (var i = 0; i < rings.length; i++) {
            predecessors.add(new StaticRing(i).predecessor(digest, test));
        }
        return predecessors;
    }

    @Override
    public int rank(int ring, Digest item, Digest dest) {
        return 0;
    }

    @Override
    public int rank(int ring, Digest item, T dest) {
        return 0;
    }

    @Override
    public int rank(int ring, T item, T dest) {
        return 0;
    }

    public Ring<T> ring(int index) {
        if (index < 0 || index >= rings.length) {
            throw new IndexOutOfBoundsException(index);
        }
        return null;
    }

    @Override
    public <N extends T> List<T> sample(int range, BitsStreamGenerator entropy, Digest exc) {
        return null;
    }

    @Override
    public <N extends T> List<T> sample(int range, BitsStreamGenerator entropy, Predicate<T> excluded) {
        return null;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public Stream<T> streamPredecessors(int ring, Digest location, Predicate<T> predicate) {
        return null;
    }

    @Override
    public Stream<T> streamPredecessors(int ring, T m, Predicate<T> predicate) {
        return null;
    }

    @Override
    public Stream<T> streamSuccessors(int ring, Digest location, Predicate<T> predicate) {
        return null;
    }

    @Override
    public Stream<T> streamSuccessors(int ring, T m, Predicate<T> predicate) {
        return null;
    }

    @Override
    public T successor(int ring, Digest hash) {
        return null;
    }

    @Override
    public T successor(int ring, Digest hash, Predicate<T> predicate) {
        return null;
    }

    @Override
    public T successor(int ring, T m) {
        return null;
    }

    @Override
    public T successor(int ring, T m, Predicate<T> predicate) {
        return null;
    }

    public List<T> successors(Digest digest) {
        var successors = new ArrayList<T>();
        for (var i = 0; i < rings.length; i++) {
            successors.add(new StaticRing(i).successor(digest));
        }
        return successors;
    }

    public List<T> successors(Digest digest, Predicate<T> test) {
        var successors = new ArrayList<T>();
        for (var i = 0; i < rings.length; i++) {
            successors.add(new StaticRing(i).successor(digest, test));
        }
        return successors;
    }

    @Override
    public List<T> successors(T key) {
        return null;
    }

    @Override
    public List<T> successors(T key, Predicate<T> test) {
        return null;
    }

    @Override
    public Iterable<T> successors(int ring, Digest location) {
        return null;
    }

    @Override
    public Iterable<T> successors(int ring, Digest location, Predicate<T> predicate) {
        return null;
    }

    @Override
    public Iterable<T> successors(int ring, T m, Predicate<T> predicate) {
        return null;
    }

    @Override
    public int timeToLive() {
        return 0;
    }

    @Override
    public int toleranceLevel() {
        return 0;
    }

    @Override
    public int totalCount() {
        return 0;
    }

    @Override
    public Iterable<T> traverse(int ring, T member) {
        return null;
    }

    @Override
    public boolean validRing(int ring) {
        return false;
    }

    private void initialize(Collection<T> members) {
        record ringMapping<T extends Member>(T m, short i) {
        }
        var i = 0;
        for (var m : members) {
            this.members[i++] = m;
        }
        Arrays.sort(this.members);
        for (int j = 0; j < rings.length; j++) {
            var mapped = new TreeMap<Digest, ringMapping>();
            for (short idx = 0; i < this.members.length; i++) {
                var m = this.members[idx];
                mapped.put(Context.hashFor(id, j, m.getId()), new ringMapping<T>(m, idx));
            }
            short index = 0;
            for (var e : mapped.entrySet()) {
                rings[j][index] = (T) e.getValue().m;
                ringMap[j][index] = e.getValue().i;
                index++;
            }
        }
    }

    public class StaticRing {
        private final int index;

        private StaticRing(int index) {
            this.index = index;
        }

        public Digest hashFor(Digest d) {
            return Context.hashFor(id, index, d);
        }

        public T predecessor(Digest digest) {
            return pred(Context.hashFor(id, index, digest), d -> true);
        }

        public T predecessor(Digest digest, Predicate<T> test) {
            return pred(Context.hashFor(id, index, digest), test);
        }

        public Iterable<T> predecessors(T digest) {
            return preds(Context.hashFor(id, index, digest.getId()), d -> true);
        }

        public Iterable<T> predecessors(T digest, Predicate<Digest> test) {
            return preds(Context.hashFor(id, index, digest.getId()), test);
        }

        public Stream<T> stream() {
            Iterable<T> iterable = new Iterable<T>() {

                @Override
                public Iterator<T> iterator() {
                    return new Iterator<T>() {
                        private final Ring<T> ring = ring();
                        private short current = 0;

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
                            return digest;
                        }
                    };
                }
            };
            return StreamSupport.stream(iterable.spliterator(), false);
        }

        public T successor(Digest digest) {
            return succ(Context.hashFor(id, index, digest), d -> true);
        }

        public T successor(Digest digest, Predicate<T> test) {
            return succ(Context.hashFor(id, index, digest), test);
        }

        public Iterable<T> successors(Digest digest) {
            return succs(Context.hashFor(id, index, digest), d -> true);
        }

        public Iterable<T> sucessors(Digest digest, Predicate<T> test) {
            return succs(Context.hashFor(id, index, digest), test);
        }

        private T pred(Digest digest, Predicate<T> test) {
            var ring = ring();
            short startIndex = (short) Arrays.binarySearch(ring.rehashed, digest);
            if (startIndex < 0) {
                for (short i = (short) (ring.rehashed.length - 1); i >= 0; i--) {
                    final var tested = ring.get(i, members);
                    if (test.test(tested)) {
                        return tested;
                    }
                }
                return null;
            }
            for (short i = (short) (startIndex - 1); i >= 0; i--) {
                final var tested = ring.get(i, members);
                if (test.test(tested)) {
                    return tested;
                }
            }
            for (short i = (short) (ring.rehashed.length - 1); i > startIndex; i--) {
                final var tested = ring.get(i, members);
                if (test.test(tested)) {
                    return tested;
                }
            }
            return null;
        }

        private Iterable<T> preds(Digest digest, Predicate<Digest> test) {
            var ring = ring();
            short startIndex = (short) Arrays.binarySearch(ring.rehashed, digest);
            final var iterator = new HeadIterator(startIndex, test);
            return new Iterable<>() {

                @Override
                public Iterator<T> iterator() {
                    return iterator;
                }
            };
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
                    if (test.test(tested)) {
                        return tested;
                    }
                }
                return null;
            }
            for (short i = (short) (startIndex + 1); i < ring.rehashed.length; i++) {
                final var tested = ring.get(i, members);
                if (test.test(tested)) {
                    return tested;
                }
            }
            for (short i = 0; i < startIndex; i++) {
                final var tested = ring.get(i, members);
                if (test.test(tested)) {
                    return tested;
                }
            }
            return null;
        }

        private Iterable<T> succs(Digest digest, Predicate<T> test) {
            var ring = ring();
            short startIndex = (short) Arrays.binarySearch(ring.rehashed, digest);
            final Iterator<T> iterator = new TailIterator<T>(startIndex, test);
            return new Iterable<>() {

                @Override
                public Iterator<T> iterator() {
                    return iterator;
                }
            };
        }

        private static class HeadIterator<T extends Member> implements Iterator<T> {
            private final short        start;
            private final Predicate<T> test;
            private       short        current;
            private       T[]          ids;
            private       Ring<T>      ring;

            private HeadIterator(short start, Predicate<T> test) {
                this.start = start;
                this.test = test;
                current = (short) ((start - 1) % ids.length);
            }

            @Override
            public boolean hasNext() {
                return current != start && test.test(ring.get(current, ids));
            }

            @Override
            public T next() {
                if (current == start || test.test(ring.get(current, ids))) {
                    throw new NoSuchElementException();
                }
                var m = ring.get(start, ids);
                current = (short) ((current + 1) % ids.length);
                return m;
            }
        }

        // A Ring is a list of rehashed ids and a map from these ids to the original id
        private record Ring<T extends Member>(T[] rehashed, int[] mapping) {
            private T get(short i, T[] members) {
                return members[mapping[i]];
            }
        }

        private static class TailIterator<T extends Member> implements Iterator<T> {
            private final short        start;
            private final Predicate<T> test;
            private       short        current;
            private       T[]          ids;
            private       Ring<T>      ring;

            private TailIterator(short start, Predicate<T> test) {
                this.start = start;
                this.test = test;
                current = (short) ((start - 1) % ids.length);
            }

            @Override
            public boolean hasNext() {
                return current != start && test.test(ring.get(current, ids));
            }

            @Override
            public T next() {
                if (current == start || test.test(ring.get(current, ids))) {
                    throw new NoSuchElementException();
                }
                var member = ring.get(start, ids);
                current = (short) (current - 1 % ids.length);
                return member;
            }
        }
    }
}
