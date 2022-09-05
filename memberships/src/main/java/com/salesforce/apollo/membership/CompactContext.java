/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership;

import static com.salesforce.apollo.membership.Context.hashFor;
import static com.salesforce.apollo.membership.Context.minMajority;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;

/**
 * Compact context structure that mimics a context, but only tracks the digest
 * ids of the members.
 *
 * @author hal.hildebrand
 *
 */
public class CompactContext {

    public static class Builder {
        private int          bias    = 2;
        private double       epsilon = Context.DEFAULT_EPSILON;
        private Digest       id      = DigestAlgorithm.DEFAULT.getOrigin();
        private List<Digest> members;
        private double       pByz    = 0.1;                                // 10% chance any node is out to get ya

        public Builder() {
        }

        public Builder(com.salesforce.apollo.membership.Context.Builder<Member> builder) {
            bias = builder.bias;
            epsilon = builder.epsilon;
            id = builder.id;
            pByz = builder.pByz;
        }

        public CompactContext build() {
            if (members == null) {
                throw new IllegalArgumentException("Members must not be null");
            }
            return new CompactContext(id, Math.max(bias + 1, members.size()), pByz, bias, members, epsilon);
        }

        public int getBias() {
            return bias;
        }

        public double getEpsilon() {
            return epsilon;
        }

        public Digest getId() {
            return id;
        }

        public List<Digest> getMembers() {
            return members;
        }

        public double getpByz() {
            return pByz;
        }

        public Builder setBias(int bias) {
            this.bias = bias;
            return this;
        }

        public Builder setEpsilon(double epsilon) {
            this.epsilon = epsilon;
            return this;
        }

        public Builder setId(Digest id) {
            this.id = id;
            return this;
        }

        public Builder setMembers(List<Digest> members) {
            this.members = members;
            return this;
        }

        public Builder setpByz(double pByz) {
            this.pByz = pByz;
            return this;
        }
    }

    public class CompactRing {
        private static class HeadIterator implements Iterator<Digest> {
            private int                     current;
            private Digest[]                ids;
            private Ring                    ring;
            private final int               start;
            private final Predicate<Digest> test;

            private HeadIterator(int start, Predicate<Digest> test) {
                this.start = start;
                this.test = test;
                current = (start - 1) % ids.length;
            }

            @Override
            public boolean hasNext() {
                return current != start && test.test(ring.get(current, ids));
            }

            @Override
            public Digest next() {
                if (current == start || test.test(ring.get(current, ids))) {
                    throw new NoSuchElementException();
                }
                var digest = ring.get(start, ids);
                current = (current + 1) % ids.length;
                return digest;
            }
        }

        private record Ring(Digest[] keys, short[] values) {
            private Digest get(int i, Digest[] ids) {
                return ids[values[i]];
            }
        }

        private static class TailIterator implements Iterator<Digest> {
            private int                     current;
            private Digest[]                ids;
            private Ring                    ring;
            private final int               start;
            private final Predicate<Digest> test;

            private TailIterator(int start, Predicate<Digest> test) {
                this.start = start;
                this.test = test;
                current = (start - 1) % ids.length;
            }

            @Override
            public boolean hasNext() {
                return current != start && test.test(ring.get(current, ids));
            }

            @Override
            public Digest next() {
                if (current == start || test.test(ring.get(current, ids))) {
                    throw new NoSuchElementException();
                }
                var digest = ring.get(start, ids);
                current = current - 1 % ids.length;
                return digest;
            }
        }

        private final int index;

        private CompactRing(int index) {
            this.index = index;
        }

        public Digest hashFor(Digest d) {
            return Context.hashFor(id, index, d);
        }

        public Digest predecessor(Digest digest) {
            return pred(Context.hashFor(id, index, digest), d -> true);
        }

        public Digest predecessor(Digest digest, Predicate<Digest> test) {
            return pred(Context.hashFor(id, index, digest), test);
        }

        public Iterable<Digest> predecessors(Digest digest) {
            return preds(Context.hashFor(id, index, digest), d -> true);
        }

        public Iterable<Digest> predecessors(Digest digest, Predicate<Digest> test) {
            return preds(Context.hashFor(id, index, digest), test);
        }

        public Stream<Digest> stream() {
            Iterable<Digest> iterable = new Iterable<Digest>() {

                @Override
                public Iterator<Digest> iterator() {
                    return new Iterator<Digest>() {
                        private int        current = 0;
                        private final Ring ring    = ring();

                        @Override
                        public boolean hasNext() {
                            return current < ids.length;
                        }

                        @Override
                        public Digest next() {
                            if (current >= ids.length) {
                                throw new NoSuchElementException();
                            }
                            var digest = ring.get(current, ids);
                            current++;
                            return digest;
                        }
                    };
                }
            };
            return StreamSupport.stream(iterable.spliterator(), false);
        }

        public Digest successor(Digest digest) {
            return succ(Context.hashFor(id, index, digest), d -> true);
        }

        public Digest successor(Digest digest, Predicate<Digest> test) {
            return succ(Context.hashFor(id, index, digest), test);
        }

        public Iterable<Digest> successors(Digest digest) {
            return succs(Context.hashFor(id, index, digest), d -> true);
        }

        public Iterable<Digest> sucessors(Digest digest, Predicate<Digest> test) {
            return succs(Context.hashFor(id, index, digest), test);
        }

        private Digest pred(Digest digest, Predicate<Digest> test) {
            var ring = ring();
            short startIndex = (short) Arrays.binarySearch(ring.keys, digest);
            if (startIndex < 0) {
                for (short i = (short) (ring.keys.length - 1); i >= 0; i--) {
                    final var tested = ring.get(i, ids);
                    if (test.test(tested)) {
                        return tested;
                    }
                }
                return null;
            }
            for (short i = (short) (startIndex - 1); i >= 0; i--) {
                final var tested = ring.get(i, ids);
                if (test.test(tested)) {
                    return tested;
                }
            }
            for (short i = (short) (ring.keys.length - 1); i > startIndex; i--) {
                final var tested = ring.get(i, ids);
                if (test.test(tested)) {
                    return tested;
                }
            }
            return null;
        }

        private Iterable<Digest> preds(Digest digest, Predicate<Digest> test) {
            var ring = ring();
            short startIndex = (short) Arrays.binarySearch(ring.keys, digest);
            final var iterator = new HeadIterator(startIndex, test);
            return new Iterable<>() {

                @Override
                public Iterator<Digest> iterator() {
                    return iterator;
                }
            };
        }

        private Ring ring() {
            return new Ring(rings[index], ringMap[index]);
        }

        private Digest succ(Digest digest, Predicate<Digest> test) {
            var ring = ring();
            short startIndex = (short) Arrays.binarySearch(ring.keys, digest);
            if (startIndex < 0) {
                for (short i = 0; i < ring.keys.length; i++) {
                    final var tested = ring.get(i, ids);
                    if (test.test(tested)) {
                        return tested;
                    }
                }
                return null;
            }
            for (short i = (short) (startIndex + 1); i < ring.keys.length; i++) {
                final var tested = ring.get(i, ids);
                if (test.test(tested)) {
                    return tested;
                }
            }
            for (short i = 0; i < startIndex; i++) {
                final var tested = ring.get(i, ids);
                if (test.test(tested)) {
                    return tested;
                }
            }
            return null;
        }

        private Iterable<Digest> succs(Digest digest, Predicate<Digest> test) {
            var ring = ring();
            short startIndex = (short) Arrays.binarySearch(ring.keys, digest);
            final var iterator = new TailIterator(startIndex, test);
            return new Iterable<>() {

                @Override
                public Iterator<Digest> iterator() {
                    return iterator;
                }
            };
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(Context.Builder<Member> ctxBuilder) {
        return new Builder(ctxBuilder);
    }

    private final Digest    id;
    private final Digest[]  ids;
    private final short[][] ringMap;

    private final Digest[][] rings;

    public CompactContext(Digest id, int cardinality, double pByz, int bias, List<Digest> ids, double epsilon) {
        this(id, ids, (short) ((minMajority(pByz, cardinality, epsilon, bias) * bias) + 1));
    }

    public CompactContext(Digest id, List<Digest> ids, short rings) {
        this.id = id;
        this.ids = new Digest[ids.size()];
        this.rings = new Digest[rings][];
        for (int j = 0; j < rings; j++) {
            this.rings[j] = new Digest[ids.size()];
        }
        this.ringMap = new short[rings][];
        for (int j = 0; j < rings; j++) {
            this.ringMap[j] = new short[ids.size()];
        }
        initialize(ids);
    }

    public int getRingCount() {
        return rings.length;
    }

    public List<Digest> predecessors(Digest digest) {
        var predecessors = new ArrayList<Digest>();
        for (var i = 0; i < rings.length; i++) {
            predecessors.add(new CompactRing(i).predecessor(digest));
        }
        return predecessors;
    }

    public List<Digest> predecessors(Digest digest, Predicate<Digest> test) {
        var predecessors = new ArrayList<Digest>();
        for (var i = 0; i < rings.length; i++) {
            predecessors.add(new CompactRing(i).predecessor(digest, test));
        }
        return predecessors;
    }

    public CompactRing ring(int index) {
        if (index < 0 || index >= rings.length) {
            throw new IndexOutOfBoundsException(index);
        }
        return new CompactRing(index);
    }

    public List<Digest> successors(Digest digest) {
        var successors = new ArrayList<Digest>();
        for (var i = 0; i < rings.length; i++) {
            successors.add(new CompactRing(i).successor(digest));
        }
        return successors;
    }

    public List<Digest> successors(Digest digest, Predicate<Digest> test) {
        var successors = new ArrayList<Digest>();
        for (var i = 0; i < rings.length; i++) {
            successors.add(new CompactRing(i).successor(digest, test));
        }
        return successors;
    }

    private void initialize(List<Digest> members) {
        for (short i = 0; i < members.size(); i++) {
            ids[i] = members.get(i);
        }
        Arrays.sort(ids);
        for (int j = 0; j < rings.length; j++) {
            var mapped = new TreeMap<Digest, Short>();
            for (short i = 0; i < ids.length; i++) {
                mapped.put(hashFor(id, j, ids[i]), i);
            }
            short index = 0;
            for (var e : mapped.entrySet()) {
                rings[j][index] = e.getKey();
                ringMap[j][index] = e.getValue();
                index++;
            }
        }
    }
}
