/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.salesforce.apollo.crypto.Digest;

/**
 * A ring of members. Also, too, addressable functions by Digest, for ring
 * operations to obtain members.
 * 
 * @author hal.hildebrand
 * @since 220
 */
public class Ring<T extends Member> implements Iterable<T> {
    public enum IterateResult {
        CONTINUE, FAIL, SUCCESS;
    }

    private final ContextImpl<T>                    context;
    private final int                               index;
    private final ConcurrentNavigableMap<Digest, T> ring = new ConcurrentSkipListMap<>();

    public Ring(int index, ContextImpl<T> context) {
        this.index = index;
        this.context = context;
    }

    /**
     * @param start
     * @param stop
     * @return Return all counter-clockwise items between (but not including) start
     *         and stop
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
     * @return all clockwise items between (but not including) start item and stop
     *         item.
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

        ConcurrentNavigableMap<Digest, T> headMap = ring.headMap(stopHash, false);
        ConcurrentNavigableMap<Digest, T> tailMap = ring.tailMap(startHash, false);

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

    public Set<Digest> difference(Ring<T> r) {
        return Sets.difference(ring.keySet(), r.ring.keySet());
    }

    /**
     * @param d         - the digest
     * @param predicate - the test function.
     * @return the first successor of d for which function evaluates to SUCCESS.
     *         Answer null if function evaluates to FAIL.
     */
    public T findPredecessor(Digest d, Function<T, IterateResult> predicate) {
        return pred(hash(d), predicate);
    }

    /**
     * @param m         - the member
     * @param predicate - the test function.
     * @return the first successor of m for which function evaluates to SUCCESS.
     *         Answer null if function evaluates to FAIL.
     */
    public T findPredecessor(T m, Function<T, IterateResult> predicate) {
        return pred(hash(m), predicate);
    }

    /**
     * @param d         - the digest
     * @param predicate - the test function.
     * @return the first successor of d for which function evaluates to SUCCESS.
     *         Answer null if function evaluates to FAIL.
     */
    public T findSuccessor(Digest d, Function<T, IterateResult> predicate) {
        return succ(hash(d), predicate);
    }

    /**
     * @param m         - the member
     * @param predicate - the test function.
     * @return the first successor of m for which function evaluates to SUCCESS.
     *         Answer null if function evaluates to FAIL.
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
        LoggerFactory.getLogger(getClass()).trace("Adding: {} to ring: {}", m, index);
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
     * @return true if the member item is between the pred and succ members on the
     *         ring
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
     * @return the first predecessor of m for which predicate evaluates to True. m
     *         is never evaluated.
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
     * @return the first predecessor of m for which predicate evaluates to True. m
     *         is never evaluated.
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
     * @return an Iterable of all items counter-clock wise in the ring from (but
     *         excluding) start location to (but excluding) the first item where
     *         predicate(item) evaluates to True.
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
     * @return an Iterable of all items counter-clock wise in the ring from (but
     *         excluding) start location to (but excluding) the first item where
     *         predicate(item) evaluates to True.
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
     * @return a Stream of all items counter-clock wise in the ring from (but
     *         excluding) start location to (but excluding) the first item where
     *         predicate(item) evaluates to True.
     */
    public Stream<T> streamPredecessors(Digest location, Predicate<T> predicate) {
        return StreamSupport.stream(predecessors(location, predicate).spliterator(), false);
    }

    /**
     * @param start
     * @param predicate
     * @return a list of all items counter-clock wise in the ring from (but
     *         excluding) start item to (but excluding) the first item where
     *         predicate(item) evaluates to True.
     */
    public Stream<T> streamPredecessors(T m, Predicate<T> predicate) {
        return StreamSupport.stream(predecessors(m, predicate).spliterator(), false);
    }

    /**
     * @param start
     * @param predicate
     * @return a Stream of all items counter-clock wise in the ring from (but
     *         excluding) start location to (but excluding) the first item where
     *         predicate(item) evaluates to True.
     */
    public Stream<T> streamSuccessors(Digest location, Predicate<T> predicate) {
        return StreamSupport.stream(successors(location, predicate).spliterator(), false);
    }

    /**
     * @param start
     * @param predicate
     * @return a Stream of all items counter-clock wise in the ring from (but
     *         excluding) start item to (but excluding) the first item where
     *         predicate(item) evaluates to True.
     */
    public Stream<T> streamSuccessors(T m, Predicate<T> predicate) {
        return StreamSupport.stream(successors(m, predicate).spliterator(), false);
    }

    /**
     * @param start
     * @param predicate
     * @return a iterable of all items counter-clock wise in the ring from (but
     *         excluding) start location to (but excluding) the first item where
     *         predicate(item) evaluates to True.
     */
    public T successor(Digest hash) {
        return successor(hash, e -> true);
    }

    /**
     * @param hash      - the location to start on the ring
     * @param predicate - the test predicate
     * @return the first successor of m for which predicate evaluates to True. m is
     *         never evaluated..
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
     * @return the first successor of m for which predicate evaluates to True. m is
     *         never evaluated..
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
     * @return an Iterable of all items counter-clock wise in the ring from (but
     *         excluding) start location to (but excluding) the first item where
     *         predicate(item) evaluates to True.
     */
    public Iterable<T> successors(Digest location, Predicate<T> predicate) {
        return succs(hash(location), predicate);
    }

    /**
     * @param start
     * @param predicate
     * @return an Iterable of all items counter-clock wise in the ring from (but
     *         excluding) start item to (but excluding) the first item where
     *         predicate(item) evaluates to True.
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
