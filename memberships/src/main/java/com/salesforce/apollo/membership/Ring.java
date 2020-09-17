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
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.salesforce.apollo.protocols.HashKey;

/**
 * A ring of members. Also, too, addressable functions by HashKey, for ring
 * operations to obtain members.
 * 
 * @author hal.hildebrand
 * @since 220
 */
public class Ring<T extends Member> implements Iterable<T> {
    private final Context<T>                         context;
    private final int                                index;
    private final ConcurrentNavigableMap<HashKey, T> ring = new ConcurrentSkipListMap<>();

    public Ring(int index, Context<T> context) {
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
        HashKey startHash = hash(start);
        HashKey stopHash = hash(stop);

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
        HashKey startHash = hash(start);
        HashKey stopHash = hash(stop);
        if (startHash.compareTo(stopHash) < 0) {
            return ring.subMap(startHash, false, stopHash, false).values();
        }

        ConcurrentNavigableMap<HashKey, T> headMap = ring.headMap(stopHash, false);
        ConcurrentNavigableMap<HashKey, T> tailMap = ring.tailMap(startHash, false);

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

    public int getIndex() {
        return index;
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
     * @return true if the member m is between the pred and succ members on the ring
     */
    public boolean isBetween(T predecessor, T item, T successor) {
        if (predecessor.equals(item) || successor.equals(item)) {
            return true;
        }
        HashKey predHash = hash(predecessor);
        HashKey memberHash = hash(item);
        HashKey succHash = hash(successor);
        return predHash.compareTo(memberHash) < 0 && memberHash.compareTo(succHash) < 0;
    }

    @Override
    public Iterator<T> iterator() {
        return ring.values().iterator();
    }

    public Collection<T> members() {
        return ring.values();
    }

    public T predecessor(HashKey location) {
        return predecessor(location, m -> true);
    }

    /**
     * @param location  - the target
     * @param predicate - the test predicate
     * @return the first predecessor of m for which predicate evaluates to True. m
     *         is never evaluated.
     */
    public T predecessor(HashKey location, Predicate<T> predicate) {
        for (T member : ring.headMap(location, false).descendingMap().values()) {
            if (predicate.test(member)) {
                return member;
            }
        }
        for (T member : ring.tailMap(location, false).descendingMap().values()) {
            if (predicate.test(member)) {
                return member;
            }
        }
        return null;
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
        HashKey itemHash = hash(m);

        for (T member : ring.headMap(itemHash, false).descendingMap().values()) {
            if (predicate.test(member)) {
                return member;
            }
        }
        for (T member : ring.tailMap(itemHash, false).descendingMap().values()) {
            if (predicate.test(member)) {
                return member;
            }
        }
        return null;
    }

    /**
     * @param location
     * @param predicate
     * @return an Iterable of all items counter-clock wise in the ring from (but
     *         excluding) start location to (but excluding) the first item where
     *         predicate(item) evaluates to True.
     */
    public Iterable<T> predecessors(HashKey location, Predicate<T> predicate) {
        Iterator<T> tail = ring.tailMap(location, false).descendingMap().values().iterator();
        Iterator<T> head = ring.headMap(location, false).descendingMap().values().iterator();

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
     * @param start
     * @param predicate
     * @return an Iterable of all items counter-clock wise in the ring from (but
     *         excluding) start location to (but excluding) the first item where
     *         predicate(item) evaluates to True.
     */
    public Iterable<T> predecessors(T start, Predicate<T> predicate) {
        return predecessors(hash(start), predicate);
    }

    /**
     * @return the number of items between item and dest
     */
    public int rank(HashKey item, HashKey dest) {
        if (item.compareTo(dest) < 0) {
            return ring.subMap(item, false, dest, false).size();
        }
        return ring.tailMap(item, false).size() + ring.headMap(dest, false).size();
    }

    /**
     * @return the number of items between item and dest
     */
    public int rank(HashKey item, T dest) {
        return rank(item, hash(dest));
    }

    /**
     * @return the number of items between item and dest
     */
    public int rank(T item, T dest) {
        HashKey itemHash = hash(item);
        HashKey destHash = hash(dest);
        return rank(itemHash, destHash);
    }

    public int size() {
        return ring.size();
    }

    /**
     * @param start
     * @param predicate
     * @return a Stream of all items counter-clock wise in the ring from (but
     *         excluding) start location to (but excluding) the first item where
     *         predicate(item) evaluates to True.
     */
    public Stream<T> streamPredecessors(HashKey location, Predicate<T> predicate) {
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
        return StreamSupport.stream(predecessors(hash(m), predicate).spliterator(), false);
    }

    /**
     * @param start
     * @param predicate
     * @return a Stream of all items counter-clock wise in the ring from (but
     *         excluding) start location to (but excluding) the first item where
     *         predicate(item) evaluates to True.
     */
    public Stream<T> streamSuccessors(HashKey location, Predicate<T> predicate) {
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
        return StreamSupport.stream(successors(hash(m), predicate).spliterator(), false);
    }

    /**
     * @param start
     * @param predicate
     * @return a iterable of all items counter-clock wise in the ring from (but
     *         excluding) start location to (but excluding) the first item where
     *         predicate(item) evaluates to True.
     */
    public T successor(HashKey hash) {
        return successor(hash, e -> true);
    }

    /**
     * @param hash      - the location to start on the ring
     * @param predicate - the test predicate
     * @return the first successor of m for which predicate evaluates to True. m is
     *         never evaluated..
     */
    public T successor(HashKey hash, Predicate<T> predicate) {

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
        return successor(hash(m), predicate);
    }

    /**
     * @param start
     * @param predicate
     * @return an Iterable of all items counter-clock wise in the ring from (but
     *         excluding) start location to (but excluding) the first item where
     *         predicate(item) evaluates to True.
     */
    public Iterable<T> successors(HashKey location, Predicate<T> predicate) {
        Iterator<T> tail = ring.tailMap(location, false).values().iterator();
        Iterator<T> head = ring.headMap(location, false).values().iterator();

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

    /**
     * @param start
     * @param predicate
     * @return an Iterable of all items counter-clock wise in the ring from (but
     *         excluding) start item to (but excluding) the first item where
     *         predicate(item) evaluates to True.
     */
    public Iterable<T> successors(T m, Predicate<T> predicate) {
        return successors(hash(m), predicate);
    }

    @Override
    public String toString() {
        return "Ring[" + index + "] : " + ring;
    }

    /**
     * @param member
     * @return the iteratator to traverse the ring starting at the member
     */
    public Iterable<T> traverse(T member) {
        HashKey hash = hash(member);
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

    protected void delete(T m) {
        ring.remove(hash(m));
    }

    protected HashKey hash(T m) {
        return context.hashFor(m, index);
    }

    protected T insert(T m) {
        return ring.put(hash(m), m);
    }

    /**
     * for testing
     * 
     * @return
     */
    public Map<HashKey, T> getRing() {
        return ring;
    }
}
