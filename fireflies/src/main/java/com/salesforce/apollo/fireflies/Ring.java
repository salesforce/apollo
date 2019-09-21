/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

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
 * A ring of members. Also, too, addressable functions by HashKey, for ring operations to obtain members.
 * 
 * @author hal.hildebrand
 * @since 220
 */
public class Ring implements Iterable<Member> {
    private final int index;
    private final ConcurrentNavigableMap<HashKey, Member> ring = new ConcurrentSkipListMap<>();

    public Ring(int index) {
        this.index = index;
    }

    /**
     * @param start
     * @param stop
     * @return Return all counter-clockwise items between (but not including) start and stop
     */
    public Iterable<Member> betweenPredecessors(Member start, Member stop) {
        if (start.equals(stop)) {
            return Collections.emptyList();
        }
        HashKey startHash = hash(start);
        HashKey stopHash = hash(stop);

        if (startHash.compareTo(stopHash) < 0) {
            Iterator<Member> head = ring.headMap(startHash, false).descendingMap().values().iterator();
            Iterator<Member> tail = ring.tailMap(stopHash, false).descendingMap().values().iterator();
            return new Iterable<Member>() {

                @Override
                public Iterator<Member> iterator() {
                    return new Iterator<Member>() {

                        @Override
                        public boolean hasNext() {
                            return tail.hasNext() || head.hasNext();
                        }

                        @Override
                        public Member next() {
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
    public Iterable<Member> betweenSuccessor(Member start, Member stop) {
        if (start.equals(stop)) {
            return Collections.emptyList();
        }
        HashKey startHash = hash(start);
        HashKey stopHash = hash(stop);
        if (startHash.compareTo(stopHash) < 0) {
            return ring.subMap(startHash, false, stopHash, false).values();
        }

        ConcurrentNavigableMap<HashKey, Member> headMap = ring.headMap(stopHash, false);
        ConcurrentNavigableMap<HashKey, Member> tailMap = ring.tailMap(startHash, false);

        Iterator<Member> head = headMap.values().iterator();
        Iterator<Member> tail = tailMap.values().iterator();
        return new Iterable<Member>() {

            @Override
            public Iterator<Member> iterator() {
                return new Iterator<Member>() {

                    @Override
                    public boolean hasNext() {
                        return tail.hasNext() || head.hasNext();
                    }

                    @Override
                    public Member next() {
                        return tail.hasNext() ? tail.next() : head.next();
                    }
                };
            }
        };
    }

    public int getIndex() {
        return index;
    }

    public Member insert(Member m) {
        return ring.put(hash(m), m);
    }

    /**
     * <pre>
     * Please note the following semantic:
     *
     *    - An item lies between itself. That is, if pred == itm == succ, True is
     *    returned.
     *
     *    - Everything lies between an item and item and itself. That is, if pred == succ, then 
     *    this method always returns true.
     *              
     *    - An item is always between itself and any other item. That is, if
     *    pred == item, or succ == item, this method returns True.
     *
     *    - Raises a ValueError if succ, pred, or item is not in the ring.
     * </pre>
     * 
     * @param predecessor
     *            - the asserted predecessor on the ring
     * @param item
     *            - the item to test
     * @param successor
     *            - the asserted successor on the ring
     * @return true if the member m is between the pred and succ members on the ring
     */
    public boolean isBetween(Member predecessor, Member item, Member successor) {
        if (predecessor.equals(item) || successor.equals(item)) {
            return true;
        }
        HashKey predHash = hash(predecessor);
        HashKey memberHash = hash(item);
        HashKey succHash = hash(successor);
        return predHash.compareTo(memberHash) < 0 && memberHash.compareTo(succHash) < 0;
    }

    @Override
    public Iterator<Member> iterator() {
        return ring.values().iterator();
    }

    /**
     * @param m
     *            - the member
     * @return the predecessor of the member
     */
    public Member predecessor(Member m) {
        return predecessor(m, e -> true);
    }

    /**
     * @param m
     *            - the member
     * @param predicate
     *            - the test predicate
     * @return the first predecessor of m for which predicate evaluates to True. m is never evaluated.
     */
    public Member predecessor(Member m, Predicate<Member> predicate) {
        HashKey itemHash = hash(m);

        for (Member member : ring.headMap(itemHash, false).descendingMap().values()) {
            if (predicate.test(member)) {
                return member;
            }
        }
        for (Member member : ring.tailMap(itemHash, false).descendingMap().values()) {
            if (predicate.test(member)) {
                return member;
            }
        }
        return null;
    }

    /**
     * @param location
     * @param predicate
     * @return an Iterable of all items counter-clock wise in the ring from (but excluding) start location to (but
     *         excluding) the first item where predicate(item) evaluates to True.
     */
    public Iterable<Member> predecessors(HashKey location, Predicate<Member> predicate) {
        Iterator<Member> tail = ring.tailMap(location, false).descendingMap().values().iterator();
        Iterator<Member> head = ring.headMap(location, false).descendingMap().values().iterator();

        Iterator<Member> iterator = new Iterator<Member>() {
            private Member next = nextMember();

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public Member next() {
                if (next == null) {
                    throw new NoSuchElementException();
                }
                Member current = next;
                next = nextMember();
                return current;
            }

            private Member nextMember() {
                while (head.hasNext()) {
                    Member next = head.next();
                    return predicate.test(next) ? null : next;
                }
                while (tail.hasNext()) {
                    Member next = tail.next();
                    return predicate.test(next) ? null : next;
                }
                return null;
            }
        };
        return new Iterable<Member>() {

            @Override
            public Iterator<Member> iterator() {
                return iterator;
            }
        };
    }

    /**
     * @param start
     * @param predicate
     * @return an Iterable of all items counter-clock wise in the ring from (but excluding) start location to (but
     *         excluding) the first item where predicate(item) evaluates to True.
     */
    public Iterable<Member> predecessors(Member start, Predicate<Member> predicate) {
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
    public int rank(HashKey item, Member dest) {
        return rank(item, hash(dest));
    }

    /**
     * @return the number of items between item and dest
     */
    public int rank(Member item, Member dest) {
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
     * @return a Stream of all items counter-clock wise in the ring from (but excluding) start location to (but
     *         excluding) the first item where predicate(item) evaluates to True.
     */
    public Stream<Member> streamPredecessors(HashKey location, Predicate<Member> predicate) {
        return StreamSupport.stream(predecessors(location, predicate).spliterator(), false);
    }

    /**
     * @param start
     * @param predicate
     * @return a list of all items counter-clock wise in the ring from (but excluding) start item to (but excluding) the
     *         first item where predicate(item) evaluates to True.
     */
    public Stream<Member> streamPredecessors(Member m, Predicate<Member> predicate) {
        return StreamSupport.stream(predecessors(hash(m), predicate).spliterator(), false);
    }

    /**
     * @param start
     * @param predicate
     * @return a Stream of all items counter-clock wise in the ring from (but excluding) start location to (but
     *         excluding) the first item where predicate(item) evaluates to True.
     */
    public Stream<Member> streamSuccessors(HashKey location, Predicate<Member> predicate) {
        return StreamSupport.stream(successors(location, predicate).spliterator(), false);
    }

    /**
     * @param start
     * @param predicate
     * @return a Stream of all items counter-clock wise in the ring from (but excluding) start item to (but excluding)
     *         the first item where predicate(item) evaluates to True.
     */
    public Stream<Member> streamSuccessors(Member m, Predicate<Member> predicate) {
        return StreamSupport.stream(successors(hash(m), predicate).spliterator(), false);
    }

    /**
     * @param start
     * @param predicate
     * @return a iterable of all items counter-clock wise in the ring from (but excluding) start location to (but
     *         excluding) the first item where predicate(item) evaluates to True.
     */
    public Member successor(HashKey hash) {
        return successor(hash, e -> true);
    }

    /**
     * @param hash
     *            - the location to start on the ring
     * @param predicate
     *            - the test predicate
     * @return the first successor of m for which predicate evaluates to True. m is never evaluated..
     */
    public Member successor(HashKey hash, Predicate<Member> predicate) {

        for (Member member : ring.tailMap(hash, false).values()) {
            if (predicate.test(member)) {
                return member;
            }
        }
        for (Member member : ring.headMap(hash, false).values()) {
            if (predicate.test(member)) {
                return member;
            }
        }
        return null;
    }

    /**
     * @param m
     *            - the member
     * @return the successor of the member
     */
    public Member successor(Member m) {
        return successor(m, e -> true);
    }

    /**
     * @param m
     *            - the member
     * @param predicate
     *            - the test predicate
     * @return the first successor of m for which predicate evaluates to True. m is never evaluated..
     */
    public Member successor(Member m, Predicate<Member> predicate) {
        return successor(hash(m), predicate);
    }

    /**
     * @param start
     * @param predicate
     * @return an Iterable of all items counter-clock wise in the ring from (but excluding) start location to (but
     *         excluding) the first item where predicate(item) evaluates to True.
     */
    public Iterable<Member> successors(HashKey location, Predicate<Member> predicate) {
        Iterator<Member> tail = ring.tailMap(location, false).values().iterator();
        Iterator<Member> head = ring.headMap(location, false).values().iterator();

        Iterator<Member> iterator = new Iterator<Member>() {
            private Member next = nextMember();

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public Member next() {
                if (next == null) {
                    throw new NoSuchElementException();
                }
                Member current = next;
                next = nextMember();
                return current;
            }

            private Member nextMember() {
                while (tail.hasNext()) {
                    Member next = tail.next();
                    return predicate.test(next) ? null : next;
                }
                while (head.hasNext()) {
                    Member next = head.next();
                    return predicate.test(next) ? null : next;
                }
                return null;
            }
        };
        return new Iterable<Member>() {

            @Override
            public Iterator<Member> iterator() {
                return iterator;
            }
        };
    }

    /**
     * @param start
     * @param predicate
     * @return an Iterable of all items counter-clock wise in the ring from (but excluding) start item to (but
     *         excluding) the first item where predicate(item) evaluates to True.
     */
    public Iterable<Member> successors(Member m, Predicate<Member> predicate) {
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
    public Iterable<Member> traverse(Member member) {
        HashKey hash = hash(member);
        Iterator<Member> head = ring.headMap(hash, false).values().iterator();
        Iterator<Member> tail = ring.tailMap(hash, false).values().iterator();
        return new Iterable<Member>() {

            @Override
            public Iterator<Member> iterator() {
                return new Iterator<Member>() {

                    @Override
                    public boolean hasNext() {
                        return tail.hasNext() || head.hasNext();
                    }

                    @Override
                    public Member next() {
                        return tail.hasNext() ? tail.next() : head.next();
                    }
                };
            }
        };
    }

    protected HashKey hash(Member m) {
        return m.hashFor(index);
    }

    /**
     * for testing
     * 
     * @return
     */
    Map<HashKey, Member> getRing() {
        return ring;
    }
}
