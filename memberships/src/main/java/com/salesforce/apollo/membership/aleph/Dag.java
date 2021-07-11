/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.aleph;

import static com.salesforce.apollo.membership.aleph.Dag.minimalQuorum;
import static com.salesforce.apollo.membership.aleph.PreUnit.decode;
import static com.salesforce.apollo.membership.aleph.SlottedUnits.newSlottedUnits;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
@SuppressWarnings("unused")
public interface Dag {

    record DagInfo(int epoch, List<Integer> heights) {}

    record fiberMap(Map<Integer, SlottedUnits> content, short width, AtomicInteger len, ReadWriteLock mx) {

        public SlottedUnits getFiber(int value) {
            final Lock lock = mx.readLock();
            try {
                return content.get(value);
            } finally {
                lock.unlock();
            }
        }

        public int length() {
            final Lock lock = mx.readLock();
            try {
                return len.get();
            } finally {
                lock.unlock();
            }
        }

        public void extendBy(int nValues) {
            final Lock lock = mx.writeLock();
            try {
                for (int i = 0; i < len.get() + nValues; i++) {
                    content.put(i, newSlottedUnits(width));
                }
                len.addAndGet(nValues);
            } finally {
                lock.unlock();
            }
        }

        public record getResult(List<List<Unit>> result, int unknown) {}

        // get takes a list of heights (of length nProc) and returns a slice (of length
        // nProc) of slices of corresponding units. The second returned value is the
        // number of unknown units (no units for that creator-height pair).
        public getResult get(List<Integer> heights) {
            if (heights.size() != width) {
                throw new IllegalStateException("Wrong number of heights passed to fiber map");
            }
            var result = new ArrayList<List<Unit>>();
            var unknown = 0;
            final Lock lock = mx.readLock();
            lock.lock();
            try {
                for (short pid = 0; pid < heights.size(); pid++) {
                    var h = heights.get(pid);
                    if (h == -1) {
                        continue;
                    }
                    var su = content.get(h);
                    if (su != null) {
                        result.add(su.get(pid));
                    }
                    if (result.get(pid).isEmpty()) {
                        unknown++;
                    }

                }
                return new getResult(result, unknown);
            } finally {
                lock.unlock();
            }
        }

        public List<Unit> above(List<Integer> heights) {
            if (heights.size() != width) {
                throw new IllegalStateException("Incorrect number of heights");
            }
            var min = heights.get(0);
            for (int h : heights.subList(1, heights.size())) {
                if (h < min) {
                    min = h;
                }
            }
            var result = new ArrayList<Unit>();
            final Lock lock = mx.readLock();
            lock.lock();
            try {
                for (int height = min + 1; height < length(); height++) {
                    var su = content.get(height);
                    for (short i = 0; i < width; i++) {
                        if (height > heights.get(i)) {
                            result.addAll(su.get(i));
                        }
                    }
                }
                return result;
            } finally {
                lock.unlock();
            }
        }
    }

    record dag(short nProc, int epoch, ConcurrentMap<Digest, Unit> units, fiberMap levelUnits, fiberMap heightUnits,
               SlottedUnits maxUnits, List<BiConsumer<Unit, Dag>> checks, List<Consumer<Unit>> preInsert,
               List<Consumer<Unit>> postInsert)
              implements Dag

    {

        @Override
        public void addCheck(BiConsumer<Unit, Dag> checker) {
            checks.add(checker);
        }

        @Override
        public void afterInsert(Consumer<Unit> h) {
            postInsert.add(h);
        }

        @Override
        public void beforeInsert(Consumer<Unit> h) {
            preInsert.add(h);
        }

        @Override
        public Unit build(PreUnit base, List<Unit> parents) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void check(Unit u) {
            // TODO Auto-generated method stub

        }

        @Override
        public List<Unit> decodeParents(PreUnit unit) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public int epoch() {
            return epoch;
        }

        @Override
        public Unit get(Digest digest) {
            return units.get(digest);
        }

        @Override
        public List<Unit> get(List<Digest> digests) {
            return digests.stream().map(e -> units.get(e)).toList();
        }

        @Override
        public List<Unit> get(long id) {
            var decoded = decode(id);
            if (decoded.epoch() != epoch) {
                return null;
            }
            var fiber = heightUnits.getFiber(decoded.height());
            return fiber == null ? null : fiber.get(decoded.creator());
        }

        @Override
        public void insert(Unit u) {
            // TODO Auto-generated method stub

        }

        @Override
        public boolean isQuorum(short cardinality) {
            return cardinality >= minimalQuorum(cardinality);
        }

        @Override
        public SlottedUnits maximalUnitsPerProcess() {
            return maxUnits;
        }

        @Override
        public short nProc() {
            return nProc;
        }

        // return all units present in dag that are above (in height sense)
        // given heights. When called with null argument, returns all units in the dag.
        // Units returned by this method are in random order.
        @Override
        public List<Unit> unitsAbove(List<Integer> heights) {
            if (heights == null) {
                return units.values().stream().toList();
            }
            return heightUnits.above(heights);
        }

        // returns the prime units at the requested level, indexed by their creator ids.
        @Override
        public SlottedUnits unitsOnLevel(int level) {
            var res = levelUnits.getFiber(level);
            return res != null ? res : newSlottedUnits(nProc);
        }

    }

    static short minimalQuorum(short nProcesses) {
        return (short) (nProcesses - nProcesses / 3);
    }

    static short minimalTrusted(short nProcesses) {
        return (short) ((nProcesses - 1) / 3 + 1);
    }

    static Dag newDag(Config config, int epoch) {
        return new dag(config.nProc(), epoch, new ConcurrentHashMap<>(),
                       newFiberMap(config.nProc(), config.epochLength()),
                       newFiberMap(config.nProc(), config.epochLength()), newSlottedUnits(config.nProc()),
                       new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    }

    static fiberMap newFiberMap(short width, int initialLength) {
        var newMap = new fiberMap(new HashMap<>(), width, new AtomicInteger(initialLength),
                                  new ReentrantReadWriteLock());
        for (int i = 0; i < initialLength; i++) {
            newMap.content.put(i, newSlottedUnits(width));
        }
        return newMap;
    }

    void addCheck(BiConsumer<Unit, Dag> checker);

    void afterInsert(Consumer<Unit> h);

    void beforeInsert(Consumer<Unit> h);

    Unit build(PreUnit base, List<Unit> parents);

    void check(Unit u);

    // return a slce of parents of the specified unit if control hash matches
    List<Unit> decodeParents(PreUnit unit);

    int epoch();

    Unit get(Digest digest);

    List<Unit> get(List<Digest> digests);

    List<Unit> get(long id);

    void insert(Unit u);

    boolean isQuorum(short cardinality);

    SlottedUnits maximalUnitsPerProcess();

    default DagInfo maxView() {
        var maxes = maximalUnitsPerProcess();
        var heights = new ArrayList<Integer>();
        maxes.iterate(units -> {
            var h = -1;
            for (Unit u : units) {
                if (u.height() > h) {
                    h = u.height();
                }
            }
            heights.add(h);
            return true;
        });
        return new DagInfo(epoch(), heights);
    }

    short nProc();

    List<Unit> unitsAbove(List<Integer> heights);

    SlottedUnits unitsOnLevel(int level);
}
