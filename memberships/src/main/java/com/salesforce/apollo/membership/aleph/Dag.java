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
import java.util.Collections;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.aleph.Adder.Correctness;

/**
 * @author hal.hildebrand
 *
 */
@SuppressWarnings("unused")
public interface Dag {

    record DagInfo(int epoch, List<Integer> heights) {}

    public interface Decoded {
        default Correctness classification() {
            return Correctness.CORRECT;
        }

        default boolean inError() {
            return true;
        }

        default List<Unit> parents() {
            return Collections.emptyList();
        }
    }

    public record DecodedR(List<Unit> parents) implements Decoded {
        @Override
        public boolean inError() {
            return false;
        }
    }

    record fiberMap(Map<Integer, SlottedUnits> content, short width, AtomicInteger len, ReadWriteLock mx) {

        public SlottedUnits getFiber(int value) {
            final Lock lock = mx.readLock();
            lock.lock();
            try {
                return content.get(value);
            } finally {
                lock.unlock();
            }
        }

        public int length() {
            final Lock lock = mx.readLock();
            lock.lock();
            try {
                return len.get();
            } finally {
                lock.unlock();
            }
        }

        public void extendBy(int nValues) {
            final Lock lock = mx.writeLock();
            lock.lock();
            try {
                for (int i = len.get(); i < len.get() + nValues; i++) {
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
            List<List<Unit>> result = IntStream.range(0, width).mapToObj(e -> new ArrayList<Unit>())
                                               .collect(Collectors.toList());
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
                        result.set(pid, (su.get(pid)));
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

    record AmbiguousParents(List<List<Unit>> units) implements Decoded {

        @Override
        public Correctness classification() {
            return Correctness.ABIGUOUS_PARENTS;
        }
    }

    record DuplicateUnit(Unit u) implements Decoded {

        @Override
        public Correctness classification() {
            return Correctness.DUPLICATE_UNIT;
        }
    }

    record UnknownParents(int unknown) implements Decoded {

        @Override
        public Correctness classification() {
            return Correctness.UNKNOWN_PARENTS;
        }
    }

    record dag(short nProc, int epoch, ConcurrentMap<Digest, Unit> units, fiberMap levelUnits, fiberMap heightUnits,
               SlottedUnits maxUnits, List<BiConsumer<Unit, Dag>> checks, List<Consumer<Unit>> preInsert,
               List<Consumer<Unit>> postInsert)
              implements Dag {

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
            return base.from(parents);
        }

        @Override
        public void check(Unit u) {
            for (var check : checks) {
                check(u);
            }
        }

        @Override
        public Decoded decodeParents(PreUnit pu) {
            var u = get(pu.hash());
            if (u != null) {
                return new DuplicateUnit(u);
            }
            var heights = pu.view().heights();
            var possibleParents = heightUnits.get(heights);
            if (possibleParents.unknown > 0) {
                return new UnknownParents(possibleParents.unknown);
            }
            List<Unit> parents = IntStream.range(0, nProc).mapToObj(e -> (Unit) null).collect(Collectors.toList());

            int i = -1;
            for (List<Unit> units : possibleParents.result) {
                i++;
                if (heights.get(i) == -1) {
                    continue;
                }
                if (units.size() > 1) {
                    return new AmbiguousParents(possibleParents.result);
                }
                parents.set(i, units.get(0));
            }
            return new DecodedR(parents);
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
        public void insert(Unit v) {
            var unit = v.embed(this);
            for (var hook : preInsert) {
                hook.accept(unit);
            }
            updateUnitsOnHeight(unit);
            updateUnitsOnLevel(unit);
            units.put(unit.hash(), unit);
            updateMaximal(unit);
            for (var hook : postInsert) {
                hook.accept(unit);
            }
        }

        private void updateUnitsOnHeight(Unit u) {
            var height = u.height();
            var creator = u.creator();
            if (height >= heightUnits.len.get()) {
                heightUnits.extendBy(10);
            }
            var su = heightUnits.getFiber(height);

            var oldUnitsOnHeightByCreator = su.get(creator);
            var unitsOnHeightByCreator = new ArrayList<>(oldUnitsOnHeightByCreator);
            unitsOnHeightByCreator.add(u);
            su.set(creator, unitsOnHeightByCreator);

        }

        private void updateMaximal(Unit u) {
            var creator = u.creator();
            var maxByCreator = maxUnits.get(creator);
            var newMaxByCreator = new ArrayList<Unit>();
            // The below code works properly assuming that no unit in the dag created by
            // creator is >= u
            for (var v : maxByCreator) {
                if (!u.above(v)) {
                    newMaxByCreator.add(v);
                }
            }
            newMaxByCreator.add(u);
            maxUnits.set(creator, newMaxByCreator);

        }

        private void updateUnitsOnLevel(Unit u) {
            if (u.level() >= levelUnits.len.get()) {
                levelUnits.extendBy(10);
            }
            var su = levelUnits.getFiber(u.level());
            var creator = u.creator();
            var primesByCreator = new ArrayList<Unit>(su.get(creator));
            primesByCreator.add(u);
            su.set(creator, primesByCreator);

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

    private static fiberMap newFiberMap(short width, int initialLength) {
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
    Decoded decodeParents(PreUnit unit);

    int epoch();

    Unit get(Digest digest);

    List<Unit> get(List<Digest> digests);

    List<Unit> get(long id);

    void insert(Unit u);

    boolean isQuorum(short cardinality);

    SlottedUnits maximalUnitsPerProcess();

    default int maxLevel() {
        AtomicInteger maxLevel = new AtomicInteger(-1);
        maximalUnitsPerProcess().iterate(units -> {
            for (Unit v : units) {
                if (v.level() > maxLevel.get()) {
                    maxLevel.set(v.level());
                }
            }
            return true;
        });
        return maxLevel.get();
    }

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
