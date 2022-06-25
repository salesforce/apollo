/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import static com.salesforce.apollo.ethereal.PreUnit.decode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.ethereal.proto.PreUnit_s;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;

/**
 * @author hal.hildebrand
 *
 */
public interface Dag {
    public class DagImpl implements Dag {

        private final List<BiFunction<Unit, Dag, Correctness>> checks     = new ArrayList<>();
        private final Config                                   config;
        private final int                                      epoch;
        private final fiberMap                                 heightUnits;
        private final fiberMap                                 levelUnits;
        private final List<Unit>                               maxUnits;
        private final List<Consumer<Unit>>                     postInsert = new ArrayList<>();
        private final List<Consumer<Unit>>                     preInsert  = new ArrayList<>();
        private final ReadWriteLock                            rwLock     = new ReentrantReadWriteLock(true);
        private final Map<Digest, Unit>                        units      = new HashMap<>();

        /**
         * @param config
         * @param epoch
         */
        public DagImpl(Config config, int epoch) {
            this.config = config;
            this.epoch = epoch;
            levelUnits = Dag.newFiberMap(config.nProc(), config.epochLength());
            heightUnits = Dag.newFiberMap(config.nProc(), config.epochLength());
            maxUnits = Dag.newSlottedUnits(config.nProc());
        }

        @Override
        public void addCheck(BiFunction<Unit, Dag, Correctness> checker) {
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
        public Unit build(PreUnit base, Unit[] parents) {
            assert parents.length == config.nProc();
            return base.from(parents, config.bias());
        }

        @Override
        public Correctness check(Unit u) {
            for (var check : checks) {
                var err = check.apply(u, this);
                if (err != null) {
                    return err;
                }
            }
            return null;
        }

        @Override
        public boolean contains(Digest digest) {
            return read(() -> units.containsKey(digest));
        }

        @Override
        public boolean contains(long id) {
            return read(() -> {
                var decoded = decode(id);
                if (decoded.epoch() != epoch) {
                    log.trace("Does not contain: {} wrong epoch: {} on: {}", decoded, epoch, config.logLabel());
                    return null;
                }
                var fiber = heightUnits.getFiber(decoded.height());
                if (fiber == null) {
                    log.trace("Does not contain: {} no height fiber on: {}", decoded, config.logLabel());
                    return false;
                }
                if (fiber.get(decoded.creator()) == null) {
                    log.trace("Does not contain: {} on: {}", decoded, config.logLabel());
                    return false;
                }

                return true;
            });
        }

        @Override
        public Decoded decodeParents(PreUnit pu) {
            return read(() -> {
                var u = get(pu.hash());
                if (u != null) {
                    return new DuplicateUnit(u);
                }
                var heights = pu.view().heights();
                var possibleParents = heightUnits.get(heights);
                if (possibleParents.unknown() > 0) {
                    return new UnknownParents(possibleParents.unknown());
                }
                Unit[] parents = new Unit[config.nProc()];

                int i = -1;
                for (Unit unit : possibleParents.result()) {
                    i++;
                    if (heights[i] == -1) {
                        continue;
                    }
                    parents[i] = unit;
                }
                return new DecodedR(parents);
            });
        }

        @Override
        public int epoch() {
            return epoch;
        }

        @Override
        public Unit get(Digest digest) {
            return read(() -> units.get(digest));
        }

        @Override
        public List<Unit> get(List<Digest> digests) {
            return read(() -> digests.stream().map(e -> units.get(e)).toList());
        }

        @Override
        public Unit get(long id) {
            return read(() -> {
                var decoded = decode(id);
                if (decoded.epoch() != epoch) {
                    return null;
                }
                var fiber = heightUnits.getFiber(decoded.height());

                return fiber == null ? null : fiber.get(decoded.creator());
            });
        }

        @Override
        public void have(DigestBloomFilter biff) {
            read(() -> {
                units.entrySet()
                     .stream()
                     .filter(e -> e.getValue().epoch() == epoch)
                     .map(e -> e.getKey())
                     .forEach(d -> biff.add(d));
            });
        }

        @Override
        public void insert(Unit v) {
            if (v.epoch() != epoch) {
                throw new IllegalStateException("Invalid insert of: " + v + " into epoch: " + epoch + " on: "
                + config.logLabel());
            }
            write(() -> {
                var unit = v.embed(this);
                for (var hook : preInsert) {
                    hook.accept(unit);
                }
                updateUnitsOnHeight(unit);
                updateUnitsOnLevel(unit);
                units.put(unit.hash(), unit);
                updateMaximal(unit);
                log.trace("Inserted: {}:{} on: {}", v.hash(), v, config.logLabel());
                for (var hook : postInsert) {
                    hook.accept(unit);
                }
            });
        }

        @Override
        public boolean isQuorum(short cardinality) {
            return cardinality >= Dag.minimalQuorum(nProc(), config.bias());
        }

        @Override
        public void iterateMaxUnitsPerProcess(Consumer<Unit> work) {
            read(() -> maximalUnitsPerProcess().forEach(work));
        }

        @Override
        public void iterateUnits(Function<Unit, Boolean> consumer) {
            read(() -> {
                for (Unit u : units.values()) {
                    if (!consumer.apply(u)) {
                        break;
                    }
                }
            });
        }

        @Override
        public void iterateUnitsOnLevel(int level, Function<Unit, Boolean> work) {
            read(() -> {
                for (var u : unitsOnLevel(level)) {
                    if (u != null && !work.apply(u)) {
                        return;
                    }
                }
            });
        }

        @Override
        public List<Unit> maximalUnitsPerProcess() {
            return read(() -> new ArrayList<>(maxUnits));
        }

        /** returns the maximal level of a unit in the dag. */
        @Override
        public int maxLevel() {
            return read(() -> {
                AtomicInteger maxLevel = new AtomicInteger(-1);
                maximalUnitsPerProcess().forEach(unit -> {
                    if (unit != null && unit.level() > maxLevel.get()) {
                        maxLevel.set(unit.level());
                    }
                });
                return maxLevel.get();
            });
        }

        @Override
        public DagInfo maxView() {
            return read(() -> {
                var maxes = maximalUnitsPerProcess();
                var heights = new ArrayList<Integer>();
                maxes.forEach(unit -> {
                    var h = -1;
                    if (unit != null && unit.height() > h) {
                        h = unit.height();
                    }
                    heights.add(h);
                });
                return new DagInfo(epoch(), heights.stream().mapToInt(i -> i).toArray());
            });
        }

        @Override
        public void missing(BloomFilter<Digest> have, List<PreUnit_s> missing) {
            read(() -> {
                units.entrySet().forEach(e -> {
                    if (!have.contains(e.getKey())) {
                        missing.add(e.getValue().toPreUnit_s());
                    }
                });
            });
        }

        @Override
        public void missing(BloomFilter<Digest> have, Map<Digest, PreUnit_s> missing) {
            read(() -> {
                units.entrySet().forEach(e -> {
                    if (e.getValue().epoch() == epoch && !have.contains(e.getKey())) {
                        missing.computeIfAbsent(e.getKey(), h -> e.getValue().toPreUnit_s());
                    }
                });
            });
        }

        @Override
        public short nProc() {
            return config.nProc();
        }

        @Override
        public short pid() {
            return config.pid();
        }

        @Override
        public <T> T read(Callable<T> call) {
            final Lock lock = rwLock.readLock();
            lock.lock();
            try {
                return call.call();
            } catch (Exception e) {
                throw new IllegalStateException("Error during read locked call on: " + config.logLabel(), e);
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void read(Runnable r) {
            final Lock lock = rwLock.readLock();
            lock.lock();
            try {
                r.run();
            } catch (Exception e) {
                throw new IllegalStateException("Error during read locked call on: " + config.logLabel(), e);
            } finally {
                lock.unlock();
            }
        }

        /**
         * return all units present in dag that are above (in height sense) given
         * heights. When called with null argument, returns all units in the dag. Units
         * returned by this method are in random order.
         */
        @Override
        public List<Unit> unitsAbove(int[] heights) {
            return read(() -> {
                if (heights == null) {
                    return units.values().stream().toList();
                }
                return heightUnits.above(heights);
            });
        }

        /**
         * returns the prime units at the requested level, indexed by their creator ids.
         */
        @Override
        public List<Unit> unitsOnLevel(int level) {
            return read(() -> {
                var res = levelUnits.getFiber(level);
                return res != null ? new ArrayList<>(res) : emptySlotList(config.nProc());
            });
        }

        @Override
        public void write(Runnable r) {
            final Lock lock = rwLock.writeLock();
            lock.lock();
            try {
                r.run();
            } catch (Exception e) {
                throw new IllegalStateException("Error during write locked call on: " + config.logLabel(), e);
            } finally {
                lock.unlock();
            }
        }

        private List<Unit> emptySlotList(short nProc) {
            var arr = new ArrayList<Unit>();
            for (var i = 0; i < nProc; i++) {
                arr.add(null);
            }
            return arr;
        }

        private void updateMaximal(Unit u) {
            var creator = u.creator();
            var maxByCreator = maxUnits.get(creator);
            if (maxByCreator == null || u.above(maxByCreator)) {
                maxUnits.set(creator, u);
            }

        }

        private void updateUnitsOnHeight(Unit u) {
            var height = u.height();
            var creator = u.creator();
            if (height >= heightUnits.len().get()) {
                heightUnits.extendBy(Math.max(10, height - heightUnits.len().get()));
            }
            var su = heightUnits.getFiber(height);
            su.set(creator, u);
        }

        private void updateUnitsOnLevel(Unit u) {
            if (u.level() >= levelUnits.len().get()) {
                levelUnits.extendBy(10);
            }
            var su = levelUnits.getFiber(u.level());
            var creator = u.creator();
            su.set(creator, u);

        }
    }

    record DagInfo(int epoch, int[] heights) {}

    public interface Decoded {
        default Correctness classification() {
            return Correctness.CORRECT;
        }

        default boolean inError() {
            return true;
        }

        default Unit[] parents() {
            return new Unit[0];
        }
    }

    public record DecodedR(Unit[] parents) implements Decoded {
        @Override
        public boolean inError() {
            return false;
        }
    }

    record fiberMap(List<List<Unit>> content, short width, AtomicInteger len) {

        public List<Unit> getFiber(int value) {
            return value < 0 || value >= content.size() ? null : content.get(value);
        }

        public int length() {
            return len.get();
        }

        public void extendBy(int nValues) {
            for (int i = len.get(); i < len.get() + nValues; i++) {
                content.add(Dag.newSlottedUnits(width));
            }
            len.addAndGet(nValues);
        }

        public record getResult(List<Unit> result, int unknown) {}

        /**
         * get takes a list of heights (of length nProc) and returns a slice (of length
         * nProc) of slices of corresponding units. The second returned value is the
         * number of unknown units (no units for that creator-height pair).
         */
        public getResult get(int[] heights) {
            if (heights.length != width) {
                throw new IllegalStateException("Wrong number of heights passed to fiber map: " + heights.length
                + " expected: " + width);
            }
            List<Unit> result = IntStream.range(0, width).mapToObj(e -> (Unit) null).collect(Collectors.toList());
            var unknown = 0;
            for (short pid = 0; pid < heights.length; pid++) {
                var h = heights[pid];
                if (h == -1) {
                    continue;
                }
                var su = (h < content.size()) ? content.get(h) : null;
                if (su != null) {
                    result.set(pid, (su.get(pid)));
                }
                if (result.get(pid) == null) {
                    unknown++;
                }

            }
            return new getResult(result, unknown);
        }

        public List<Unit> above(int[] heights) {
            if (heights.length != width) {
                throw new IllegalStateException("Incorrect number of heights");
            }
            var min = heights[0];
            for (int i = 1; i < heights.length; i++) {
                if (heights[i] < min) {
                    min = heights[i];
                }
            }
            var result = new ArrayList<Unit>();
            for (int height = min + 1; height < length(); height++) {
                var su = content.get(height);
                for (short i = 0; i < width; i++) {
                    if (height > heights[i]) {
                        result.add(su.get(i));
                    }
                }
            }
            return result;
        }
    }

    record AmbiguousParents(List<Unit> units) implements Decoded {

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

    static final Logger log = LoggerFactory.getLogger(Dag.class);

    public static List<Unit> newSlottedUnits(short nProc) {
        var arr = new ArrayList<Unit>();
        for (var i = 0; i < nProc; i++) {
            arr.add(null);
        }
        return arr;
    }

    static short minimalQuorum(short np, double bias) {
        var nProcesses = (double) np;
        short minimalQuorum = (short) (nProcesses - 1 - (nProcesses / bias) + 1);
        return minimalQuorum;
    }

    static Dag newDag(Config config, int epoch) {
        log.trace("New dag for epoch: {} on: {}", epoch, config.logLabel());
//        return new dag(config.nProc(), epoch, new ConcurrentHashMap<>(),
//                       newFiberMap(config.nProc(), config.epochLength()),
//                       newFiberMap(config.nProc(), config.epochLength()), newSlottedUnits(config.nProc()),
//                       config.checks(), new ArrayList<>(), new ArrayList<>(), config.bias());
        return new DagImpl(config, epoch);
    }

    static fiberMap newFiberMap(short width, int initialLength) {
        var newMap = new fiberMap(new ArrayList<List<Unit>>(), width, new AtomicInteger(initialLength));
        for (int i = 0; i < initialLength; i++) {
            newMap.content.add(Dag.newSlottedUnits(width));
        }
        return newMap;
    }

    static short threshold(short np) {
        var nProcesses = (double) np;
        short minimalTrusted = (short) ((nProcesses - 1.0) / 3.0);
        return minimalTrusted;
    }

    void addCheck(BiFunction<Unit, Dag, Correctness> checker);

    void afterInsert(Consumer<Unit> h);

    void beforeInsert(Consumer<Unit> h);

    Unit build(PreUnit base, Unit[] parents);

    Correctness check(Unit u);

    boolean contains(Digest digest);

    boolean contains(long parentID);

    /** return a slce of parents of the specified unit if control hash matches */
    Decoded decodeParents(PreUnit unit);

    int epoch();

    Unit get(Digest digest);

    List<Unit> get(List<Digest> digests);

    Unit get(long id);

    void have(DigestBloomFilter biff);

    void insert(Unit u);

    boolean isQuorum(short cardinality);

    void iterateMaxUnitsPerProcess(Consumer<Unit> work);

    void iterateUnits(Function<Unit, Boolean> consumer);

    void iterateUnitsOnLevel(int level, Function<Unit, Boolean> work);

    List<Unit> maximalUnitsPerProcess();

    /** returns the maximal level of a unit in the dag. */
    int maxLevel();

    DagInfo maxView();

    void missing(BloomFilter<Digest> have, List<PreUnit_s> missing);

    void missing(BloomFilter<Digest> have, Map<Digest, PreUnit_s> missing);

    short nProc();

    short pid();

    <T> T read(Callable<T> c);

    void read(Runnable r);

    List<Unit> unitsAbove(int[] heights);

    List<Unit> unitsOnLevel(int level);

    void write(Runnable r);
}
