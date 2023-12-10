/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import static com.salesforce.apollo.ethereal.PreUnit.decode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
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

import com.salesforce.apollo.ethereal.proto.PreUnit_s;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.ethereal.PreUnit.DecodedId;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.bloomFilters.BloomFilter;
import com.salesforce.apollo.bloomFilters.BloomFilter.DigestBloomFilter;

/**
 * @author hal.hildebrand
 */
public interface Dag {
    static final Logger log = LoggerFactory.getLogger(Dag.class);

    static short threshold(int np) {
        var nProcesses = (double) np;
        short minimalTrusted = (short) ((nProcesses - 1.0) / 3.0);
        return minimalTrusted;
    }

    static boolean validate(int nProc) {
        var threshold = threshold(nProc);
        return (threshold * 3 + 1) == nProc;
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

    /** returns the maximal level of a unit in the dag. */
    int maxLevel();

    DagInfo maxView();

    List<Unit> maximalUnitsPerProcess();

    void missing(BloomFilter<Digest> have, List<PreUnit_s> missing);

    void missing(BloomFilter<Digest> have, Map<Digest, PreUnit_s> missing);

    short nProc();

    short pid();

    <T> T read(Callable<T> c);

    void read(Runnable r);

    List<Unit> unitsAbove(int[] heights);

    List<Unit> unitsOnLevel(int level);

    void write(Runnable r);

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

    public class DagImpl implements Dag {

        private final List<BiFunction<Unit, Dag, Correctness>> checks     = new ArrayList<>();
        private final Config                                   config;
        private final int                                      epoch;
        private final fiberMap                                 heightUnits;
        private final fiberMap                                 levelUnits;
        private final Unit[]                                   maxUnits;
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
            levelUnits = new fiberMap(config.nProc());
            heightUnits = new fiberMap(config.nProc());
            maxUnits = new Unit[config.nProc()];
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
                    return false;
                }
                return heightUnits.contains(decoded);
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
                return heightUnits.get(decoded);
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
                throw new IllegalStateException(
                "Invalid insert of: " + v + " into epoch: " + epoch + " on: " + config.logLabel());
            }
            write(() -> {
                var unit = v.embed(this);
                for (var hook : preInsert) {
                    hook.accept(unit);
                }
                heightUnits.updateHeight(unit);
                levelUnits.updateLevel(unit);
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
            return cardinality >= Context.minimalQuorum(nProc(), config.bias());
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
        public int maxLevel() {
            return read(() -> {
                int maxLevel = -1;
                for (Unit unit : maxUnits) {
                    if (unit != null && unit.level() > maxLevel) {
                        maxLevel = unit.level();
                    }

                }
                return maxLevel;
            });
        }

        @Override
        public DagInfo maxView() {
            return read(() -> {
                var heights = new int[config.nProc()];
                int i = 0;
                for (var u : maxUnits) {
                    var h = -1;
                    if (u != null && u.height() > h) {
                        h = u.height();
                    }
                    heights[i++] = h;
                }
                return new DagInfo(epoch(), heights);
            });
        }

        @Override
        public List<Unit> maximalUnitsPerProcess() {
            return read(() -> Arrays.asList(maxUnits));
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

        @Override
        public List<Unit> unitsAbove(int[] heights) {
            return read(() -> {
                if (heights == null) {
                    return units.values().stream().toList();
                }
                return heightUnits.above(heights);
            });
        }

        @Override
        public List<Unit> unitsOnLevel(int level) {
            return read(() -> levelUnits.on(level));
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

        private void updateMaximal(Unit u) {
            var creator = u.creator();
            var maxByCreator = maxUnits[creator];
            if (maxByCreator == null || u.above(maxByCreator)) {
                maxUnits[creator] = u;
            }

        }
    }

    public record DecodedR(Unit[] parents) implements Decoded {
        @Override
        public boolean inError() {
            return false;
        }
    }

    record DagInfo(int epoch, int[] heights) {
    }

    class fiberMap {
        private final List<Unit[]> content = new ArrayList<>();
        private final short        width;
        fiberMap(short width) {
            this.width = width;
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
                final var su = content.get(height);
                for (short i = 0; i < width; i++) {
                    if (height > heights[i]) {
                        result.add(su[i]);
                    }
                }
            }
            return result;
        }

        public boolean contains(DecodedId decoded) {
            if (decoded.height() >= content.size()) {
                return false;
            }
            final var fiber = content.get(decoded.height());
            return fiber[decoded.creator()] != null;
        }

        public Unit get(DecodedId decoded) {
            if (decoded.height() >= content.size()) {
                return null;
            }
            final var fiber = content.get(decoded.height());
            return fiber[decoded.creator()];
        }

        /**
         * get takes a list of heights (of length nProc) and returns a slice (of length nProc) of slices of
         * corresponding units. The second returned value is the number of unknown units (no units for that
         * creator-height pair).
         */
        public getResult get(int[] heights) {
            if (heights.length != width) {
                throw new IllegalStateException(
                "Wrong number of heights passed to fiber map: " + heights.length + " expected: " + width);
            }
            List<Unit> result = IntStream.range(0, width).mapToObj(e -> (Unit) null).collect(Collectors.toList());
            var unknown = 0;
            for (short pid = 0; pid < heights.length; pid++) {
                var h = heights[pid];
                if (h == -1) {
                    continue;
                }
                final var su = (h < content.size()) ? content.get(h) : null;
                if (su != null) {
                    result.set(pid, su[pid]);
                }
                if (result.get(pid) == null) {
                    unknown++;
                }

            }
            return new getResult(result, unknown);
        }

        public int length() {
            return content.size();
        }

        public List<Unit> on(int level) {
            if (level >= content.size()) {
                return Collections.emptyList();
            }
            final var fiber = content.get(level);
            return Arrays.asList(fiber);
        }

        public void updateHeight(Unit u) {
            assert u != null : "Cannot insert null unit";
            final var fiber = getFiber(u.height());
            if (fiber[u.creator()] == null) {
                fiber[u.creator()] = u;
            }
        }

        public void updateLevel(Unit u) {
            assert u != null : "Cannot insert null unit";
            final var fiber = getFiber(u.level());
            if (fiber[u.creator()] == null) {
                fiber[u.creator()] = u;
            }
        }

        private Unit[] getFiber(int height) {
            if (content.size() < height + 1) {
                for (var i = content.size() - 1; i < height + 1; i++) {
                    content.add(new Unit[width]);
                }
            }
            return content.get(height);
        }

        public record getResult(List<Unit> result, int unknown) {
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
}
