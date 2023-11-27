/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.ethereal.proto.Gossip;
import com.salesfoce.apollo.ethereal.proto.Missing;
import com.salesfoce.apollo.ethereal.proto.Update;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.ethereal.Dag.DagImpl;
import com.salesforce.apollo.ethereal.EpochProofBuilder.epochProofImpl;
import com.salesforce.apollo.ethereal.EpochProofBuilder.sharesDB;
import com.salesforce.apollo.ethereal.linear.Extender;
import com.salesforce.apollo.ethereal.linear.TimingRound;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @author hal.hildebrand
 */
public class Ethereal {

    private static final Logger log = LoggerFactory.getLogger(Ethereal.class);
    private final Config               config;
    private final ThreadPoolExecutor   consumer;
    private final Creator              creator;
    private final AtomicInteger        currentEpoch = new AtomicInteger(-1);
    private final Map<Integer, epoch>  epochs       = new ConcurrentHashMap<>();
    private final Set<Digest>          failed       = new ConcurrentSkipListSet<>();
    private final Queue<Unit>          lastTiming;
    private final int                  maxSerializedSize;
    private final Consumer<Integer>    newEpochAction;
    private final AtomicBoolean        started      = new AtomicBoolean();
    private final Consumer<List<Unit>> toPreblock;
    public Ethereal(Config config, int maxSerializedSize, DataSource ds, BiConsumer<List<ByteString>, Boolean> blocker,
                    Consumer<Integer> newEpochAction, String label) {
        this(config, maxSerializedSize, ds, blocker(blocker, config), newEpochAction, label);
    }
    public Ethereal(Config conf, int maxSerializedSize, DataSource ds, Consumer<List<Unit>> toPreblock,
                    Consumer<Integer> newEpochAction, String label) {
        if (!Dag.validate(conf.nProc())) {
            throw new IllegalArgumentException("Invalid # of processes, unable to build quorum: " + conf.nProc());
        }
        this.config = conf;
        this.lastTiming = new LinkedBlockingDeque<>();
        this.toPreblock = toPreblock;
        this.newEpochAction = newEpochAction;
        this.maxSerializedSize = maxSerializedSize;
        this.consumer = consumer(label);

        creator = new Creator(config, ds, lastTiming, u -> {
            assert u.creator() == config.pid();
            log.trace("Sending: {} on: {}", u, config.logLabel());
            insert(u);
        }, epoch -> new epochProofImpl(config, epoch, new sharesDB(config, new ConcurrentHashMap<>())));
    }

    private static ThreadPoolExecutor consumer(String label) {
        return new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS, new PriorityBlockingQueue<>(),
                                      Thread.ofVirtual().name("Ethereal Consumer[" + label + "]").factory(),
                                      (r, t) -> log.trace("Shutdown, cannot consume unit", t));
    }

    /**
     * return a preblock from a slice of units containing a timing round. It assumes that the timing unit is the last
     * unit in the slice, and that random source data of the timing unit starts with random bytes from the previous
     * level.
     */
    public static List<ByteString> toList(List<Unit> round) {
        var data = new ArrayList<ByteString>();
        for (Unit u : round) {
            if (!u.dealing()) {// data in dealing units doesn't come from users, these are new epoch proofs
                data.add(u.data());
            }
        }
        return data.isEmpty() ? null : data;
    }

    private static Consumer<List<Unit>> blocker(BiConsumer<List<ByteString>, Boolean> blocker, Config config) {
        return units -> {
            var print = log.isTraceEnabled() ? units.stream().map(e -> e.shortString()).toList() : null;
            log.trace("Make pre block: {} on: {}", print, config.logLabel());
            List<ByteString> preBlock = toList(units);
            var timingUnit = units.get(units.size() - 1);
            var last = false;
            if (timingUnit.level() == config.lastLevel() && timingUnit.epoch() == config.numberOfEpochs() - 1) {
                log.debug("Closing at last level: {} at epoch: {} on: {}", timingUnit.level(), timingUnit.epoch(),
                          config.logLabel());
                last = true;
            }
            if (preBlock != null) {

                log.trace("Emitting last: {} pre block: {} on: {}", last, print, config.logLabel());
                try {
                    blocker.accept(preBlock, last);
                } catch (Throwable t) {
                    log.error("Error consuming last: {} pre block: {} on: {}", last, print, config.logLabel(), t);
                }
            }
        };
    }

    public String dump() {
        var builder = new StringBuffer();
        builder.append("****************************").append('\n');
        epochs.keySet().stream().sorted().forEach(e -> {
            builder.append("Epoch: ")
                   .append(e)
                   .append('\n')
                   .append(epochs.get(e).adder.dump())
                   .append('\n')
                   .append('\n');
        });
        builder.append("****************************").append('\n').append('\n');
        return builder.toString();
    }

    public Processor processor() {
        return new Processor() {
            @Override
            public Gossip gossip(Digest context, int ring) {
                final var builder = Gossip.newBuilder().setRing(ring);
                final var current = currentEpoch.get();
                epochs.entrySet()
                      .stream()
                      .filter(e -> e.getKey() >= current)
                      .forEach(e -> builder.addHaves(e.getValue().adder().have()));
                return builder.build();
            }

            @Override
            public Update gossip(Gossip gossip) {
                final var builder = Update.newBuilder();
                final var haves = new HashSet<Integer>();
                gossip.getHavesList().forEach(have -> {
                    var epoch = retreiveEpoch(have.getEpoch());
                    if (epoch != null) {
                        haves.add(epoch.id());
                        builder.addMissings(epoch.adder().updateFor(have));
                    }
                });
                final var current = currentEpoch.get();
                epochs.entrySet()
                      .stream()
                      .filter(e -> e.getKey() >= current)
                      .filter(e -> !haves.contains(e.getKey()))
                      .forEach(e -> {
                          builder.addMissings(
                          Missing.newBuilder().setEpoch(e.getKey()).setHaves(e.getValue().adder().have()));
                      });
                return builder.build();
            }

            @Override
            public Update update(Update update) {
                final var builder = Update.newBuilder();
                final var current = currentEpoch.get();
                update.getMissingsList().forEach(missing -> {
                    var epoch = retreiveEpoch(missing.getEpoch());
                    if (epoch != null) {
                        final var adder = epoch.adder();
                        if (epoch.id() >= current) {
                            adder.updateFrom(missing);
                        }
                        builder.addMissings(adder.updateFor(missing.getHaves()));
                    }
                });
                return builder.build();
            }

            @Override
            public void updateFrom(Update update) {
                final var current = currentEpoch.get();
                update.getMissingsList().forEach(missing -> {
                    if (missing.getEpoch() >= current) {
                        var epoch = retreiveEpoch(missing.getEpoch());
                        if (epoch != null) {
                            epoch.adder().updateFrom(missing);
                        }
                    }
                });
            }
        };
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        newEpoch(0);
        creator.start();
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        log.trace("Stopping Ethereal on: {}", config.logLabel());
        consumer.getQueue().clear(); // Flush any pending consumers
        creator.stop();
        epochs.values().forEach(e -> e.close());
        epochs.clear();
        failed.clear();
        lastTiming.clear();
        log.trace("Ethereal stopped on: {}", config.logLabel());
    }

    private epoch createEpoch(int epoch) {
        Dag dg = new DagImpl(config, epoch);
        final var handleTimingRounds = handleTimingRounds();
        Extender ext = new Extender(dg, config);
        final var lastTU = new AtomicReference<TimingRound>();
        dg.afterInsert(u -> {
            if (!started.get()) {
                return;
            }

            final var current = lastTU.get();
            final var next = ext.chooseNextTimingUnits(current, handleTimingRounds);
            if (!lastTU.compareAndSet(current, next)) {
                throw new IllegalStateException(
                String.format("LastTU has been changed underneath us, expected: %s have: %s", current, next));
            }

            consumer.execute(new UnitTask(u, unit -> {
                if (!started.get()) {
                    return;
                }

                // creator already knows about units created by this node.
                if (unit.creator() != config.pid()) {
                    creator.consume(unit);
                }
            }));

        });
        final var adder = new Adder(epoch, dg, maxSerializedSize, config, failed);
        final var e = new epoch(epoch, dg, adder, new AtomicBoolean(true));
        return e;
    }

    private void finishEpoch(int epoch) {
        var ep = getEpoch(epoch);
        if (ep != null) {
            ep.noMoreUnits();
        }
    }

    private epochWithNewer getEpoch(int epoch) {
        final epoch e = epochs.get(epoch);
        final var currentId = currentEpoch.get();

        if (epoch == currentId) {
            return new epochWithNewer(e, false);
        }
        if (epoch > currentId) {
            return new epochWithNewer(e, true);
        }
        return new epochWithNewer(e, false);
    }

    /**
     * Waits for ordered round of units produced by Extenders and produces Preblocks based on them. Since Extenders in
     * multiple epochs can supply ordered rounds simultaneously, handleTimingRounds needs to ensure that Preblocks are
     * produced in ascending order with respect to epochs. For the last ordered round of the epoch, the timing unit
     * defining it is sent to the creator (to produce signature shares.)
     */
    private Consumer<List<Unit>> handleTimingRounds() {
        AtomicInteger current = new AtomicInteger(0);
        return round -> {
            var timingUnit = round.get(round.size() - 1);
            var epoch = timingUnit.epoch();

            if (timingUnit.level() >= config.lastLevel()) {
                log.trace("Last Timing: {}  on: {}", timingUnit, config.logLabel());
                lastTiming.add(timingUnit);
                finishEpoch(epoch);
            }
            if (epoch >= current.get() && timingUnit.level() <= config.lastLevel()) {
                toPreblock.accept(round);
                log.trace("Preblock produced: {} on: {}", timingUnit, config.logLabel());
            }
            current.set(epoch);
        };
    }

    /**
     * insert puts the provided unit directly into the corresponding epoch. If such epoch does not exist, creates it.
     * All correctness checks (epoch proof, adder, dag checks) are skipped. This method is meant for our own units
     * only.
     */
    private void insert(Unit unit) {
        if (unit.creator() != config.pid()) {
            log.warn("Invalid unit creator: {} on: {}", unit.creator(), config.logLabel());
            return;
        }
        var ep = retrieveEpoch(unit);
        if (ep != null) {
            ep.adder().produce(unit);
            log.debug("Produced: {} on: {}", unit, config.logLabel());
        } else {
            log.trace("Unable to retrieve epic for Unit creator: {} epoch: {} height: {} level: {} on: {}",
                      unit.creator(), unit.epoch(), unit.height(), unit.level(), config.logLabel());
        }
    }

    /**
     * newEpoch creates and returns a new epoch object with the given EpochID. If such epoch already exists, returns
     * it.
     */
    private epoch newEpoch(int epoch) {
        if (epoch >= config.numberOfEpochs()) {
            log.trace("Finished, beyond last epoch: {} on: {}", epoch, config.logLabel());
            return null;
        }
        final var currentId = currentEpoch.get();
        epoch e = epochs.get(epoch);
        if (e == null && epoch == currentId + 1) {
            e = createEpoch(epoch);
            epochs.put(epoch, e);
            log.trace("new epoch created: {} on: {}", epoch, config.logLabel());
        }

        if (epoch == currentId + 1) {
            assert e != null;
            var prev = epochs.remove(currentId - 1);
            if (prev != null) {
                prev.close();
            }
            currentEpoch.set(epoch);

            if (newEpochAction != null) {
                newEpochAction.accept(epoch);
            }
        }
        return e;
    }

    /**
     * newEpoch creates and returns a new epoch object with the given EpochID. If such epoch already exists, returns
     * it.
     */
    private epoch retreiveEpoch(int epoch) {
        final var currentId = currentEpoch.get();
        final epoch e = epochs.get(epoch);
        if (e != null && epoch == e.id()) {
            return e;
        }
        if (e == null && epoch == currentId + 1) {
            final var newEpoch = createEpoch(epoch);
            epochs.put(epoch, newEpoch);
            log.trace("new epoch created: {} on: {}", epoch, config.logLabel());
            return newEpoch;
        }
        return null;
    }

    /**
     * retrieveEpoch returns an epoch for the given preunit. If the preunit comes from a future epoch, it is checked for
     * new epoch proof.
     */
    private epoch retrieveEpoch(PreUnit pu) {
        var epochId = pu.epoch();
        var e = getEpoch(epochId);
        var epoch = e.epoch;
        if (e.newer) {
            if (EpochProofBuilder.epochProof(pu, config.WTKey())) {
                epoch = newEpoch(epochId);
            }
        }
        return epoch;
    }

    record epoch(int id, Dag dag, Adder adder, AtomicBoolean more) {

        public void close() {
            adder.close();
            more.set(false);
        }

        public void noMoreUnits() {
            more.set(false);
        }
    }

    private record epochWithNewer(epoch epoch, boolean newer) {

        public void noMoreUnits() {
            if (epoch != null) {
                epoch.noMoreUnits();
            }
        }
    }

    private record UnitTask(Unit unit, Consumer<Unit> consumer) implements Runnable, Comparable<UnitTask> {

        @Override
        public int compareTo(UnitTask o) {
            var comp = Integer.compare(unit.epoch(), o.unit.epoch());
            if (comp < 0 || comp > 0) {
                return comp;
            }
            comp = Integer.compare(unit.height(), o.unit.height());
            if (comp < 0 || comp > 0) {
                return comp;
            }
            return Integer.compare(unit.creator(), o.unit.creator());
        }

        @Override
        public void run() {
            consumer.accept(unit);
        }

    }
}
