/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.rbc;

import static com.salesforce.apollo.ethereal.Dag.newDag;

import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import org.slf4j.Logger;

import com.salesfoce.apollo.ethereal.proto.Gossip;
import com.salesfoce.apollo.ethereal.proto.Update;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.Dag;
import com.salesforce.apollo.ethereal.DataSource;
import com.salesforce.apollo.ethereal.PreUnit;
import com.salesforce.apollo.ethereal.RandomSource;
import com.salesforce.apollo.ethereal.RandomSource.RandomSourceFactory;
import com.salesforce.apollo.ethereal.Unit;
import com.salesforce.apollo.ethereal.creator.Creator;
import com.salesforce.apollo.ethereal.creator.Creator.RsData;
import com.salesforce.apollo.ethereal.creator.EpochProofBuilder;
import com.salesforce.apollo.ethereal.creator.EpochProofBuilder.epochProofImpl;
import com.salesforce.apollo.ethereal.creator.EpochProofBuilder.sharesDB;
import com.salesforce.apollo.ethereal.linear.ExtenderService;

/**
 * Orderer orders ordered orders into ordered order. The Jesus Nut of the
 * Ethereal pipeline
 *
 * @author hal.hildebrand
 *
 */
public class ChRbcOrderer {

    private record epoch(int id, Dag dag, ChRbcAdder adder, ExtenderService extender, RandomSource rs,
                         AtomicBoolean more) {

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
            epoch.noMoreUnits();
        }
    }

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(ChRbcOrderer.class);

    private final Config                 config;
    private final Creator                creator;
    private final AtomicReference<epoch> current  = new AtomicReference<>();
    private volatile Thread              currentThread;
    private final ExecutorService        executor;
    private final Queue<Unit>            lastTiming;
    private final ReadWriteLock          mx       = new ReentrantReadWriteLock();
    private final Consumer<Integer>      newEpochAction;
    private final AtomicReference<epoch> previous = new AtomicReference<>();
    private final RandomSourceFactory    rsf;
    private final AtomicBoolean          started  = new AtomicBoolean();
    private final int                    threshold;
    private final Consumer<List<Unit>>   toPreblock;

    public ChRbcOrderer(Config conf, int threshold, DataSource ds, Consumer<List<Unit>> toPreblock,
                        Consumer<Integer> newEpochAction, RandomSourceFactory rsf) {
        this.threshold = threshold;
        this.config = conf;
        this.lastTiming = new LinkedBlockingDeque<>();
        this.toPreblock = toPreblock;
        this.newEpochAction = newEpochAction;
        this.rsf = rsf;
        creator = new Creator(config, ds, u -> {
            assert u.creator() == config.pid();
            final Lock lock = mx.writeLock();
            lock.lock();
            try {
                log.trace("Sending: {} on: {}", u, config.logLabel());
                insert(u);
            } finally {
                lock.unlock();
            }
        }, rsData(), epoch -> new epochProofImpl(config, epoch, new sharesDB(config, new ConcurrentHashMap<>())));
        executor = Executors.newSingleThreadExecutor(r -> {
            final var t = new Thread(r, "Order Executor[" + conf.logLabel() + "]");
            t.setDaemon(true);
            return t;
        });
    }

    public Config getConfig() {
        return config;
    }

    public Processor processor() {
        return new Processor() {
            @Override
            public Gossip gossip(Digest context, int ring) {
                return currentProcessor().gossip(context, ring);
            }

            @Override
            public Update gossip(Gossip gossip) {
                return currentProcessor().gossip(gossip);
            }

            @Override
            public Update update(Update update) {
                return currentProcessor().update(update);
            }

            @Override
            public void updateFrom(Update update) {
                currentProcessor().updateFrom(update);
            }
        };
    }

    public void start() {
        newEpoch(0);
        creator.start();
        started.set(true);
    }

    public void stop() {
        log.trace("Stopping Orderer on: {}", config.logLabel());
        started.set(false);
        executor.shutdownNow();
        final var c = currentThread;
        if (c != null) {
            c.interrupt();
        }
        if (previous.get() != null) {
            previous.get().close();
        }
        if (current != null) {
            current.get().close();
        }
        log.trace("Orderer stopped on: {}", config.logLabel());
    }

    private epoch createEpoch(int epoch) {
        Dag dg = newDag(config, epoch);
        RandomSource rs = rsf.newRandomSource(dg);
        ExtenderService ext = new ExtenderService(dg, rs, config, handleTimingRounds());
        dg.afterInsert(u -> {
            if (!started.get()) {
                return;
            }
            try {
                executor.execute(() -> {
                    if (!started.get()) {
                        return;
                    }
                    currentThread = Thread.currentThread();
                    try {
                        ext.chooseNextTimingUnits();
                        // don't put our own units on the unit belt, creator already knows about them.
                        if (u.creator() != config.pid()) {
                            creator.consume(Collections.singletonList(u), lastTiming);
                        }
                    } finally {
                        currentThread = null;
                    }
                });
            } catch (RejectedExecutionException e) {
                // ignored
            }
        });
        return new epoch(epoch, dg, new ChRbcAdder(dg, 1024 * 1024, config, threshold), ext, rs,
                         new AtomicBoolean(true));
    }

    private Processor currentProcessor() {
        epoch c = current.get();
        if (c == null) {
            throw new IllegalStateException("No current epoch on: " + config.logLabel());
        }
        return c.adder.processor();
    }

    private void finishEpoch(int epoch) {
        var ep = getEpoch(epoch);
        if (ep != null) {
            ep.noMoreUnits();
        }
    }

    private epochWithNewer getEpoch(int epoch) {
        final epoch c = current.get();
        if (c == null || epoch > c.id) {
            return new epochWithNewer(null, true);
        }
        if (epoch == c.id()) {
            return new epochWithNewer(c, false);
        }
        final epoch p = previous.get();
        if (epoch == p.id()) {
            return new epochWithNewer(p, false);
        }
        return new epochWithNewer(null, false);
    }

    /**
     * Waits for ordered round of units produced by Extenders and produces Preblocks
     * based on them. Since Extenders in multiple epochs can supply ordered rounds
     * simultaneously, handleTimingRounds needs to ensure that Preblocks are
     * produced in ascending order with respect to epochs. For the last ordered
     * round of the epoch, the timing unit defining it is sent to the creator (to
     * produce signature shares.)
     */
    private Consumer<List<Unit>> handleTimingRounds() {
        AtomicInteger current = new AtomicInteger(0);
        return round -> {
            var timingUnit = round.get(round.size() - 1);
            var epoch = timingUnit.epoch();

            if (timingUnit.level() == config.lastLevel()) {
                lastTiming.add(timingUnit);
                finishEpoch(epoch);
            }
            if (epoch >= current.get() && timingUnit.level() <= config.lastLevel()) {
                toPreblock.accept(round);
                log.debug("Preblock produced level: {}, epoch: {} on: {}", timingUnit.level(), epoch,
                          config.logLabel());
            }
            current.set(epoch);
        };
    }

    /**
     * insert puts the provided unit directly into the corresponding epoch. If such
     * epoch does not exist, creates it. All correctness checks (epoch proof, adder,
     * dag checks) are skipped. This method is meant for our own units only.
     */
    private void insert(Unit unit) {
        if (unit.creator() != config.pid()) {
            log.warn("Invalid unit creator: {} on: {}", unit.creator(), config.logLabel());
            return;
        }
        var rslt = getEpoch(unit.epoch());
        epoch ep = rslt.epoch;
        if (rslt.newer) {
            ep = newEpoch(unit.epoch());
        }
        if (ep != null) {
            ep.dag.insert(unit);
            log.debug("Inserted: {} on: {}", unit, config.logLabel());
        } else {
            log.trace("Unable to retrieve epic for Unit creator: {} epoch: {} height: {} level: {} on: {}",
                      unit.creator(), unit.epoch(), unit.height(), unit.level(), config.logLabel());
        }
    }

    /**
     * newEpoch creates and returns a new epoch object with the given EpochID. If
     * such epoch already exists, returns it.
     */
    private epoch newEpoch(int epoch) {
        final Lock lock = mx.writeLock();
        lock.lock();
        try {
            final epoch c = current.get();
            if (c == null || epoch > c.id()) {
                final epoch p = previous.get();
                if (p != null) {
                    p.close();
                }
                previous.set(c);
                epoch newEpoch = createEpoch(epoch);
                current.set(newEpoch);
                if (newEpochAction != null) {
                    newEpochAction.accept(epoch);
                }
                return newEpoch;
            }
            if (epoch == c.id()) {
                return c;
            }
            epoch p = previous.get();
            if (epoch == p.id()) {
                return p;
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * retrieveEpoch returns an epoch for the given preunit. If the preunit comes
     * from a future epoch, it is checked for new epoch proof.
     */
    @SuppressWarnings("unused")
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

    /**
     * rsData produces random source data for a unit with provided level, parents
     * and epoch.
     */
    private RsData rsData() {
        return (level, parents, epoch) -> {
            final RandomSourceFactory r = rsf;
            byte[] result = null;
            if (level == 0) {
                result = r.dealingData(epoch);
            } else {
                epochWithNewer ep = getEpoch(epoch);
                if (ep != null && ep.epoch != null) {
                    result = ep.epoch.rs().dataToInclude(parents, level);
                }
            }
            if (result != null) {
                return new byte[0];
            }
            return result;
        };
    }
}
