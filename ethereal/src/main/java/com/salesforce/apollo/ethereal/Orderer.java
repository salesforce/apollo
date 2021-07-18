/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import static com.salesforce.apollo.ethereal.Dag.newDag;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import org.slf4j.Logger;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.ethereal.Adder.AdderImpl;
import com.salesforce.apollo.ethereal.Adder.Correctness;
import com.salesforce.apollo.ethereal.Dag.DagInfo;
import com.salesforce.apollo.ethereal.RandomSource.RandomSourceFactory;
import com.salesforce.apollo.ethereal.creator.Creator;
import com.salesforce.apollo.ethereal.creator.Creator.RsData;
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
public class Orderer {
    public record epoch(int id, Dag dag, Adder adder, ExtenderService extender, RandomSource rs, AtomicBoolean more) {

        public void close() {
            adder.close();
            extender.close();
            more.set(false);
        }

        public boolean wantsMoreUnits() {
            return more.get();
        }

        public Collection<? extends Unit> allUnits() {
            return dag.unitsAbove(null);
        }

        public Collection<? extends Unit> unitsAbove(List<Integer> heights) {
            return dag.unitsAbove(heights);
        }

        public void noMoreUnits() {
            more.set(false);
        }
    }

    record epochWithNewer(epoch epoch, boolean newer) {

        public void noMoreUnits() {
            epoch.noMoreUnits();
        }
    }

    // A consumer of ordered round of units produced by Extenders and
    // produces Preblocks based on them. Since Extenders in multiple epochs can
    // supply ordered rounds simultaneously, handleTimingRounds needs to ensure that
    // Preblocks are produced in ascending order with respect to epochs. For the
    // last ordered round of the epoch, the timing unit defining it is sent to the
    // creator (to produce signature shares.)
    private class OrderedRoundConsumer implements Subscriber<List<Unit>> {
        private volatile int current;

        @Override
        public void onComplete() {
        }

        @Override
        public void onError(Throwable e) {
            log.error("Error from producer", e);
        }

        @Override
        public void onNext(List<Unit> round) {
            var timingUnit = round.get(round.size() - 1);
            var epoch = timingUnit.epoch();

            if (timingUnit.level() == config.lastLevel()) {
                lastTiming.submit(timingUnit);
                finishEpoch(epoch);
            }
            if (epoch > current && timingUnit.level() <= config.lastLevel()) {
                toPreblock.accept(round);
                log.info("Preblock produced level: {}, epoch: {}", timingUnit.level(), epoch);
            }
            current = epoch;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            current = 0;
        }

    }

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(Orderer.class);

    public static epoch newEpoch(int id, Config config, RandomSourceFactory rsf, SubmissionPublisher<Unit> unitBelt,
                                 SubmissionPublisher<List<Unit>> output, Clock clock) {
        Dag dg = newDag(config, id);
        RandomSource rs = rsf.newRandomSource(dg);
        ExtenderService ext = new ExtenderService(dg, rs, config, output);
        dg.afterInsert(u -> ext.chooseNextTimingUnits());
        dg.afterInsert(u -> {
            ext.chooseNextTimingUnits();
            // don't put our own units on the unit belt, creator already knows about them.
            if (u.creator() != config.pid()) {
                unitBelt.submit(u);
            }
        });
        return new epoch(id, dg, new AdderImpl(dg, clock, config.digestAlgorithm()), ext, rs, new AtomicBoolean(true));
    }

    private final Clock                           clock;
    private final Config                          config;
    private Creator                               creator;
    private epoch                                 current;
    private final DataSource                      ds;
    private final SubmissionPublisher<Unit>       lastTiming;
    private final ReadWriteLock                   mx = new ReentrantReadWriteLock();
    private final SubmissionPublisher<List<Unit>> orderedUnits;
    private epoch                                 previous;
    private RandomSourceFactory                   rsf;
    private final Consumer<List<Unit>>            toPreblock;
    private final SubmissionPublisher<Unit>       unitBelt;

    public Orderer(Config conf, DataSource ds, Consumer<List<Unit>> toPreblock, Clock clock) {
        this.config = conf;
        this.ds = ds;
        this.lastTiming = new SubmissionPublisher<>(config.executor(), config.numberOfEpochs());
        this.orderedUnits = new SubmissionPublisher<>(config.executor(), conf.epochLength());
        this.toPreblock = toPreblock;
        this.unitBelt = new SubmissionPublisher<>(config.executor(), conf.epochLength() * conf.nProc());
        this.clock = clock;
    }

    // Sends preunits received from other committee members to their corresponding
    // epochs. It assumes preunits are ordered by ascending epochID and, within each
    // epoch, they are topologically sorted.
    public Map<Digest, Correctness> addPreunits(short source, List<PreUnit> preunits) {
        var errors = new HashMap<Digest, Correctness>();
        while (preunits.size() > 0) {
            var epoch = preunits.get(0).epoch();
            var end = 0;
            while (end < preunits.size() && preunits.get(end).epoch() == epoch) {
                end++;
            }
            epoch ep = retrieveEpoch(preunits.get(0), source);
            if (ep != null) {
                errors.putAll(ep.adder().addPreunits(source, preunits.subList(end, preunits.size())));
            }
            preunits = preunits.subList(end, preunits.size());
        }
        return errors;
    }

    // Delta returns all units present in the orderer that are newer than units
    // described by the given DagInfo. This includes all units from the epoch given
    // by the DagInfo above provided heights as well as ALL units from newer epochs.
    public List<Unit> delta(DagInfo[] info) {
        final Lock lock = mx.readLock();
        lock.lock();
        try {
            List<Unit> result = new ArrayList<Unit>();
            Consumer<DagInfo> deltaResolver = dagInfo -> {
                if (dagInfo == null) {
                    return;
                }
                if (previous != null && dagInfo.epoch() == previous.id()) {
                    result.addAll(previous.unitsAbove(dagInfo.heights()));
                }
                if (current != null && dagInfo.epoch() == current.id()) {
                    result.addAll(current.unitsAbove(dagInfo.heights()));
                }
            };
            deltaResolver.accept(info[0]);
            deltaResolver.accept(info[1]);
            if (current != null) {
                if (info[0] != null && info[0].epoch() < current.id && info[1] != null
                && info[1].epoch() < current.id()) {
                    result.addAll(current.allUnits());
                }
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

    // GetInfo returns DagInfo of the dag from the most recent epoch.
    public DagInfo[] getInfo() {
        final Lock lock = mx.readLock();
        lock.lock();
        try {
            var result = new DagInfo[2];
            if (previous != null && !previous.wantsMoreUnits()) {
                result[0] = previous.dag.maxView();
            }
            if (current != null && !current.wantsMoreUnits()) {
                result[1] = current.dag().maxView();
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

    // MaxUnits returns maximal units per process from the chosen epoch.
    public SlottedUnits maxUnits(int epoch) {
        var ep = getEpoch(epoch);
        if (ep != null) {
            return ep.epoch.dag.maximalUnitsPerProcess();
        }
        return null;
    }

    public void start(RandomSourceFactory rsf, SubmissionPublisher<PreUnit> synchronizer) {
        this.rsf = rsf;
        creator = new Creator(config, ds, u -> {
            insert(u);
            synchronizer.submit(u);
        }, rsData(), epoch -> new epochProofImpl(config, epoch, new sharesDB(config, new HashMap<>())));

        newEpoch(0);

        // Start creator
        creator.creatUnits(unitBelt, lastTiming);

        // Start preblock builder
        orderedUnits.subscribe(new OrderedRoundConsumer());
    }

    public void stop() {
        if (previous != null) {
            previous.close();
        }
        if (current != null) {
            current.close();
        }
        orderedUnits.close();
        unitBelt.close();
        log.info("Orderer stopped");
    }

    // UnitsByHash allows to access units present in the orderer using their hashes.
    // The length of the returned slice is equal to the number of argument hashes.
    // For non-present units the returned slice contains nil on the corresponding
    // position.
    public List<Unit> unitsByHash(List<Digest> ids) {
        List<Unit> result;
        final Lock lock = mx.readLock();
        lock.lock();
        try {
            result = current != null ? current.dag.get(ids) : new ArrayList<>();
            if (previous != null) {
                for (int i = 0; i < result.size(); i++) {
                    if (result.get(i) != null) {
                        result.set(i, previous.dag.get(ids.get(i)));
                    }
                }
            }
        } finally {
            lock.unlock();
        }
        return result;
    }

    // UnitsByID allows to access units present in the orderer using their ids. The
    // returned slice contains only existing units (no nil entries for non-present
    // units) and can contain multiple units with the same id (forks). Because of
    // that the length of the result can be different than the number of arguments.
    public List<Unit> unitsByID(List<Long> ids) {
        var result = new ArrayList<Unit>();
        final Lock lock = mx.readLock();
        lock.lock();
        try {
            for (var id : ids) {
                var epoch = PreUnit.decode(id).epoch();
                epochWithNewer ep = getEpoch(epoch);
                if (ep != null) {
                    result.addAll(ep.epoch.dag().get(id));
                }
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

    private void finishEpoch(int epoch) {
        var ep = getEpoch(epoch);
        if (ep != null) {
            ep.noMoreUnits();
        }
    }

    private epochWithNewer getEpoch(int epoch) {
        if (current == null || epoch > current.id) {
            return new epochWithNewer(null, true);
        }
        if (epoch == current.id()) {
            return new epochWithNewer(current, false);
        }
        if (epoch == previous.id()) {
            return new epochWithNewer(previous, false);
        }
        return new epochWithNewer(null, false);
    }

    // insert puts the provided unit directly into the corresponding epoch. If such
    // epoch does not exist, creates it. All correctness checks (epoch proof, adder,
    // dag checks) are skipped. This method is meant for our own units only.
    private void insert(Unit unit) {
        if (unit.creator() != config.pid()) {
            log.warn("Invalid unit creator: {}", unit.creator());
            return;
        }
        var rslt = getEpoch(unit.epoch());
        epoch ep = null;
        if (rslt.newer) {
            ep = newEpoch(unit.epoch());
        }
        if (ep != null) {
            ep.dag.insert(unit);
            log.info("Inserted Unit creator: {} epoch: {} height: {} level: {}", unit.creator(), unit.epoch(),
                     unit.height(), unit.level());
        } else {
            log.info("Unable to retrieve epic for Unit creator: {} epoch: {} height: {} level: {}", unit.creator(),
                     unit.epoch(), unit.height(), unit.level());
        }
    }

    // newEpoch creates and returns a new epoch object with the given EpochID. If
    // such epoch already exists, returns it.
    private epoch newEpoch(int epoch) {
        final Lock lock = mx.writeLock();
        lock.lock();
        try {
            if (current == null || epoch > current.id()) {
                if (previous != null) {
                    previous.close();
                }
                previous = current;
                current = newEpoch(epoch, config, rsf, unitBelt, orderedUnits, clock);
                return current;
            }
            if (epoch == current.id()) {
                return current;
            }
            if (epoch == previous.id()) {
                return previous;
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    // retrieveEpoch returns an epoch for the given preunit. If the preunit comes
    // from a future epoch, it is checked for new epoch proof. If failed, requests
    // gossip with source of the preunit.
    private epoch retrieveEpoch(PreUnit pu, short source) {
        var epochId = pu.epoch();
        var e = getEpoch(epochId);
        var epoch = e.epoch;
        if (e.newer) {
            if (creator.epochProof(pu, config.WTKey())) {
                epoch = newEpoch(epochId);
            } else {
//                ord.syncer.RequestGossip(source)
            }
        }
        return epoch;
    }

    // rsData produces random source data for a unit with provided level, parents
    // and epoch.
    private RsData rsData() {
        return (level, parents, epoch) -> {
            byte[] result = null;
            if (level == 0) {
                result = rsf.dealingData(epoch);
            } else {
                epochWithNewer ep = getEpoch(epoch);
                if (ep != null) {
                    result = ep.epoch.rs().dataToInclude(parents, level);
                }
            }
            if (result != null) {
                log.error("where orderer.rsData");
                return new byte[0];
            }
            return result;
        };
    }
}
