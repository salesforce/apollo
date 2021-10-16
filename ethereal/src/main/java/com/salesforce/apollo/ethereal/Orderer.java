/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import static com.salesforce.apollo.ethereal.Dag.newDag;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
import com.salesforce.apollo.ethereal.creator.EpochProofBuilder;
import com.salesforce.apollo.ethereal.creator.EpochProofBuilder.epochProofImpl;
import com.salesforce.apollo.ethereal.creator.EpochProofBuilder.sharesDB;
import com.salesforce.apollo.ethereal.linear.ExtenderService;
import com.salesforce.apollo.utils.Utils;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;

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
            more.set(false);
        }

        public Collection<? extends Unit> allUnits() {
            return dag.unitsAbove(null);
        }

        public void have(DigestBloomFilter biff) {
            dag.have(biff);
        }

        public void noMoreUnits() {
            more.set(false);
        }

        public Collection<? extends Unit> unitsAbove(int[] heights) {
            return dag.unitsAbove(heights);
        }

        public boolean wantsMoreUnits() {
            return more.get();
        }

    }

    record epochWithNewer(epoch epoch, boolean newer) {

        public void noMoreUnits() {
            epoch.noMoreUnits();
        }
    }

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(Orderer.class);

    private final Config                 config;
    private final Creator                creator;
    private final AtomicReference<epoch> current  = new AtomicReference<>();
    private final Queue<Unit>            lastTiming;
    private final ReadWriteLock          mx       = new ReentrantReadWriteLock();
    private final AtomicReference<epoch> previous = new AtomicReference<>();
    private final RandomSourceFactory    rsf;
    private final Consumer<List<Unit>>   toPreblock;

    public Orderer(Config conf, DataSource ds, Consumer<List<Unit>> toPreblock, Consumer<Unit> rbc,
                   RandomSourceFactory rsf) {
        this.config = conf;
        this.lastTiming = new LinkedBlockingDeque<>();
        this.toPreblock = toPreblock;
        this.rsf = rsf;
        creator = new Creator(config, ds, u -> {
            assert u.creator() == config.pid();
            final Lock lock = mx.writeLock();
            lock.lock();
            try {
                log.trace("Sending: {} on: {}", u, config.pid());
                insert(u);
                rbc.accept(u);
            } finally {
                lock.unlock();
            }
        }, rsData(), epoch -> new epochProofImpl(config, epoch, new sharesDB(config, new ConcurrentHashMap<>())));
    }

    /**
     * Sends preunits received from other committee members to their corresponding
     * epochs. It assumes preunits are ordered by ascending epochID and, within each
     * epoch, they are topologically sorted.
     */
    public Map<Digest, Correctness> addPreunits(short source, List<PreUnit> preunits) {
        final Lock lock = mx.writeLock();
        lock.lock();
        try {
            log.debug("Adding: {} from: {} on: {}", preunits, source, config.pid());
            var errors = new HashMap<Digest, Correctness>();
            while (preunits.size() > 0) {
                var epoch = preunits.get(0).epoch();
                var end = 0;
                while (end < preunits.size() && preunits.get(end).epoch() == epoch) {
                    end++;
                }
                epoch ep = retrieveEpoch(preunits.get(0), source);
                if (ep != null) {
                    errors.putAll(ep.adder().addPreunits(source, preunits.subList(0, end)));
                } else {
                    log.debug("No epoch for: {} from: {} on: {}", preunits, source, config.pid());
                }
                preunits = preunits.subList(end, preunits.size());
            }
            return errors;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns all units present in the orderer that are newer than units described
     * by the given DagInfo. This includes all units from the epoch given by the
     * DagInfo above provided heights as well as ALL units from newer epochs.
     */
    public List<Unit> delta(DagInfo[] info) {
        final Lock lock = mx.readLock();
        lock.lock();
        try {
            List<Unit> result = new ArrayList<>();
            final epoch c = current.get();
            Consumer<DagInfo> deltaResolver = dagInfo -> {
                if (dagInfo == null) {
                    return;
                }
                final epoch p = previous.get();
                if (p != null && dagInfo.epoch() == p.id()) {
                    result.addAll(p.unitsAbove(dagInfo.heights()));
                }
                if (c != null && dagInfo.epoch() == c.id()) {
                    result.addAll(c.unitsAbove(dagInfo.heights()));
                }
            };
            deltaResolver.accept(info[0]);
            deltaResolver.accept(info[1]);
            if (c != null) {
                if (info[0] != null && info[0].epoch() < c.id && info[1] != null && info[1].epoch() < c.id()) {
                    result.addAll(c.allUnits());
                }
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

    public Config getConfig() {
        return config;
    }

    /** GetInfo returns DagInfo of the dag from the most recent epoch. */
    public DagInfo[] getInfo() {
        final Lock lock = mx.readLock();
        lock.lock();
        try {
            var result = new DagInfo[2];
            final epoch p = previous.get();
            if (p != null && !p.wantsMoreUnits()) {
                result[0] = p.dag.maxView();
            }
            final epoch c = current.get();
            if (c != null && !c.wantsMoreUnits()) {
                result[1] = c.dag().maxView();
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Answer the BloomFilter containing the receiver's DAG state in the current and
     * previous epoch
     */
    public BloomFilter<Digest> have() {
        var biff = new BloomFilter.DigestBloomFilter(Utils.bitStreamEntropy().nextLong(),
                                                     config.epochLength() * 2 * config.nProc(), 0.125);
        final var lock = mx.readLock();
        lock.lock();
        try {
            var p = previous.get();
            if (p != null) {
                p.have(biff);
            }
            var c = current.get();
            if (c != null) {
                c.have(biff);
            }
        } finally {
            lock.unlock();
        }
        return biff;
    }

    /** MaxUnits returns maximal units per process from the chosen epoch. */
    public SlottedUnits maxUnits(int epoch) {
        var ep = getEpoch(epoch);
        if (ep != null) {
            return ep.epoch.dag.maximalUnitsPerProcess();
        }
        return null;
    }

    public void start() {
        newEpoch(0);
        creator.start();
    }

    public void stop() {
        if (previous.get() != null) {
            previous.get().close();
        }
        if (current != null) {
            current.get().close();
        }
        log.trace("Orderer stopped on: {}", config.pid());
    }

    /**
     * UnitsByHash allows to access units present in the orderer using their hashes.
     * The length of the returned slice is equal to the number of argument hashes.
     * For non-present units the returned slice contains nil on the corresponding
     * position.
     */
    public List<Unit> unitsByHash(List<Digest> ids) {
        List<Unit> result;
        final Lock lock = mx.readLock();
        lock.lock();
        try {
            final epoch c = current.get();
            result = c != null ? c.dag.get(ids) : new ArrayList<>();
            final epoch p = previous.get();
            if (p != null) {
                for (int i = 0; i < result.size(); i++) {
                    if (result.get(i) != null) {
                        result.set(i, p.dag.get(ids.get(i)));
                    }
                }
            }
        } finally {
            lock.unlock();
        }
        return result;
    }

    /**
     * UnitsByID allows to access units present in the orderer using their ids. The
     * returned slice contains only existing units (no nil entries for non-present
     * units) and can contain multiple units with the same id (forks). Because of
     * that the length of the result can be different than the number of arguments.
     */
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

    private epoch createEpoch(int epoch) {
        Dag dg = newDag(config, epoch);
        RandomSource rs = rsf.newRandomSource(dg);
        ExtenderService ext = new ExtenderService(dg, rs, config, handleTimingRounds());
        dg.afterInsert(u -> ext.chooseNextTimingUnits());
        dg.afterInsert(u -> {
            // don't put our own units on the unit belt, creator already knows about them.
            if (u.creator() != config.pid()) {
                creator.consume(Collections.singletonList(u), lastTiming);
            }
        });
        return new epoch(epoch, dg, new AdderImpl(dg, config), ext, rs, new AtomicBoolean(true));
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
                log.debug("Preblock produced level: {}, epoch: {} on: {}", timingUnit.level(), epoch, config.pid());
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
            log.warn("Invalid unit creator: {} on: {}", unit.creator(), config.pid());
            return;
        }
        var rslt = getEpoch(unit.epoch());
        epoch ep = rslt.epoch;
        if (rslt.newer) {
            ep = newEpoch(unit.epoch());
        }
        if (ep != null) {
            ep.dag.insert(unit);
            log.trace("Inserted: {} on: {}", unit, config.pid());
        } else {
            log.debug("Unable to retrieve epic for Unit creator: {} epoch: {} height: {} level: {} on: {}",
                      unit.creator(), unit.epoch(), unit.height(), unit.level(), config.pid());
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
     * from a future epoch, it is checked for new epoch proof. If failed, requests
     * gossip with source of the preunit.
     */
    private epoch retrieveEpoch(PreUnit pu, short source) {
        var epochId = pu.epoch();
        var e = getEpoch(epochId);
        var epoch = e.epoch;
        if (e.newer) {
            if (EpochProofBuilder.epochProof(pu, config.WTKey())) {
                epoch = newEpoch(epochId);
            } else {
//                ord.syncer.RequestGossip(source)
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
                if (ep != null) {
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
