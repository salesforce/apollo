/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.rbc;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.Dag;
import com.salesforce.apollo.ethereal.DataSource;
import com.salesforce.apollo.ethereal.PreUnit;
import com.salesforce.apollo.ethereal.Unit;
import com.salesforce.apollo.ethereal.creator.EpochProofBuilder;

/**
 * Creator is a component responsible for producing new units. It processes
 * units produced by other committee members and stores the ones with the
 * highest level as possible parents (candidates). Whenever there are enough
 * parents to produce a unit on a new level, the creator creates a new Unit from
 * the available DataSource, signs and sends (using a function given to the
 * constructor) this new unit.
 * 
 * @author hal.hildebrand
 *
 */
public class ChRbcCreator {

    @FunctionalInterface
    public interface RandomSourceData {
        byte[] apply(int level, List<Unit> parents, int epoch);
    }

    @FunctionalInterface
    public interface RsData {
        byte[] rsData(int level, Unit[] parents, int epoch);
    }

    private record built(Unit[] parents, int level) {}

    private static final Logger log = LoggerFactory.getLogger(ChRbcCreator.class);

    private final Unit[]                               candidates;
    private final Config                               conf;
    private final DataSource                           ds;
    private final AtomicInteger                        epoch      = new AtomicInteger(0);
    private final AtomicBoolean                        epochDone  = new AtomicBoolean();
    private final AtomicReference<EpochProofBuilder>   epochProof = new AtomicReference<>();
    private final Function<Integer, EpochProofBuilder> epochProofBuilder;
    private final Queue<Unit>                          lastTiming;
    private final AtomicInteger                        level      = new AtomicInteger();
    private final AtomicInteger                        maxLvl     = new AtomicInteger();
    private final Lock                                 mx         = new ReentrantLock();
    private final AtomicInteger                        onMaxLvl   = new AtomicInteger();
    private final int                                  quorum;
    private final RsData                               rsData;
    private final Consumer<Unit>                       send;
    private final BlockingQueue<Unit>                  unitBelt;

    public ChRbcCreator(Config config, DataSource ds, Queue<Unit> lastTiming, Consumer<Unit> send, RsData rsData,
                        Function<Integer, EpochProofBuilder> epochProofBuilder) {
        this.conf = config;
        this.ds = ds;
        this.rsData = rsData;
        this.epochProofBuilder = epochProofBuilder;
        this.send = send;
        this.candidates = new Unit[config.nProc()];
        this.lastTiming = lastTiming;

        quorum = Dag.minimalQuorum(config.nProc(), config.bias()) + 1;

        // Topologically sorted queue of units waiting to be processed
        this.unitBelt = new PriorityBlockingQueue<>(config.nProc(), new Comparator<Unit>() {
            @Override
            public int compare(Unit a, Unit b) {
                // start with height
                var hCmp = Integer.compare(a.height(), b.height());
                if (hCmp != 0) {
                    return hCmp;
                }
                // next PID
                var pCmp = Short.compare(a.creator(), b.creator());
                if (pCmp != 0) {
                    return pCmp;
                }
                return 0;
            }
        });
    }

    /**
     * Accept a new Unit from ye group
     * 
     * @param u - the new Unit
     */
    public void accept(Unit u) {
        final var success = unitBelt.add(u); // We're using an unbounded queue assert
        assert success : "We have violated space/time directives we should not";
//        consume(Collections.singletonList(u));
    }

    public void drain() {
        var units = new ArrayList<Unit>();
        unitBelt.drainTo(units);
        if (!units.isEmpty()) {
            consume(units);
        }
    }

    public void start() {
        newEpoch(epoch.get(), ByteString.EMPTY, -1);
    }

    public void stop() {
    }

    private built buildParents() {
        var l = candidates[conf.pid()].level() + 1;
        final Unit[] parents = getParents();
        final var count = count(parents);
        if (count >= quorum) {
            log.trace("Parents ready: {} on: {}", parents, conf.logLabel());
            return new built(parents, l);
        } else {
            log.trace("Parents not ready: {} current: {} required: {}  on: {}", parents, count, quorum,
                      conf.logLabel());
            return null;
        }
    }

    /**
     * Units are examined and stored to be used as parents of future units. When
     * there are enough new parents, a new unit is produced. lastTiming is a channel
     * on which the last timing unit of each epoch is expected to appear.
     */
    private void consume(List<Unit> units) {
        log.trace("Processing next units: {} on: {}", units.size(), conf.logLabel());
        mx.lock();
        try {
            for (Unit u : units) {
                update(u);
            }
            var built = ready();
            if (built == null) {
                log.trace("Not ready to create unit on: {}", conf.logLabel());
            }
            while (built != null) {
                log.trace("Ready, creating unit on: {}", conf.logLabel());
                createUnit(built.parents, built.level, getData(built.level));
                built = ready();
            }
        } catch (Throwable e) {
            log.error("Error in processing units on: {}", conf.logLabel(), e);
        } finally {
            mx.unlock();
        }
    }

    private int count(Unit[] parents) {
        int count = 0;
        for (int i = 0; i < parents.length; i++) {
            if (parents[i] != null) {
                count++;
            }
        }
        return count;
    }

    private void createUnit(Unit[] parents, int level, ByteString data) {
        assert parents.length == conf.nProc();
        final int e = epoch.get();
        Unit u = PreUnit.newFreeUnit(conf.pid(), e, parents, level, data, rsData.rsData(level, parents, e),
                                     conf.digestAlgorithm(), conf.signer());
        if (log.isTraceEnabled()) {
            log.trace("Created unit: {} parents: {} on: {}", u, parents, conf.logLabel());
        } else {
            log.debug("Created unit: {} on: {}", u, conf.logLabel());
        }
        update(u);
        send.accept(u);
    }

    /**
     * produces a piece of data to be included in a unit on a given level. For
     * regular units the provided DataSource is used. For finishing units it's
     * either null or, if available, an encoded threshold signature share of hash
     * and id of the last timing unit (obtained from preblockMaker on lastTiming
     * channel)
     **/
    private ByteString getData(int level) {
        if (level < conf.lastLevel()) {
            if (ds != null) {
                return ds.getData();
            }
            return ByteString.EMPTY;
        }
        Unit timingUnit = lastTiming.poll();
        if (timingUnit == null) {
            log.trace("No timing unit: {} on: {}", level, conf.logLabel());
            return ByteString.EMPTY;
        }
        // in a rare case there can be timing units from previous epochs left on
        // lastTiming channel. the purpose of this loop is to drain and ignore them.
        while (timingUnit != null) {
            final int e = epoch.get();
            if (timingUnit.epoch() == e) {
                epochDone.set(true);
                if (e == conf.numberOfEpochs() - 1) {
                    log.trace("Finished, timing unit: {} level: {} on: {}", timingUnit, level, conf.logLabel());
                    // the epoch we just finished is the last epoch we were supposed to produce
                    return ByteString.EMPTY;
                }
                log.trace("Timing unit: {}, level: {} on: {}", timingUnit, level, conf.logLabel());
                return epochProof.get().buildShare(timingUnit);
            }
            log.info("Ignored timing unit from epoch: {} current: {} on: {}", timingUnit.epoch(), e, conf.logLabel());
            timingUnit = lastTiming.poll();
        }
        return ByteString.EMPTY;
    }

    private Unit[] getParents() {
        Unit[] result = new Unit[candidates.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = candidates[i];
        }
        return result;
    }

    /**
     * switches the creator to a chosen epoch, resets candidates and shares and
     * creates a dealing with the provided data.
     **/
    private void newEpoch(int epoch, ByteString data, int from) {
        log.trace("Changing epoch from: {} to: {} on: {}", from, epoch, conf.logLabel());
        this.epoch.set(epoch);
        epochDone.set(false);

        resetEpoch(epoch);
        epochProof.set(epochProofBuilder.apply(epoch));
        createUnit(new Unit[conf.nProc()], 0, data);
    }

    /**
     * ready checks if the creator is ready to produce a new unit. Usually that
     * means: "do we have enough new candidates to produce a unit with level higher
     * than the previous one?" Besides that, we stop producing units for the current
     * epoch after creating a unit with signature share.
     */
    private built ready() {
        final int l = candidates[conf.pid()].level();
        final var current = level.get();
        boolean ready = !epochDone.get() && current > l;
        if (ready) {
            log.trace("Ready to create epochDone: {} level: {} candidate level: {} on: {}", epochDone, current, l,
                      conf.logLabel());
            return buildParents();
        }
        log.trace("Not ready to create epochDone: {} epoch: {} level: {} candidate level: {} on: {}", epochDone,
                  epoch.get(), current, l, conf.logLabel());
        return null;
    }

    /**
     * resets the candidates and all related variables to the initial state (a slice
     * with NProc nils). This is useful when switching to a new epoch.
     * 
     * @param epoch
     */
    private void resetEpoch(int epoch) {
        log.debug("Resetting epoch: {} on: {}", epoch, conf.logLabel());
        for (int i = 0; i < candidates.length; i++) {
            candidates[i] = null;
        }
        maxLvl.set(-1);
        onMaxLvl.set(0);
        level.set(0);
    }

    /**
     * takes a unit and updates the receiver's state with information contained in
     * the unit.
     */
    private void update(Unit unit) {
        log.trace("updating: {} on: {}", unit, conf.logLabel());
        // if the unit is from an older epoch we simply ignore it
        final int e = epoch.get();
        if (unit.epoch() < e) {
            log.debug("Unit: {} from a previous epoch, current epoch: {} on: {}", unit, epoch, conf.logLabel());
            return;
        }

        // If the unit is the first epoch from a new epoch, switch to that epoch.
        if (unit.epoch() > e && unit.height() == 0) {
            if (!epochProof.get().verify(unit)) {
                log.warn("Unit did not verify epoch, rejected on: {}", conf.logLabel());
                return;
            }
            newEpoch(unit.epoch(), unit.data(), e);
        }

        // If this is a finishing unit try to extract threshold signature share from it.
        // If there are enough shares to produce the signature (and therefore a proof
        // that the current epoch is finished) switch to a new epoch.
        ByteString ep = epochProof.get().tryBuilding(unit);
        if (ep != null) {
            log.debug("Advancing epoch from: {} to: {} using: {} on: {}", e, e + 1, unit, conf.logLabel());
            newEpoch(e + 1, ep, e);
            return;
        }
        log.trace("No epoch proof generated from: {} on: {}", unit, conf.logLabel());

        updateCandidates(unit);
    }

    /**
     * updateCandidates puts the provided unit in parent candidates provided that
     * the level is higher than the level of the previous candidate for that
     * creator.
     */
    private void updateCandidates(Unit u) {
        if (u.epoch() != epoch.get()) {
            return;
        }
        var prev = candidates[u.creator()];
        if (prev == null || prev.level() < u.level()) {
            candidates[u.creator()] = u;
            log.trace("Update candidate to: {} on: {}", u, conf.logLabel());
            if (u.level() == maxLvl.get()) {
                onMaxLvl.incrementAndGet();
                log.trace("Update candidate onMaxLvl incremented to: {} by: {} on: {}", onMaxLvl.get(), u,
                          conf.logLabel());
            } else if (u.level() > maxLvl.get()) {
                maxLvl.set(u.level());
                onMaxLvl.set(1);
                log.trace("Update candidate {} new maxLvl: {} onMaxLvl: {} on: {}", u, maxLvl.get(), onMaxLvl.get(),
                          conf.logLabel());
            }
            level.set(maxLvl.get());
            log.trace("Update candidate new level: {} via: {} on: {}", level, u, conf.logLabel());
            if (onMaxLvl.get() >= quorum) {
                level.incrementAndGet();
                log.trace("Update candidate onMaxLvl: {} >= quorum: {} level now: {} via: {} on: {}", onMaxLvl.get(),
                          quorum, level.get(), u, conf.logLabel());
            }
        }
    }

}
