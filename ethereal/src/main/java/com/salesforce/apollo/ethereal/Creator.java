/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.membership.Context;

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
public class Creator {

    @FunctionalInterface
    public interface RandomSourceData {
        byte[] apply(int level, List<Unit> parents, int epoch);
    }

    @FunctionalInterface
    public interface RsData {
        byte[] rsData(int level, Unit[] parents, int epoch);
    }

    private record built(Unit[] parents, int level) {}

    private static final Logger log = LoggerFactory.getLogger(Creator.class);

    public static int parentsOnPreviousLevel(PreUnit pu) {
        var heights = pu.view().heights();
        int count = 0;
        for (short creator = 0; creator < heights.length; creator++) {
            if (heights[creator] < pu.height()) {
                count++;
            }
        }
        return count;
    }

    /**
     * MakeConsistent ensures that the set of parents follows "parent consistency
     * rule". Modifies the provided parents in place. Parent consistency rule means
     * that unit's i-th parent cannot be lower (in a level sense) than i-th parent
     * of any other of that units parents. In other words, units seen from U
     * "directly" (as parents) cannot be below the ones seen "indirectly" (as
     * parents of parents).
     */
    private static void makeConsistent(Unit[] parents) {
        for (int i = 0; i < parents.length; i++) {
            for (int j = 0; j < parents.length; j++) {
                if (parents[j] == null) {
                    continue;
                }
                Unit u = parents[j].parents()[i];
                if (parents[i] == null || (u != null && u.level() > parents[i].level())) {
                    parents[i] = u;
                }
            }
        }
    }

    private final List<Unit>                           candidates;
    private final Config                               conf;
    private final DataSource                           ds;
    private final AtomicInteger                        epoch      = new AtomicInteger(0);
    private final AtomicBoolean                        epochDone  = new AtomicBoolean();
    private final AtomicReference<EpochProofBuilder>   epochProof = new AtomicReference<>();
    private final Function<Integer, EpochProofBuilder> epochProofBuilder;
    private final Queue<Unit>                          lastTiming;
    private final int                                  quorum;
    private final Consumer<Unit>                       send;

    public Creator(Config config, DataSource ds, Queue<Unit> lastTiming, Consumer<Unit> send,
                   Function<Integer, EpochProofBuilder> epochProofBuilder) {
        this.conf = config;
        this.ds = ds;
        this.epochProofBuilder = epochProofBuilder;
        this.send = send;
        this.candidates = new CopyOnWriteArrayList<>();
        for (int i = 0; i < config.nProc(); i++) {
            candidates.add(null);
        }
        this.lastTiming = lastTiming;

        quorum = Context.minimalQuorum(config.nProc(), config.bias()) + 1;
    }

    /**
     * Unit is examined and stored to be used as parents of future units. When there
     * are enough new parents, a new unit is produced. lastTiming is a channel on
     * which the last timing unit of each epoch is expected to appear.
     * 
     */
    public void consume(Unit u) {
        log.trace("Processing next unit: {} on: {}", u, conf.logLabel());
        update(u);
        var built = ready();
        while (built != null) {
            log.trace("Ready, creating unit on: {}", conf.logLabel());
            createUnit(built.parents, built.level, getData(built.level));
            built = ready();
        }
    }

    public void start() {
        newEpoch(epoch.get(), ByteString.EMPTY, -1);
    }

    public void stop() {
    }

    private built buildParents() {
        Unit[] parents = new Unit[conf.nProc()];
        parents = candidates.toArray(parents);
        final var thisUnit = parents[conf.pid()];
        if (thisUnit == null) {
            log.trace("No unit for this proc on: {}", conf.logLabel());
            return null;
        }
        var l = thisUnit.level() + 1;
        int count = count(l, parents);
        if (count >= quorum) {
            log.trace("Parents ready: {} level: {} on: {}", quorum, l, conf.logLabel());
            makeConsistent(parents);
            return new built(parents, l);
        } else {
            log.trace("Parents not ready level: {} current: {} required: {}  on: {}", l, count, quorum,
                      conf.logLabel());
            return null;
        }
    }

    private int count(int level, Unit[] parents) {
        var count = 0;
        for (int i = 0; i < conf.nProc(); i++) {
            Unit p = parents[i];

            // Ensure all parents are < level
            for (; p != null && p.level() >= level; p = p.predecessor())
                ;
            parents[i] = p;

            // Count parents directly above this level
            if (p != null && p.level() == level - 1) {
                count++;
            }
        }
        return count;
    }

    private void createUnit(Unit[] parents, int level, ByteString data) {
        assert parents.length == conf.nProc();
        final int e = epoch.get();
        Unit u = PreUnit.newFreeUnit(conf.pid(), e, parents, level, data, conf.digestAlgorithm(), conf.signer());
        assert parentsOnPreviousLevel(u) >= quorum : "Parents: " + Arrays.asList(u.parents()) + " of: " + u
        + " for level: " + (u.level() - 1) + " count: " + parentsOnPreviousLevel(u) + " quorum: " + quorum;
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
                log.trace("Finished, last epoch timing unit: {} level: {} on: {}", timingUnit, level, conf.logLabel());
                return epochProof.get().buildShare(timingUnit);
            }
            log.trace("Ignored timing unit from epoch: {} current: {} on: {}", timingUnit.epoch(), e, conf.logLabel());
            timingUnit = lastTiming.poll();
        }
        return ByteString.EMPTY;
    }

    /**
     * switches the creator to a chosen epoch, resets candidates and shares and
     * creates a dealing with the provided data.
     **/
    private void newEpoch(int epoch, ByteString data, int from) {
        this.epoch.set(epoch);

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
        final var unit = candidates.get(conf.pid());
        if (unit == null) {
            log.trace("Candidate not set on: {}", conf.logLabel());
            return null;
        }
        if (epochDone.get()) {
            log.trace("Epoch finished : {} on: {}", unit, conf.logLabel());
            return null;
        }
        final int l = unit.level();
        log.trace("Ready to create epoch: {} candidate level: {} on: {}", epoch.get(), l, conf.logLabel());
        return buildParents();
    }

    /**
     * resets the candidates and all related variables to the initial state (a slice
     * with NProc nils). This is useful when switching to a new epoch.
     * 
     * @param epoch
     */
    private void resetEpoch(int epoch) {
        log.debug("Resetting epoch: {} on: {}", epoch, conf.logLabel());
        for (int i = 0; i < conf.nProc(); i++) {
            candidates.set(i, null);
        }
        epochDone.set(false);
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
            log.trace("Unit: {} from a previous epoch, current epoch: {} on: {}", unit, epoch.get(), conf.logLabel());
            return;
        }

        // If the unit is the first epoch from a new epoch, switch to that epoch.
        if (unit.epoch() > e && unit.height() == 0) {
            if (!epochProof.get().verify(unit)) {
                log.warn("Unit did not verify epoch proof with: {}, rejected on: {}", unit, conf.logLabel());
                return;
            }
            log.trace("Changing epoch from: {} to: {} using: {} on: {}", epoch.get(), unit.epoch(), unit,
                      conf.logLabel());
            newEpoch(unit.epoch(), unit.data(), e);
        }

        // If this is a finishing unit try to extract threshold signature share from it.
        // If there are enough shares to produce the signature (and therefore a proof
        // that the current epoch is finished) switch to a new epoch.
        ByteString ep = epochProof.get().tryBuilding(unit);
        if (ep != null) {
            log.trace("Advancing epoch from: {} to: {} using: {} on: {}", e, e + 1, unit, conf.logLabel());
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
        var prev = candidates.get(u.creator());
        if (prev == null || prev.level() < u.level()) {
            candidates.set(u.creator(), u);
        }
    }

}
