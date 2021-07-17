/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.creator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.SubmissionPublisher;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.protobuf.Any;
import com.salesfoce.apollo.ethereal.proto.EpochProof;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.DataSource;
import com.salesforce.apollo.ethereal.PreUnit;
import com.salesforce.apollo.ethereal.Unit;
import com.salesforce.apollo.ethereal.WeakThresholdKey;

/**
 * Creator is a component responsible for producing new units. It processes
 * units produced by other committee members and stores the ones with the
 * highest level as possible parents (candidates). Whenever there are enough
 * parents to produce a unit on a new level, signs and sends (using a function
 * given to the constructor) a new unit.
 * 
 * @author hal.hildebrand
 *
 */
public class Creator {
    @FunctionalInterface
    public interface RandomSourceData {
        byte[] apply(int level, List<Unit> parents, int epoch);
    }

    private record built(List<Unit> parents, int level) {}

    // MakeConsistent ensures that the set of parents follows "parent consistency
    // rule". Modifies the provided unit slice in place. Parent consistency rule
    // means that unit's i-th parent cannot be lower (in a level sense) than i-th
    // parent of any other of that units parents. In other words, units seen from U
    // "directly" (as parents) cannot be below the ones seen "indirectly" (as
    // parents of parents).
    private static void makeConsistent(List<Unit> parents) {
        for (int i = 0; i < parents.size(); i++) {
            for (int j = 0; j < parents.size(); j++) {
                if (parents.get(j) == null) {
                    continue;
                }
                Unit u = parents.get(j).parents().get(i);
                if (parents.get(i) == null || (u != null && u.level() > parents.get(i).level())) {
                    parents.set(i, u);
                }
            }
        }
    }

    @FunctionalInterface
    public interface RsData {
        byte[] rsData(int level, List<Unit> parents, int epoch);
    }

    private List<Unit>                                 candidates;
    private final Config                               conf;
    private final DataSource                           ds;
    private int                                        epoch;
    private EpochProof                                 epochProof;
    private final Function<Integer, EpochProofBuilder> epochProofBuilder;
    private Map<Short, Boolean>                        frozen;
    private int                                        level;
    private int                                        maxLvl;
    private short                                      onMaxLvl;
    private int                                        quorum;
    private final RsData                               rsData;
    private final Consumer<Unit>                       send;

    public Creator(Config config, DataSource ds, Consumer<Unit> send, RsData rsData,
                   Function<Integer, EpochProofBuilder> epochProofBuilder) {
        this.conf = config;
        this.ds = ds;
        this.rsData = rsData;
        this.epochProofBuilder = epochProofBuilder;
        this.send = send;

    }

    public void process(Unit candidate, Unit timingUnit) {
        // Step 1: update candidates with all units waiting on the unit belt
        update(candidate);
        if (ready()) {
            // Step 2: get parents and level using current strategy
            var built = buildParents();
            // Step 3: create unit
            createUnit(built.parents, built.level, getData(built.level, timingUnit));
        }
    }

    private built buildParents() {
        if (conf.canSkipLevel()) {
            return new built(getParents(), level);
        } else {
            level = candidates.get(conf.pid()).level() + 1;
            return new built(getParentsForLevel(level), level);
        }
    }

    private void createUnit(List<Unit> parents, int level, Any data) {
        var u = PreUnit.newFreeUnit(conf.pid(), epoch, parents, level, data, rsData.apply(level, parents, epoch),
                                    conf.signer(), conf.digestAlgorithm());
        send.accept(u);
        update(u);
    }

    // produces a piece of data to be included in a unit on a given level.
    // For regular units the provided DataSource is used. For finishing units it's
    // either nil or, if available, an encoded threshold signature share of hash and
    // id of the last timing unit (obtained from preblockMaker on lastTiming
    // channel)
    private Any getData(int level, Unit timingUnit) {
        if (level <= conf.lastLevel()) {
            if (ds != null) {
                return ds.getData();
            }
        }

        if (timingUnit.epoch() == epoch) {
            if (epoch == conf.numberOfEpochs() - 1) {
                // the epoch we just finished is the last epoch we were supposed to produce
                return Any.getDefaultInstance();
            }
            return epochProof.buildShare(timingUnit);
        }

        return Any.getDefaultInstance();
    }

    private List<Unit> getParents() {
        return null;
    }

    // getParentsForLevel returns a set of candidates such that their level is at
    // most level-1.
    private List<Unit> getParentsForLevel(int level) {
        var result = new ArrayList<Unit>();
        for (Unit u : candidates) {
            for (; u != null && u.level() >= level;) {
                u = u.predecessor();
            }
            result.add(u);
        }
        makeConsistent(result);
        return result;
    }

    // newEpoch switches the creator to a chosen epoch, resets candidates and shares
    // and creates a dealing with the provided data.
    private void newEpoch(int epoch, Any data) {
        this.epoch = epoch;
        resetEpoch();
        epochProof = epochProofBuilder.apply(epoch);
        createUnit(Collections.emptyList(), 0, data);
    }

    private boolean ready() {
        return false;
    }

    // resetEpoch resets the candidates and all related variables to the initial
    // state (a slice with NProc nils). This is useful when switching to a new
    // epoch.
    private void resetEpoch() {
        candidates.clear();
        for (int ix = 0; ix < candidates.size(); ix++) {
            candidates.add(null);
        }
        maxLvl = -1;
        onMaxLvl = 0;
        level = 0;
    }

    // update takes a unitand updates the receiver's state with information
    // contained in the unit.
    private void update(Unit unit) {
        // if the unit is from an older epoch or unit's creator is known to be a forker,
        // we simply ignore it
        if (frozen.get(unit.creator()) || unit.epoch() < epoch) {
            return;
        }

        // If the unit is from a new epoch, switch to that epoch.
        // Since units appear on the belt in order they were added to the dag,
        // the first unit from a new epoch is always a dealing unit.
        if (unit.epoch() > epoch) {
            if (!epochProof.verify(unit)) {
                return;
            }
            newEpoch(unit.epoch(), unit.data());
        }

        // If this is a finishing unit try to extract threshold signature share from it.
        // If there are enough shares to produce the signature (and therefore a proof
        // that the current epoch is finished) switch to a new epoch.
        var ep = epochProof.tryBuilding(unit);
        if (ep != null) {
            newEpoch(epoch + 1, ep);
            return;
        }

        updateCandidates(unit);

    }

    // updateCandidates puts the provided unit in parent candidates provided that
    // the level is higher than the level of the previous candidate for that
    // creator.
    private void updateCandidates(Unit u) {
        if (u.epoch() != epoch) {
            return;
        }
        var prev = candidates.get(u.creator());
        if (prev == null || prev.level() < u.level()) {
            candidates.set(u.creator(), u);
            if (u.level() == maxLvl) {
                onMaxLvl++;
            }
            if (u.level() > maxLvl) {
                maxLvl = u.level();
                onMaxLvl = 1;
            }
            level = maxLvl;
            if (onMaxLvl >= quorum) {
                level++;
            }
        }
    }

    public void creatUnits(SubmissionPublisher<Unit> unitBelt, SubmissionPublisher<Unit> lastTiming) {
        // TODO Auto-generated method stub
        
    }

    public boolean epochProof(PreUnit pu, WeakThresholdKey wtKey) {
        // TODO Auto-generated method stub
        return false;
    }

}
