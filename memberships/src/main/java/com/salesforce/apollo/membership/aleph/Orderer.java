/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.aleph;

import static com.salesforce.apollo.membership.aleph.Dag.newDag;

import java.time.Clock;
import java.util.List;
import java.util.function.Consumer;

import com.salesforce.apollo.membership.aleph.Adder.AdderImpl;
import com.salesforce.apollo.membership.aleph.RandomSource.RandomSourceFactory;
import com.salesforce.apollo.membership.aleph.linear.ExtenderService;

/**
 * Orderer orders ordered orders into ordered order.
 * 
 * @author hal.hildebrand
 *
 */
public class Orderer {
    public record epoch(int id, Dag dag, Adder adder, ExtenderService extender, RandomSource rs) {}

    record epochWithNewer(epoch epoch, boolean newer) {}

    public static epoch newEpoch(int id, Config config, RandomSourceFactory rsf, Consumer<Unit> unitBelt, Clock clock) {
        Dag dg = newDag(config, id);
        RandomSource rs = rsf.newRandomSource(dg);
        ExtenderService ext = new ExtenderService(dg, rs, config);
        dg.afterInsert(u -> {
            ext.chooseNextTimingUnits();
            // don't put our own units on the unit belt, creator already knows about them.
            if (u.creator() != config.pid()) {
                unitBelt.accept(u);
            }
        });
        return new epoch(id, dg, new AdderImpl(dg, clock, config.digestAlgorithm()), ext, rs);
    }

    private Config               conf;
    private epoch                current;
    private epoch                previous;
    private Consumer<List<Unit>> toPreblock;
    private Consumer<Unit>       lastTiming;
    private int                  currentProcessing = 0;

    // handleTimingRounds waits for ordered round of units produced by Extenders and
    // produces Preblocks based on them. Since Extenders in multiple epochs can
    // supply ordered rounds simultaneously, handleTimingRounds needs to ensure that
    // Preblocks are produced in ascending order with respect to epochs. For the
    // last ordered round of the epoch, the timing unit defining it is sent to the
    // creator (to produce signature shares.)
    public void handleTimingRound(List<Unit> round) {
        var timingUnit = round.get(round.size() - 1);
        var epoch = timingUnit.epoch();
        if (timingUnit.level() == conf.lastLevel()) {
            lastTiming.accept(timingUnit);
        }
        if (epoch > currentProcessing && timingUnit.level() <= conf.lastLevel()) {
            toPreblock.accept(round);
        }
        currentProcessing = epoch;
    }

    @SuppressWarnings("unused")
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

}
