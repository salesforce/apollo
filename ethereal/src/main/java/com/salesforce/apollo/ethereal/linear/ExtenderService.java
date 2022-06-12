/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.linear;

import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.Dag;
import com.salesforce.apollo.ethereal.Unit;

/**
 * ExtenderService is a component working on a dag that extends a partial order
 * of units defined by dag to a linear order. ExtenderService should be
 * notified, by the means of its chooseNextTimingUnits() method, when it should
 * try to perform its task. If successful, ExtenderService collects all the
 * units belonging to newest timing round, and sends them to the output channel.
 * 
 * @author hal.hildebrand
 *
 */
public class ExtenderService {
    private static final Logger log = LoggerFactory.getLogger(ExtenderService.class);

    private final Config               config;
    private final Dag                  dag;
    private final Extender             ordering;
    private final Consumer<List<Unit>> output;

    public ExtenderService(Dag dag, Config config, Consumer<List<Unit>> orderedUnits) {
        ordering = new Extender(dag, config);
        this.output = orderedUnits;
        this.config = config;
        this.dag = dag;
    }

    public void chooseNextTimingUnits() {
        dag.read(() -> {
            log.trace("Signaling to see if we can produce a block on: {}", config.logLabel());
            timingUnitDecider();
        });
    }

    /**
     * roundSorter picks information about newly picked timing unit from the
     * timingRounds channel, finds all units belonging to their timing round and
     * establishes linear order on them. Sends slices of ordered units to output.
     */
    private void timingUnitDecider() {
        var round = ordering.nextRound();
        while (round != null) {
            log.trace("Producing timing round: {} on: {}", round, config.logLabel());
            var units = round.orderedUnits(config.digestAlgorithm());
            log.trace("Output of: {} preBlock: {} on: {}", round, units, config.logLabel());
            output.accept(units);
            round = ordering.nextRound();
        }
    }
}
