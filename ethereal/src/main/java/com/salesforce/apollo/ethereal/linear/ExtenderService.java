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
import com.salesforce.apollo.ethereal.RandomSource;
import com.salesforce.apollo.ethereal.Unit;
import com.salesforce.apollo.utils.SimpleChannel;

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

    private final Extender                   ordering;
    private final SimpleChannel<List<Unit>>  output;
    private final SimpleChannel<TimingRound> timingRounds;
    private final SimpleChannel<Boolean>     trigger;

    public ExtenderService(Dag dag, RandomSource rs, Config config, SimpleChannel<List<Unit>> output) {
        ordering = new Extender(dag, rs, config);
        this.output = output;
        trigger = new SimpleChannel<>(100);
        timingRounds = new SimpleChannel<>(config.epochLength());

        trigger.consumeEach(timingUnitDecider());
        timingRounds.consumeEach(roundSorter());
    }

    public void chooseNextTimingUnits() {
        log.info("Signaling to see if we can produce a block");
        trigger.submit(true);
    }

    public void close() {
        trigger.close();
        timingRounds.close();
    }

    /**
     * Picks information about newly picked timing unit from the timingRounds
     * channel, finds all units belonging to their timing round and establishes
     * linear order on them. Sends slices of ordered units to output.
     */
    private Consumer<TimingRound> roundSorter() {
        return round -> {
            var units = round.orderedUnits();
            log.info("Output on: {} preBlock: {}", round);
            output.submit(units);
        };
    }

    /**
     * roundSorter picks information about newly picked timing unit from the
     * timingRounds channel, finds all units belonging to their timing round and
     * establishes linear order on them. Sends slices of ordered units to output.
     */
    private Consumer<Boolean> timingUnitDecider() {
        return t -> {
            var round = ordering.nextRound();
            while (round != null) {
                log.info("Producing round: {}", round);
                timingRounds.submit(round);
                round = ordering.nextRound();
            }
        };
    }
}
