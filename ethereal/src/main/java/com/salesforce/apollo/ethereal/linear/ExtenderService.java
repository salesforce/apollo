/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.linear;

import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.Dag;
import com.salesforce.apollo.ethereal.RandomSource;
import com.salesforce.apollo.ethereal.Unit;
import com.salesforce.apollo.utils.Channel;
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

    private final Extender             ordering;
    private final Channel<List<Unit>>  output;
    private final Channel<TimingRound> timingRounds;
    private final Channel<Boolean>     trigger;
    private final Config               config;
    private final Semaphore            exclusive = new Semaphore(1);

    public ExtenderService(Dag dag, RandomSource rs, Config config, Channel<List<Unit>> output) {
        ordering = new Extender(dag, rs, config);
        this.output = output;
        trigger = new SimpleChannel<>(String.format("Trigger for: %s", config.pid()), 100);
        timingRounds = new SimpleChannel<>(String.format("Timing Rounds for: %s", config.pid()), config.epochLength());

        trigger.consumeEach(timingUnitDecider());
        timingRounds.consumeEach(roundSorter());
        this.config = config;
    }

    public void chooseNextTimingUnits() {
        log.trace("Signaling to see if we can produce a block on: {}", config.pid());
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
            var units = round.orderedUnits(config.digestAlgorithm());
            log.debug("Output of: {} preBlock: {} on: {}", round, units, config.pid());
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
            exclusive.acquireUninterruptibly();
            try {
                var round = ordering.nextRound();
                log.trace("Starting timing round: {} on: {}", round, config.pid());
                while (round != null) {
                    log.debug("Producing timing round: {} on: {}", round, config.pid());
                    timingRounds.submit(round);
                    round = ordering.nextRound();
                }
            } finally {
                exclusive.release();
            }
        };
    }
}
