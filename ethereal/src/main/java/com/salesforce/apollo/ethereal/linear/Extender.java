/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.linear;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.Dag;
import com.salesforce.apollo.ethereal.Unit;
import com.salesforce.apollo.ethereal.linear.UnanimousVoter.SuperMajorityDecider;

/**
 * Extender is a type that implements an algorithm that extends order of units
 * provided by an instance of a Dag to a linear order.
 * 
 * @author hal.hildebrand
 *
 */
public class Extender {
    private static final int FIRST_DECIDED_ROUND = 3;
    private static Logger    log                 = LoggerFactory.getLogger(Extender.class);

    private final Config                                conf;
    private final Dag                                   dag;
    private final HashMap<Digest, SuperMajorityDecider> deciders = new HashMap<>();
    private final String                                logLabel;

    public Extender(Dag dag, Config conf) {
        this.dag = dag;
        this.conf = conf;
        logLabel = conf.logLabel();
    }

    /**
     * roundSorter picks information about newly picked timing unit from the
     * timingRounds channel, finds all units belonging to their timing round and
     * establishes linear order on them. Sends slices of ordered units to output.
     */
    public TimingRound chooseNextTimingUnits(TimingRound lastTU, Consumer<List<Unit>> output) {
        TimingRound next;
        TimingRound last = lastTU;

        do {
            log.trace("Choose TR, last: {} on: {}", lastTU, conf.logLabel());
            next = nextRound(last);
            if (next != null && !next.equals(last)) {
                var units = next.orderedUnits(conf.digestAlgorithm(), conf.logLabel());
                log.trace("Output of: {} preBlock: {} on: {}", next, units, conf.logLabel());
                output.accept(units);
                last = next;
            } else {
                log.trace("Exit choose TR, last: {} on: {}", next, conf.logLabel());
                return next;
            }
        } while (next != null && !next.equals(last));
        log.trace("Exit choose TR, last: {} on: {}", next, conf.logLabel());
        return next;
    }

    public TimingRound nextRound(TimingRound lastTU) {
        var dagMaxLevel = dag.maxLevel();
        log.trace("Begin round, {} dag mxLvl: {} on: {}", lastTU, dagMaxLevel, FIRST_DECIDED_ROUND, logLabel);
        var level = 0;
        final Unit previousTU = lastTU == null ? null : lastTU.currentTU();
        if (previousTU != null) {
            level = lastTU.level() + 1;
        }
        if (dagMaxLevel < level + FIRST_DECIDED_ROUND) {
            log.trace("No round, dag mxLvl: {} is < ({} + {}) on: {}", dagMaxLevel, level, FIRST_DECIDED_ROUND,
                      logLabel);
            return lastTU;
        }

        var units = dag.unitsOnLevel(level);

        var decided = false;
        Unit currentTU = null;

        for (Unit uc : permutation(level, units, previousTU)) {
            if (uc == null) {
                continue;
            }
            var decision = getDecider(uc, deciders).decideUnitIsPopular(dagMaxLevel);
            if (decision.decision() == Vote.POPULAR) {
                currentTU = uc;
                decided = true;
                deciders.clear();
                log.trace("Popular: {} decided on: {} level: {} max: {} on: {}", uc, decision.decisionLevel(), level,
                          dagMaxLevel, logLabel);
                break;
            }
            if (decision.decision() == Vote.UNDECIDED) {
                log.trace("Undecided: {} decided on: {} level: {} max: {} on: {}", uc, decision.decisionLevel(), level,
                          dagMaxLevel, logLabel);
                break;
            }
            log.trace("Unpopular: {} decided on: {} level: {} max: {} on: {}", uc, decision.decisionLevel(), level,
                      dagMaxLevel, logLabel);

        }
        if (!decided) {
            log.trace("No round decided, dag mxLvl: {} level: {} max: {} on: {}", dagMaxLevel, level, logLabel);
            return lastTU;
        }
        final var current = new TimingRound(currentTU, level, lastTU == null ? null : lastTU.currentTU());
        log.trace("{} dag mxLvl: {} on: {}", current, dagMaxLevel, logLabel);
        return current;
    }

    private SuperMajorityDecider getDecider(Unit uc, HashMap<Digest, SuperMajorityDecider> deciders) {
        return deciders.computeIfAbsent(uc.hash(),
                                        h -> new SuperMajorityDecider(new UnanimousVoter(dag, uc, new HashMap<>(),
                                                                                         logLabel)));
    }

    private List<Unit> permutation(int level, List<Unit> unitsOnLevel, Unit previousTU) {
        final var pidOrder = pidOrder(level, previousTU);
        List<Unit> permutation = pidOrder.stream().map(s -> unitsOnLevel.get(s)).toList();
        if (log.isTraceEnabled()) {
            log.trace("CRP level: {} permutation: {} pidOrder: {} previous: {} on: {}", level,
                      permutation.stream().map(e -> e == null ? null : e.shortString()).toList(), pidOrder,
                      previousTU == null ? null : previousTU.shortString(), logLabel);
        }
        return permutation;
    }

    private List<Short> pidOrder(int level, Unit tu) {
        var pids = new ArrayList<Short>();
        int rnd = Math.abs((tu == null ? 0 : (short) tu.hash().getLongs()[0]));
        for (int pid = 0; pid < conf.nProc(); pid++) {
            pids.add((short) ((pid + level + rnd) % conf.nProc()));
        }
        if (tu == null) {
            return pids;

        }
        for (short pid : new ArrayList<>(pids)) {
            pids.set(pid, (short) ((pids.get(pid) + tu.creator()) % conf.nProc()));
        }
        return pids;

    }
}
