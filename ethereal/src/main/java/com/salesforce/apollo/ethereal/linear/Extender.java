/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.linear;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.crypto.Digest;
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
    private static Logger log = LoggerFactory.getLogger(Extender.class);

    private final Config conf;
    private final Dag    dag;
    private final String logLabel;

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
        if (dagMaxLevel < conf.orderStartLevel()) {
            log.trace("No round, dag mxLvl: {} is < order start level: {} on: {}", dagMaxLevel, conf.orderStartLevel(),
                      logLabel);
            return lastTU;
        }
        log.trace("Begin round, {} dag mxLvl: {} on: {}", lastTU, dagMaxLevel, conf.firstDecidedRound(), logLabel);
        var level = conf.orderStartLevel();
        final Unit previousTU = lastTU == null ? null : lastTU.currentTU();
        if (previousTU != null) {
            level = lastTU.level() + 1;
        }
        if (dagMaxLevel < level + conf.firstDecidedRound()) {
            log.trace("No round, dag mxLvl: {} is < ({} + {}) on: {}", dagMaxLevel, level, conf.firstDecidedRound(),
                      logLabel);
            return lastTU;
        }

        var units = dag.unitsOnLevel(level);

        var decided = false;
        var deciders = new ConcurrentHashMap<Digest, SuperMajorityDecider>();
        Unit currentTU = null;

        for (Unit uc : commonRandomPermutation(level, units, previousTU)) {
            SuperMajorityDecider decider = getDecider(uc, deciders);
            var decision = decider.decideUnitIsPopular(dagMaxLevel);
            if (decision.decision() == Vote.POPULAR) {
                currentTU = uc;
                decided = true;
                log.trace("Popular: {} level: {} on: {}", uc, level, logLabel);
                break;
            }
            if (decision.decision() == Vote.UNDECIDED) {
                log.trace("Undecided: {} level: {} on: {}", uc, level, logLabel);
                break;
            }
            log.trace("Unpopular: {} level: {} on: {}", uc, level, logLabel);

        }
        if (!decided) {
            log.trace("No round decided, dag mxLvl: {} level: {} on: {}", dagMaxLevel, level, logLabel);
            return lastTU;
        }
        final var current = new TimingRound(currentTU, level, lastTU == null ? null : lastTU.currentTU());
        log.trace("{} dag mxLvl: {} on: {}", current, dagMaxLevel, logLabel);
        return current;
    }

    private List<Unit> commonRandomPermutation(int level, List<List<Unit>> unitsOnLevel, Unit previousTU) {
        final var pidOrder = pidOrder(level, previousTU);
        List<Unit> permutation = randomPermutation(level, pidOrder, unitsOnLevel, previousTU);
        if (log.isWarnEnabled()) {
            log.trace("CRP level: {} permutation: {} pidOrder: {} previous: {} on: {}", level,
                      permutation.stream().map(e -> e.shortString()).toList(), pidOrder,
                      previousTU == null ? null : previousTU.shortString(), logLabel);
        }
        return permutation;
    }

    private SuperMajorityDecider getDecider(Unit uc, Map<Digest, SuperMajorityDecider> deciders) {
        return deciders.computeIfAbsent(uc.hash(),
                                        h -> new SuperMajorityDecider(new UnanimousVoter(dag, uc,
                                                                                         conf.zeroVoteRoundForCommonVote(),
                                                                                         conf.commonVoteDeterministicPrefix(),
                                                                                         new HashMap<>(), logLabel),
                                                                      logLabel));
    }

    private List<Short> pidOrder(int level, Unit tu) {
        var pids = new ArrayList<Short>();
        for (int pid = 0; pid < conf.nProc(); pid++) {
            pids.add((short) ((pid + level) % conf.nProc()));
        }
        if (tu == null) {
            return pids;

        }
        for (short pid : new ArrayList<>(pids)) {
            pids.set(pid, (short) ((pids.get(pid) + tu.creator()) % conf.nProc()));
        }
        return pids;

    }

    private List<Unit> randomPermutation(int level, List<Short> pids, List<List<Unit>> unitsOnLevel, Unit unit) {
        var permutation = new ArrayList<Unit>();
        var priority = new HashMap<Digest, Digest>();

        var cumulative = unit == null ? conf.digestAlgorithm().getOrigin() : unit.hash();
        for (short pid : pids) {
            var units = unitsOnLevel.get(pid);
            if (units.isEmpty()) {
                continue;
            }
            for (var u : units) {
                cumulative = cumulative.xor(u.hash());
                priority.put(u.hash(), cumulative);
            }
            permutation.addAll(units);
        }
        Collections.sort(permutation, (a, b) -> priority.get(a.hash()).compareTo(priority.get(b.hash())));

        return permutation;

    }

}
