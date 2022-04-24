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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.Dag;
import com.salesforce.apollo.ethereal.RandomSource;
import com.salesforce.apollo.ethereal.Unit;
import com.salesforce.apollo.ethereal.linear.UnanimousVoter.SuperMajorityDecider;
import com.salesforce.apollo.ethereal.linear.UnanimousVoter.Vote;

/**
 * Extender is a type that implements an algorithm that extends order of units
 * provided by an instance of a Dag to a linear order.
 * 
 * @author hal.hildebrand
 *
 */
public class Extender {
    private static Logger log = LoggerFactory.getLogger(Extender.class);

    private final CommonRandomPermutation           crpIterator;
    private AtomicReference<Unit>                   currentTU = new AtomicReference<>();
    private final Dag                               dag;
    private final Map<Digest, SuperMajorityDecider> deciders  = new ConcurrentHashMap<>();
    private final DigestAlgorithm                   digestAlgorithm;
    private final int                               firstDecidedRound;
    private AtomicReference<List<Unit>>             lastTUs   = new AtomicReference<>();
    private final int                               orderStartLevel;
    private final RandomSource                      randomSource;
    private final int                               zeroVoteRoundForCommonVote;

    public Extender(Dag dag, RandomSource rs, Config conf) {
        this.dag = dag;
        randomSource = rs;
//        lastTUs = IntStream.range(0, conf.zeroVoteRoundForCommonVote()).mapToObj(i -> (Unit) null)
//                           .collect(Collectors.toList());
        lastTUs.set(new ArrayList<>());
        zeroVoteRoundForCommonVote = conf.zeroVoteRoundForCommonVote();
        firstDecidedRound = conf.firstDecidedRound();
        orderStartLevel = conf.orderStartLevel();
        digestAlgorithm = conf.digestAlgorithm();
        crpIterator = new CommonRandomPermutation(dag, rs, (short) (Dag.minimalTrusted(conf.nProc()) + 1),
                                                  digestAlgorithm);
    }

    public TimingRound nextRound() {
        var dagMaxLevel = dag.maxLevel();
        if (dagMaxLevel < orderStartLevel) {
            log.trace("No round, max dag level: {} is < order start level: {} on: {}", dagMaxLevel, orderStartLevel,
                      dag.pid());
            return null;
        }
        var level = orderStartLevel;
        final Unit previousTU = currentTU.get();
        if (previousTU != null) {
            level = previousTU.level() + 1;
        }
        if (dagMaxLevel < level + firstDecidedRound) {
            log.trace("No round, max dag level: {} is < ({} + {}) on: {}", dagMaxLevel, level, firstDecidedRound,
                      dag.pid());
            return null;
        }

        var units = dag.unitsOnLevel(level);
        var count = 0;
        for (short i = 0; i < dag.nProc(); i++) {
            final var atPid = units.get(i);
            if (atPid != null && !atPid.isEmpty()) {
                count++;
            }
        }
        if (count <= Dag.minimalTrusted(dag.nProc())) {
            log.trace("No round, max dag level: {} level: {} count: {} is less than quorum on: {}", dagMaxLevel, level,
                      count, dag.pid());
            return null;
        } else {
            log.trace("Round proceeding, max dag level: {} level: {} count: {} on: {}", dagMaxLevel, level, count,
                      dag.pid());
        }

        var decided = new AtomicBoolean();
        if (!crpIterator.iterate(level, units, previousTU, uc -> {
            SuperMajorityDecider decider = getDecider(uc, crpIterator.crpFixedPrefix());
            var decision = decider.decideUnitIsPopular(dagMaxLevel);
            if (decision.decision() == Vote.POPULAR) {
                final List<Unit> ltus = lastTUs.get();
                var next = ltus.isEmpty() ? ltus : new ArrayList<>(ltus.subList(1, ltus.size()));
                next.add(previousTU);
                lastTUs.set(next);
                currentTU.set(uc);
                deciders.clear();
                decided.set(true);
                log.trace("Round decided");
                return false;
            }
            if (decision.decision() == Vote.UNDECIDED) {
                log.trace("No round, undecided on: {}", dag.pid());
                return false;
            }
            return true;
        })) {
            log.trace("No round, max dag level: {} level: {} count: {} could not match permutation on: {}", dagMaxLevel,
                      level, count, dag.pid());
            return null;
        }
        if (!decided.get()) {
            log.trace("Nothing decided");
            return null;
        }
        final var ctu = currentTU.get();
        final var ltu = lastTUs.get();
        log.trace("Timing round: {} last: {} count: {} dag mxLvl: {} on: {}", ctu, ltu, count, dagMaxLevel, dag.pid());
        return new TimingRound(ctu, new ArrayList<>(ltu));
    }

    private SuperMajorityDecider getDecider(Unit uc, short prefix) {
        return deciders.computeIfAbsent(uc.hash(),
                                        h -> new SuperMajorityDecider(new UnanimousVoter(dag, randomSource, uc,
                                                                                         zeroVoteRoundForCommonVote,
                                                                                         prefix, new HashMap<>())));
    }
}
