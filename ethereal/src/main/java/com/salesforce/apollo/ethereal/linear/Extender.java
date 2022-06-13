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

    private final int                               commonVoteDeterministicPrefix;
    private final CommonRandomPermutation           crpIterator;
    private AtomicReference<Unit>                   currentTU = new AtomicReference<>();
    private final Dag                               dag;
    private final Map<Digest, SuperMajorityDecider> deciders  = new ConcurrentHashMap<>();
    private final DigestAlgorithm                   digestAlgorithm;
    private final int                               firstDecidedRound;
    private AtomicReference<List<Unit>>             lastTUs   = new AtomicReference<>();
    private final int                               orderStartLevel;
    private final int                               zeroVoteRoundForCommonVote;

    public Extender(Dag dag, Config conf) {
        this.dag = dag;
        lastTUs.set(new ArrayList<>());
        zeroVoteRoundForCommonVote = conf.zeroVoteRoundForCommonVote();
        firstDecidedRound = conf.firstDecidedRound();
        orderStartLevel = conf.orderStartLevel();
        digestAlgorithm = conf.digestAlgorithm();
        commonVoteDeterministicPrefix = conf.commonVoteDeterministicPrefix();
        crpIterator = new CommonRandomPermutation(dag.nProc(), digestAlgorithm, conf.logLabel());
    }

    public TimingRound nextRound() {
        var dagMaxLevel = dag.maxLevel();
        if (dagMaxLevel < orderStartLevel) {
            log.trace("No round, dag mxLvl: {} is < order start level: {} on: {}", dagMaxLevel, orderStartLevel,
                      dag.pid());
            return null;
        }
        var level = orderStartLevel;
        final Unit previousTU = currentTU.get();
        if (previousTU != null) {
            level = previousTU.level() + 1;
        }
        if (dagMaxLevel < level + firstDecidedRound) {
            log.trace("No round, dag mxLvl: {} is < ({} + {}) on: {}", dagMaxLevel, level, firstDecidedRound,
                      dag.pid());
            return null;
        }

        var units = dag.unitsOnLevel(level);

        var decided = new AtomicBoolean();
        crpIterator.iterate(level, units, previousTU, uc -> {
            SuperMajorityDecider decider = getDecider(uc);
            var decision = decider.decideUnitIsPopular(dagMaxLevel);
            if (decision.decision() == Vote.POPULAR) {
                final List<Unit> ltus = lastTUs.get();
//                var next = ltus.isEmpty() ? ltus : new ArrayList<>(ltus.subList(1, ltus.size()));
                if (previousTU != null) {
                    ltus.add(previousTU);
                }
                lastTUs.set(ltus);
                currentTU.set(uc);
                deciders.clear();
                decided.set(true);
                log.trace("Round decided: {} on: {}", uc.level(), dag.pid());
                return false;
            }
            if (decision.decision() == Vote.UNDECIDED) {
                log.trace("No round, undecided on: {}", dag.pid());
                return false;
            }
            return true;
        });
        if (!decided.get()) {
            log.trace("No round decided, dag mxLvl: {} level: {} on: {}", dagMaxLevel, level, dag.pid());
            return null;
        }
        final var ctu = currentTU.get();
        final var ltu = lastTUs.get();
        if (log.isTraceEnabled()) {
            log.trace("Timing round: {} last: {} dag mxLvl: {} level: {} on: {}", ctu.shortString(),
                      ltu.isEmpty() ? "null" : ltu.get(ltu.size() - 1).shortString(), dagMaxLevel, level, dag.pid());
        }
        return new TimingRound(ctu, new ArrayList<>(ltu));
    }

    private SuperMajorityDecider getDecider(Unit uc) {
        return deciders.computeIfAbsent(uc.hash(),
                                        h -> new SuperMajorityDecider(new UnanimousVoter(dag, uc,
                                                                                         zeroVoteRoundForCommonVote,
                                                                                         commonVoteDeterministicPrefix,
                                                                                         new HashMap<>())));
    }
}
