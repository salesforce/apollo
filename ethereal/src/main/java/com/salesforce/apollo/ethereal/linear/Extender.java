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

    private final int                               commonVoteDeterministicPrefix;
    private final CommonRandomPermutation           crpIterator;
    private AtomicReference<Unit>                   currentTU        = new AtomicReference<>();
    private final Dag                               dag;
    private final Map<Digest, SuperMajorityDecider> deciders         = new ConcurrentHashMap<>();
    private final DigestAlgorithm                   digestAlgorithm;
    private final int                               firstDecidedRound;
    private AtomicBoolean                           lastDecideResult = new AtomicBoolean();
    private AtomicReference<List<Unit>>             lastTUs          = new AtomicReference<>();
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
        commonVoteDeterministicPrefix = conf.commonVoteDeterministicPrefix();
        digestAlgorithm = conf.digestAlgorithm();
        crpIterator = new CommonRandomPermutation(dag, rs, conf.crpFixedPrefix(), digestAlgorithm);
    }

    public TimingRound nextRound() {
        lastDecideResult.set(false);
        var dagMaxLevel = dag.maxLevel();
        if (dagMaxLevel < orderStartLevel) {
            log.trace("No round, max dag level: {} is < order start level: {}", dagMaxLevel, orderStartLevel);
            return null;
        }
        var level = orderStartLevel;
        final Unit c = currentTU.get();
        if (c != null) {
            level = c.level() + 1;
        }
        if (dagMaxLevel < level + firstDecidedRound) {
            log.trace("No round, max dag level: {} is < ({} + {})", dagMaxLevel, level, firstDecidedRound);
            return null;
        }

        var previousTU = currentTU.get();
        var decided = new AtomicBoolean();
        var randomBytesPresent = crpIterator.iterate(level, previousTU, uc -> {
            SuperMajorityDecider decider = getDecider(uc);
            var decision = decider.decideUnitIsPopular(dagMaxLevel);
            if (decision.decision() == Vote.POPULAR) {
                final List<Unit> ltus = lastTUs.get();
                lastTUs.set(ltus.isEmpty() ? ltus : new ArrayList<>(ltus.subList(1, ltus.size())));
                lastTUs.get().add(previousTU);
                currentTU.set(uc);
                lastDecideResult.set(true);
                deciders.clear();
                decided.set(true);
                log.trace("Round, decided");
                return false;
            }
            if (decision.decision() == Vote.UNDECIDED) {
                log.trace("No round, undecided");
                return false;
            }
            return true;
        });
        if (!randomBytesPresent) {
            log.trace("Missing random bytes");
        }
        if (!decided.get()) {
            log.trace("Nothing decided");
            return null;
        }
        log.trace("Timing round: {} last: {}", currentTU.get(), lastTUs.get());
        return new TimingRound(currentTU.get(), new ArrayList<>(lastTUs.get()));
    }

    private SuperMajorityDecider getDecider(Unit uc) {
        return deciders.computeIfAbsent(uc.hash(),
                                        h -> new SuperMajorityDecider(new UnanimousVoter(dag, randomSource, uc,
                                                                                         zeroVoteRoundForCommonVote,
                                                                                         commonVoteDeterministicPrefix,
                                                                                         new HashMap<>())));
    }
}
