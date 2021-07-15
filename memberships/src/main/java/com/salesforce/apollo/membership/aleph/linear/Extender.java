/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.aleph.linear;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.aleph.Config;
import com.salesforce.apollo.membership.aleph.Dag;
import com.salesforce.apollo.membership.aleph.RandomSource;
import com.salesforce.apollo.membership.aleph.Unit;
import com.salesforce.apollo.membership.aleph.linear.UnanimousVoter.SuperMajorityDecider;
import com.salesforce.apollo.membership.aleph.linear.UnanimousVoter.Vote;

/**
 * Extender is a type that implements an algorithm that extends order of units
 * provided by an instance of a Dag to a linear order.
 * 
 * @author hal.hildebrand
 *
 */
public class Extender {
    public Extender(Dag dag, RandomSource rs, Config conf, DigestAlgorithm digestAlgorithm) {
        this.digestAlgorithm = digestAlgorithm;
        this.dag = dag;
        randomSource = rs;
        lastTUs = new ArrayList<>();
        zeroVoteRoundForCommonVote = conf.zeroVotRoundForCommonVote();
        firstDecidedRound = conf.firstDecidedRound();
        orderStartLevel = conf.orderStartLevel();
        commonVoteDeterministicPrefix = conf.commonVoteDeterministicPrefix();
        crpIterator = new CommonRandomPermutation(dag, rs, conf.crpFixedPrefix(), digestAlgorithm);
    }

    private final Map<Digest, SuperMajorityDecider> deciders = new HashMap<>();
    private final Dag                               dag;
    private final RandomSource                      randomSource;
    private List<Unit>                              lastTUs;
    private Unit                                    currentTU;
    private boolean                                 lastDecideResult;
    private int                                     zeroVoteRoundForCommonVote;
    private int                                     firstDecidedRound;
    private int                                     orderStartLevel;
    private int                                     commonVoteDeterministicPrefix;
    private CommonRandomPermutation                 crpIterator;
    private final DigestAlgorithm                   digestAlgorithm;

    public TimingRound nextRound() {
        if (lastDecideResult) {
            lastDecideResult = false;
        }
        var dagMaxLevel = dag.maxLevel();
        if (dagMaxLevel < orderStartLevel) {
            return null;
        }
        var level = orderStartLevel;
        if (currentTU != null) {
            level = currentTU.level() + 1;
        }
        if (dagMaxLevel < level + firstDecidedRound) {
            return null;
        }

        var previousTU = currentTU;
        var decided = new AtomicBoolean();
        var randomBytesPresent = crpIterator.iterate(level, previousTU, uc -> {
            SuperMajorityDecider decider = getDecider(uc);
            var decision = decider.decideUnitIsPopular(dagMaxLevel);
            if (decision.decision() == Vote.POPULAR) {
                lastTUs = lastTUs.isEmpty() ? lastTUs : lastTUs.subList(1, lastTUs.size());
                lastTUs.add(currentTU);
                currentTU = uc;
                lastDecideResult = true;
                deciders.clear();
                decided.set(true);
                return false;
            }
            if (decision.decision() == Vote.UNDECIDED) {
                return false;
            }
            return true;
        });
        if (!randomBytesPresent) {
            log.info("Missing random bytes");
        }
        if (!decided.get()) {
            return null;
        }
        return new TimingRound(currentTU, lastTUs, digestAlgorithm);
    }

    private static Logger log = LoggerFactory.getLogger(Extender.class);

    private SuperMajorityDecider getDecider(Unit uc) {
        return deciders.computeIfAbsent(uc.hash(),
                                        h -> new SuperMajorityDecider(new UnanimousVoter(dag, randomSource, uc,
                                                                                         zeroVoteRoundForCommonVote,
                                                                                         commonVoteDeterministicPrefix,
                                                                                         new HashMap<>())));
    }
}
