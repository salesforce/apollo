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

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.aleph.Config;
import com.salesforce.apollo.membership.aleph.Dag;
import com.salesforce.apollo.membership.aleph.RandomSource;
import com.salesforce.apollo.membership.aleph.Unit;
import com.salesforce.apollo.membership.aleph.linear.UnanimousVoter.SuperMajorityDecider;

/**
 * Extender is a type that implements an algorithm that extends order of units
 * provided by an instance of a Dag to a linear order.
 * 
 * @author hal.hildebrand
 *
 */
public class Extender {
    public Extender(Dag dag, RandomSource rs, Config conf) {
        this.dag = dag;
        randomSource = rs;
        lastTUs = new ArrayList<>();
        zeroVotRoundForCommonVote = conf.zeroVotRoundForCommonVote();
        firstDecidedRound = conf.firstDecidedRound();
        orderStartLevel = conf.orderStartLevel();
        commonVoteDeterministicPrefix = conf.commonVoteDeterministicPrefix();
        crpIterator = new CommonRandomPermutation(dag, rs, conf.crpFixedPrefix(), digestAlgorithm);
    }

    private final Map<Digest, SuperMajorityDecider> deciders = new HashMap<>();
    private final Dag                               dag;
    private final RandomSource                      randomSource;
    List<Unit>                                      lastTUs;
    Unit                                            currentTU;
    boolean                                         lastDecideResult;
    int                                             zeroVotRoundForCommonVote;
    int                                             firstDecidedRound;
    int                                             orderStartLevel;
    int                                             commonVoteDeterministicPrefix;
    CommonRandomPermutation                         crpIterator;
    DigestAlgorithm                                 digestAlgorithm;

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
        var decided = false;
        var randomBytesPresent = crpIterator.iterate(level, previousTU, unit -> {

            return false;
        });
        return null;
    }
}
