/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche;

import java.util.concurrent.TimeUnit;

import com.salesforce.apollo.avalanche.DagWood.DagWoodParameters;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class AvalancheParameters {

    public static class CoreParameters {
        /**
         * The percentage of k members that need to vote for a txn to be preferred
         */
        public double alpha = 0.9;
        /**
         * The confidence value for a transaction for early finalization
         */
        public int    beta1 = 11;
        /**
         * The consecutive counter - i.e. "votes" - for a transaction for finalization
         */
        public int    beta2 = 150;

        /**
         * The number of members to sample for a vote
         */
        public int k = 10;
    }

    /**
     * Core parameters to the Avalanche protocol
     */
    public CoreParameters    core                     = new CoreParameters();
    /**
     * Our "wood"
     */
    public DagWoodParameters dagWood                  = new DagWoodParameters();
    /**
     * Max # of parents to apply to a NoOp txn
     */
    public int               maxNoOpParents           = 10;
    /**
     * Periodic rate we cull "finalized" NoOps that have propagated throughout the
     * network, so can be "culled"
     */
    public long              noOpGenerationCullMillis = 60_000;
    /**
     * How many queries per generation of NoOps
     */
    public long              noOpQueryFactor          = 80;
    /**
     * How many NoOp txns to generate per round
     */
    public int               noOpsPerRound            = 10;
    /**
     * Number of threads to allocate per queries - i.e. how many simultaneous
     * outbound queries we can make.
     */
    public int               outstandingQueries       = 10;                     // Currently same as K
    /**
     * The number of parents we desire for new txns
     */
    public int               parentCount              = 5;
    /**
     * The limit on the Avalanche query batch size
     */
    public int               queryBatchSize           = 40;
    /**
     * Query timeout
     */
    public long              timeout                  = 30;
    /**
     * Query timeout unit
     */
    public TimeUnit          unit                     = TimeUnit.SECONDS;
}
