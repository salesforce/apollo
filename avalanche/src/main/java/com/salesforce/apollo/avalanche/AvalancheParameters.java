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
    private static final String DEFAULT_CONNECTION = "jdbc:h2:mem:dagCache;LOCK_MODE=0;EARLY_FILTER=TRUE;MULTI_THREADED=1;MVCC=TRUE";

    /**
     * The percentage of k members that need to vote for a txn to be preferred
     */
    public double            alpha             = 0.9;
    /**
     * The confidence value for a transaction for early finalization
     */
    public int               beta1             = 11;
    /**
     * The consecutive counter - i.e. "votes" - for a transaction for finalization
     */
    public int               beta2             = 150;
    public DagWoodParameters dagWood           = new DagWoodParameters();
    /**
     * The JDBC connection URL
     */
    public String            dbConnect         = DEFAULT_CONNECTION;
    /**
     * The number of FF rounds per NoOp generation round
     */
    public int               delta             = 1;
    /**
     * The number of FF rounds per Avalanche round
     */
    public int               epsilon           = 1;
    public int               finalizeBatchSize = 40;
    /**
     * The number of frontier keys to keep on hand
     */
    public int frontier = 200;
    /**
     * The number of queries per FF round
     */
    public int               gamma             = 20;

    /**
     * The number of members to sample for a vote
     */
    public int               k                 = 10;

    /**
     * Max JDBC connections in pool
     */
    public int maxActiveQueries = 100;

    public int maxNoOpParents = 100;

    public int noOpsPerRound = 1;

    /**
     * The number of parents we desire for new txns
     */
    public int parentCount = 3;

    /**
     * The limit on the Avalanche query batch size
     */
    public int queryBatchSize = 40;

    /**
     * Query timeout
     */
    public long timeout = 30;

    /**
     * Query timeout unit
     */
    public TimeUnit unit = TimeUnit.SECONDS;
}
