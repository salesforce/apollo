/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche;

import java.util.concurrent.TimeUnit;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class AvalancheParameters {
    private static final String DEFAULT_CONNECTION = "jdbc:h2:mem:dagCache";

    /**
     * The percentage of k members that need to vote for a txn to be preferred
     */
    public double alpha = 0.9;
    /**
     * The confidence value for a transaction for early finalization
     */
    public int beta1 = 11;
    /**
     * The consecutive counter - i.e. "votes" - for a transaction for finalization
     */
    public int beta2 = 150;
    /**
     * The JDBC connection URL
     */
    public String dbConnect = DEFAULT_CONNECTION;
    /**
     * The number of queries per FF round
     */
    public int gamma = 2;
    /**
     * The number of FF rounds per NoOp generation round
     */
    public int delta = 9;
    /**
     * The number of FF rounds per Avalanche round
     */
    public int epsilon = 9;
    /**
     * The number of members to sample for a vote
     */
    public int k = 10;
    /**
     * The limit on gossipped DAG entries and the limit of dag entries in query batch
     */
    public int limit = 40;
    /**
     * The number of parents we desire for new txns
     */
    public int parentCount = 3;
    /**
     * Query timeout
     */
    public long timeout = 30;

    /**
     * Query timeout unit
     */
    public TimeUnit unit = TimeUnit.SECONDS;

    /**
     * Max JDBC connections in pool
     */
    public int maxQueries = 3;

    public int noOpsPerRound = 1;

    public int finalizeBatchSize = 50;

    public int insertBatchSize = 100;

    public int preferBatchSize = 50;
}
