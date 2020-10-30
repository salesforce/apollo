/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowball;

/**
 * @author hal.hildebrand
 *
 */

//UnarySnowflake is a snowflake instance deciding on one value. After
//performing a network sample of k nodes, if you have alpha votes for the
//choice, you should vote. Otherwise, you should reset.
public class UnarySnowflake implements Cloneable {

    private final int beta;
    private int       confidence;
    private boolean   finalized;

    public UnarySnowflake(int beta) {
        this.beta = beta;
    }

    public UnarySnowflake(int beta, int confidence, boolean finalized) {
        this(beta);
        this.confidence = confidence;
        this.finalized = finalized;
    }

    public UnarySnowflake clone() {
        UnarySnowflake clone = new UnarySnowflake(beta);
        clone.confidence = confidence;
        clone.finalized = finalized;
        return clone;
    }

    public int confidence() {
        return confidence;
    }

    public BinarySnowflake extend(int beta, int choice) {
        return new BinarySnowflake(beta, choice, confidence, finalized);
    }

    public boolean finalized() {
        return finalized;
    }

    public int getBeta() {
        return beta;
    }

    public void recordSuccessfulPoll() {
        confidence++;
        finalized = finalized || confidence >= beta;

    }

    public void recordUnsuccessfulPoll() {
        confidence = 0;
    }
}
