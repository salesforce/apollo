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

//UnarySnowball is a snowball instance deciding on one value. After performing
//a network sample of k nodes, if you have alpha votes for the choice, you
//should vote. Otherwise, you should reset.
public class UnarySnowball extends UnarySnowflake {

    private int numSuccessfulPolls;

    public int getNumSuccessfulPolls() {
        return numSuccessfulPolls;
    }

    public UnarySnowball(int beta) {
        super(beta);
    }

    public UnarySnowball(int beta, int confidence, boolean finalized) {
        super(beta, confidence, finalized);
    }

    public void recordSuccessfulPoll() {
        numSuccessfulPolls++;
        super.recordSuccessfulPoll();
    }

    public BinarySnowball extend(int beta, int originalPreference) {
        BinarySnowball bs = new BinarySnowball(beta, originalPreference, confidence(), finalized());
        bs.numSuccessfulPoll(originalPreference, numSuccessfulPolls);
        return bs;
    }

    public UnarySnowball clone() {
        UnarySnowball clone = new UnarySnowball(getBeta(), confidence(), finalized());
        clone.numSuccessfulPolls = numSuccessfulPolls;
        return clone;
    }

}
