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

//BinarySnowflake is a snowball instance deciding between two values
//After performing a network sample of k nodes, if you have alpha votes for
//one of the choices, you should vote for that choice. Otherwise, you should
//reset.
public class BinarySnowflake extends BinarySlush {
    private int     confidence;
    private int     beta;
    private boolean finalized;

    public BinarySnowflake(int beta, int initialPreference) {
        super(initialPreference);
        this.beta = beta;
    }

    @Override
    public void recordSuccessfulPoll(int choice) {
        if (finalized) {
            return;
        }
        int preference = super.preference();
        if (preference == choice) {
            confidence++;
        } else {
            confidence = 1;
        }
        finalized = confidence >= beta;
        super.recordSuccessfulPoll(choice);
    }

    public void recordUnsuccessfulPoll() {
        confidence = 0;
    }

    public boolean finalized() {
        return finalized;
    }

    @Override
    public String toString() {
        return String.format("BinarySnowflake [confidence=%s, finalized=%s, preference=%s]", confidence, finalized,
                             preference);
    }

}
