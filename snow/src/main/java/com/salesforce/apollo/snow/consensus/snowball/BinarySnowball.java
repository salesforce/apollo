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

//BinarySnowball augments BinarySnowflake with a counter that tracks the total
//number of positive responses from a network sample.
public class BinarySnowball extends BinarySnowflake {
    private int[] numSucessfulPolls = new int[2];
    private int   preference;

    public BinarySnowball(int beta, int initialPreference) {
        super(beta, initialPreference);
        preference = initialPreference;
    }

    public BinarySnowball(int beta, int originalPreference, int confidence, boolean finalized) {
        super(beta, originalPreference, confidence, finalized);
    }

    @Override
    public int preference() {
        // It is possible, with low probability, that the snowflake preference is
        // not equal to the snowball preference when snowflake finalizes. However,
        // this case is handled for completion. Therefore, if snowflake is
        // finalized, then our finalized snowflake choice should be preferred.
        if (finalized()) {
            return super.preference();
        }
        return preference;
    }

    @Override
    public void recordSuccessfulPoll(int choice) {
        numSucessfulPolls[choice]++;
        if (numSucessfulPolls[choice] > numSucessfulPolls[1 - choice]) {
            preference = choice;
        }
        super.recordSuccessfulPoll(choice);
    }

    void numSuccessfulPoll(int choice, int polls) {
        numSucessfulPolls[choice] = polls;
    }

}
