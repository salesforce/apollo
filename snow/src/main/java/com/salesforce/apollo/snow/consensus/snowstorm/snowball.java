/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowstorm;

/**
 * @author hal.hildebrand
 *
 */
public class snowball {

    private int     numSuccessfulPolls;
    private int     confidence;
    private int     lastVote;
    private boolean rogue;

    public int confidence(int currentVote) {
        if (lastVote != currentVote) {
            return 0;
        }
        return confidence;
    }

    public void recordSuccessfulPoll(int currentVote) {
        // If this choice wasn't voted for during the last poll, the confidence
        // should have been reset during the last poll. So, we reset it now.
        if (lastVote + 1 != currentVote) {
            confidence = 0;
        }

        // This choice was voted for in this poll. Mark it as such.
        lastVote = currentVote;

        // An affirmative vote increases both the snowball and snowflake counters.
        numSuccessfulPolls++;
        confidence++;
    }
    
    public boolean finalized(int betaVirtuous, int betaRogue)   {
        // This choice is finalized if the snowflake counter is at least
        // [betaRogue]. If there are no known conflicts with this operation, it can
        // be accepted with a snowflake counter of at least [betaVirtuous].
        return (!rogue && confidence >= betaVirtuous) ||
            confidence >= betaRogue;
    }
}
