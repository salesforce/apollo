/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowball;

import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 *
 */

//NnarySnowflake is a snowflake instance deciding between an unbounded number
//of values. After performing a network sample of k nodes, if you have alpha
//votes for one of the choices, you should vote for that choice. Otherwise, you
//should reset.
public class NnarySnowflake extends NnarySlush {
    private int     betaRogue;
    private int     betaVirtuous;
    private int     confidence;
    private boolean finalized;
    private boolean rogue;

    public NnarySnowflake(int betaVirtuous, int betaRogue, ID initialPreference) {
        super(initialPreference);
        this.betaVirtuous = betaVirtuous;
        this.betaRogue = betaRogue;

    }

    public void add(ID choice) {
        rogue = rogue || !choice.equals(preference());
    }

    public boolean finalized() {
        return finalized;
    }

    public boolean isRogue() {
        // TODO Auto-generated method stub
        return rogue;
    }

    @Override
    public void recordSuccessfulPoll(ID choice) {
        if (finalized) {
            return;
        }

        ID preference = preference();
        if (preference.equals(choice)) {
            confidence++;
        } else {
            confidence = 1;
        }
        finalized = (!rogue && confidence >= betaVirtuous) || confidence >= betaRogue;
        super.recordSuccessfulPoll(choice);
    }

    public void recordUnsuccessfulPoll() {
        confidence = 0;
    }

    // for testing
    ID sfPreference() {
        return super.preference();
    }
}
