/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowball;

import java.util.HashMap;
import java.util.Map;

import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 *
 */

//NnarySnowball augments NnarySnowflake with a counter that tracks the total
//number of positive responses from a network sample.
public class NnarySnowball extends NnarySnowflake {
    private ID                     preference;
    private int                    maxSuccessfulPolls;
    private final Map<ID, Integer> numSuccessfulPolls = new HashMap<>();

    public NnarySnowball(int betaVirtuous, int betaRogue, ID initialPreference) {
        super(betaVirtuous, betaRogue, initialPreference);
        this.preference = initialPreference;
    }

    @Override
    public ID preference() {
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
    public void recordSuccessfulPoll(ID choice) {
        Integer numSuccess = numSuccessfulPolls.getOrDefault(choice, 0) + 1;
        numSuccessfulPolls.put(choice, numSuccess);
        if (numSuccess > maxSuccessfulPolls) {
            preference = choice;
            maxSuccessfulPolls = numSuccess;
        }

        super.recordSuccessfulPoll(choice);
    }

}
