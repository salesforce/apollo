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
public interface NarySnowflake {
    void initialize(int betaVirtuous, int betaRogue, ID initialPreference);

    void add(ID newChoice);

    ID preference();

    void recordSuccessfulPoll(ID choice);

    void recordUnsuccessfulPoll();

    boolean finalized();
}
