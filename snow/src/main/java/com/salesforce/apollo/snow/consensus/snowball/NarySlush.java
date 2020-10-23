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

//NnarySlush is a slush instance deciding between an unbounded number of
//values. After performing a network sample of k nodes, if you have alpha
//votes for one of the choices, you should vote for that choice.
public interface NarySlush {

    void initialize(ID initialPreference);

    ID preference();

    void recordSuccessfulPoll(ID choice);
}
