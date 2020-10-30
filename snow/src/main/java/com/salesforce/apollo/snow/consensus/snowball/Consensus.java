/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowball;

import com.salesforce.apollo.snow.ids.Bag;
import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 *
 */
public interface Consensus {
    interface Factory {
        public Consensus construct();
    }

    void add(ID newChoice);

    boolean finalized();

    Parameters parameters();

    ID preference();

    void recordPoll(Bag votes);

    void recordUnsuccessfulPoll();
}
