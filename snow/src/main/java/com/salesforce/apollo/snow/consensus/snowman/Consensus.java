/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowman;

import com.salesforce.apollo.snow.Context;
import com.salesforce.apollo.snow.consensus.snowball.Parameters;
import com.salesforce.apollo.snow.ids.Bag;
import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 *
 */
public interface Consensus {

    interface Factory {
        Consensus construct(Context context, Parameters params, ID rootId, Metrics metrics);
    }

    void add(Block b);

    boolean finalized();

    boolean issued(Block b);

    Parameters parameters();

    ID preference();

    void recordPoll(Bag poll);

}
