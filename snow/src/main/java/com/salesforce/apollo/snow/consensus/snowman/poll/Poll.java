/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowman.poll;

import com.google.common.collect.Multiset;
import com.salesforce.apollo.snow.ids.Bag;
import com.salesforce.apollo.snow.ids.ID;
import com.salesforce.apollo.snow.ids.ShortID;

/**
 * @author hal.hildebrand
 *
 */
public interface Poll {

    interface Factory {
        Poll newPoll(Multiset<ShortID> vdrs);
    }

    void drop(ShortID vdr);

    boolean finished();

    Bag result();

    void vote(ShortID vdr, ID vote);

}
