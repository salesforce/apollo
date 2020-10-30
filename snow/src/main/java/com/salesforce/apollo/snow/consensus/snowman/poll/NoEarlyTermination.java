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
public class NoEarlyTermination implements Poll {

    private final Multiset<ShortID> polled;
    private final Bag               votes = new Bag();

    public NoEarlyTermination(Multiset<ShortID> polled) {
        this.polled = polled;
    }

    @Override
    public void drop(ShortID vdr) {
        polled.remove(vdr);
    }

    @Override
    public boolean finished() {
        return polled.size() == 0;
    }

    @Override
    public Bag result() {
        return votes;
    }

    @Override
    public void vote(ShortID vdr, ID vote) {
        int count = polled.count(vdr);
        polled.remove(vdr);
        votes.addCount(vote, count);
    }

}
