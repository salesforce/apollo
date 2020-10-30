/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowball.tree;

import com.salesforce.apollo.snow.ids.Bag;
import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 *
 */
abstract public class Node<Snowball> {

    // snowball wraps the snowball logic
    protected final Snowball snowball;

    // tree references the tree that contains this node
    protected final Tree tree;

    public Node(Tree tree, Snowball snowball) {
        this.tree = tree;
        this.snowball = snowball;
    }

    abstract public Node<?> add(ID newChoice);

    abstract public int decidedPrefix();

    abstract public boolean finalized();

    public Snowball getSnowball() {
        return snowball;
    }

    abstract public ID preference();

    abstract public Node<?> recordPoll(Bag votes, boolean shouldReset);

}
