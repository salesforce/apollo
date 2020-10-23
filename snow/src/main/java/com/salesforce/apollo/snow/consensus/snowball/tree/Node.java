/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowball.tree;

import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 *
 */
public class Node<Snowball> {
    // child is the, possibly nil, node that votes on the next bits in the
// decision
    protected Node<Snowball> child;

// commonPrefix is the last bit in the prefix that this node transitively
// references
    protected int commonPrefix; // Will be in the range (decidedPrefix, 256)

// decidedPrefix is the last bit in the prefix that is assumed to be decided
    protected int decidedPrefix; // Will be in the range [0, 255)

// preference is the choice that is preferred at every branch in this
// sub-tree
    protected ID preference;

// shouldReset is used as an optimization to prevent needless tree
// traversals. It is the continuation of shouldReset in the Tree struct.
    protected boolean shouldReset;

// snowball wraps the snowball logic
    protected final Snowball snowball;

// tree references the tree that contains this node
    protected final Tree tree;

    public Node(Tree tree, ID choice, int numbits, Snowball snowball) {
        this.tree = tree;
        this.preference = choice;
        this.commonPrefix = numbits;
        this.snowball = snowball;
    }

    public Snowball getSnowball() {
        return snowball;
    }
}
