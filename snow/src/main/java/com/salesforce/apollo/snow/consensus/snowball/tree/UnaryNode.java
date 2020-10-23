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
public class UnaryNode implements Node {
    public UnaryNode(Tree tree, ID choice, int numbits, UnarySnowball snowball) {
        this.tree = tree;
        this.preference = choice;
        this.commonPrefix = numbits;
        this.snowball = snowball;
    }

    // tree references the tree that contains this node
    private final Tree tree;

    // preference is the choice that is preferred at every branch in this
    // sub-tree
    private ID preference;

    // decidedPrefix is the last bit in the prefix that is assumed to be decided
    private int decidedPrefix; // Will be in the range [0, 255)

    // commonPrefix is the last bit in the prefix that this node transitively
    // references
    private int commonPrefix; // Will be in the range (decidedPrefix, 256)

    // snowball wraps the snowball logic
    private UnarySnowball snowball;

    // shouldReset is used as an optimization to prevent needless tree
    // traversals. It is the continuation of shouldReset in the Tree struct.
    private boolean shouldReset;

    // child is the, possibly nil, node that votes on the next bits in the
    // decision
    private Node child;
}
