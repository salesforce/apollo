/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowball.tree;

import com.salesforce.apollo.snow.consensus.snowball.Consensus;
import com.salesforce.apollo.snow.consensus.snowball.Parameters;
import com.salesforce.apollo.snow.consensus.snowball.UnarySnowball;
import com.salesforce.apollo.snow.ids.Bag;
import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 *
 */
public class Tree implements Consensus {

    public static class TreeFactory implements Consensus.Factory {
        @Override
        public Consensus construct() {
            return new Tree();
        }

    }

    private Node       node;
    private Parameters params;

    // shouldReset is used as an optimization to prevent needless tree
    // traversals. If a snowball instance does not get an alpha majority, that
    // instance needs to reset by calling RecordUnsuccessfulPoll. Because the
    // tree splits votes based on the branch, when an instance doesn't get an
    // alpha majority none of the children of this instance can get an alpha
    // majority. To avoid calling RecordUnsuccessfulPoll on the full sub-tree of
    // a node that didn't get an alpha majority, shouldReset is used to indicate
    // that any later traversal into this sub-tree should call
    // RecordUnsuccessfulPoll before performing any other action.
    private boolean shouldReset;

    @Override
    public void add(ID newChoice) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean finalized() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void initialize(Parameters parameters, ID choice) {
        this.params = parameters;
        node = new UnaryNode(this, choice, ID.NumBits, new UnarySnowball());
    }

    @Override
    public Parameters parameters() {
        return params;
    }

    @Override
    public ID preference() { 
        return node.getSnowball().preference();
    }

    @Override
    public void recordPoll(Bag votes) { 
    }

    @Override
    public void recordUnsuccessfulPoll() {
        // TODO Auto-generated method stub

    }

}
