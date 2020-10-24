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

        private final ID         id;
        private final Parameters params;

        public TreeFactory(ID id, Parameters params) {
            this.id = id;
            this.params = params;
        }

        @Override
        public Consensus construct() {
            return new Tree(params, id);
        }

    }

    private Node<?>    node;
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

    public Tree(Parameters parameters, ID choice) {
        this.params = parameters;
        node = new UnaryNode(this, choice, (short) ID.NumBits, new UnarySnowball(parameters.betaVirtuous));
    }

    @Override
    public void add(ID choice) {
        int prefix = node.decidedPrefix();
        // Make sure that we haven't already decided against this new id
        if (ID.equalSubset(0, prefix, preference(), choice)) {
            node = node.add(choice);
        }
    }

    @Override
    public boolean finalized() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Parameters parameters() {
        return params;
    }

    @Override
    public ID preference() {
        return null; // TODO
    }

    @Override
    public void recordPoll(Bag votes) {
        // Get the assumed decided prefix of the root node.
        int decidedPrefix = node.decidedPrefix();

        // If any of the bits differ from the preference in this prefix, the vote is
        // for a rejected operation. So, we filter out these invalid votes.
        Bag filteredVotes = votes.filter(0, decidedPrefix, preference());

        // Now that the votes have been restricted to valid votes, pass them into
        // the first snowball instance
        node = node.recordPoll(filteredVotes, shouldReset);

        // Because we just passed the reset into the snowball instance, we should no
        // longer reset.
        shouldReset = false;
    }

    @Override
    public void recordUnsuccessfulPoll() {
        shouldReset = true;
    }
}
