/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowball.tree;

import com.salesforce.apollo.snow.consensus.snowball.BinarySnowball;
import com.salesforce.apollo.snow.ids.Bag;
import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 *
 */
public class BinaryNode extends Node<BinarySnowball> {
    private final int       bit;
    private final Node<?>[] children    = new Node[2];
    private final ID[]      preferences = new ID[2];
    private final boolean[] shouldReset = new boolean[2];

    public BinaryNode(Tree tree, int index, BinarySnowball snowball, ID newChoice, boolean shouldReset, ID preference,
            int bit) {
        super(tree, snowball);
        this.shouldReset[0] = shouldReset;
        this.shouldReset[1] = shouldReset;
        this.bit = index;
        preferences[bit] = preference;
        preferences[1 - bit] = newChoice;
    }

    @Override
    public Node<?> add(ID id) {
        int b = id.bit(bit);
        Node<?> child = children[b];
        // If child is nil, then we are running an instance on the last bit. Finding
        // two hashes that are equal up to the last bit would be really cool though.
        // Regardless, the case is handled
        if (child != null && // + 1 is used because we already explicitly check the p.bit bit
                ID.equalSubset(b + 1, (int) child.decidedPrefix(), preferences[b], id)) {
            children[b] = child.add(id);
        }
        // If child is nil, then the id has already been added to the tree, so
        // nothing should be done
        // If the decided prefix isn't matched, then a previous decision has made
        // the id that is being added to have already been rejected
        return this;
    }

    @Override
    public int decidedPrefix() {
        return bit;
    }

    @Override
    public boolean finalized() {
        return snowball.finalized();
    }

    @Override
    public ID preference() {
        return preferences[snowball.preference()];
    }

    @Override
    public Node<?> recordPoll(Bag votes, boolean reset) {
        // The list of votes we are passed is split into votes for bit 0 and votes
        // for bit 1
        Bag[] splitVotes = votes.split(bit);

        final int bitIndex; // Because alpha > k/2, only the larger count could be increased
        if (splitVotes[0].size() < splitVotes[1].size()) {
            bitIndex = 1;
        } else {
            bitIndex = 0;
        }

        if (reset) {
            snowball.recordUnsuccessfulPoll();
            shouldReset[bitIndex] = true;
            // 1-bit isn't set here because it is set below anyway
        }
        shouldReset[1 - bitIndex] = true; // They didn't get the threshold of votes

        Bag prunedVotes = splitVotes[bitIndex];
        // If this bit got alpha votes, it was a successful poll
        if (prunedVotes.size() >= tree.parameters().alpha) {
            snowball.recordSuccessfulPoll(bitIndex);
            Node<?> child = children[bitIndex];
            if (child != null) {
                // The votes are filtered to ensure that they are votes that should
                // count for the child
                Bag filteredVotes = prunedVotes.filter(this.bit + 1, child.decidedPrefix(), preferences[bitIndex]);

                if (snowball.finalized()) {
                    // If we are decided here, that means we must have decided due
                    // to this poll. Therefore, we must have decided on bit.
                    return child.recordPoll(filteredVotes, shouldReset[bitIndex]);
                }
                Node<?> newChild = child.recordPoll(filteredVotes, shouldReset[bitIndex]);
                children[bitIndex] = newChild;
                preferences[bitIndex] = newChild.preference();
            }
            shouldReset[bitIndex] = false; // We passed the reset down
        } else {
            snowball.recordUnsuccessfulPoll();
            // The winning child didn't get enough votes either
            shouldReset[bitIndex] = true;
        }
        return this;
    }

    void setChild(int bit, Node<?> child) {
        children[bit] = child;
    }

}
