/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowball.tree;

import com.salesforce.apollo.snow.consensus.snowball.UnarySnowball;
import com.salesforce.apollo.snow.ids.Bag;
import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 *
 */
public class UnaryNode extends Node<UnarySnowball> {
    // child is the, possibly nil, node that votes on the next bits in the
    // decision
    protected Node<?> child;

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

    public UnaryNode(Tree tree, ID choice, int commonPrefix, UnarySnowball snowball) {
        super(tree, snowball);
        this.preference = choice;
        this.commonPrefix = commonPrefix;
    }

    public UnaryNode(Tree tree, ID choice, int commonPrefix, UnarySnowball unarySnowball, int decidedPrefix) {
        this(tree, choice, commonPrefix, unarySnowball);
        this.decidedPrefix = decidedPrefix;
    }

    public UnaryNode(Tree tree, ID choice, int commonPrefix, UnarySnowball snowball, int decidedPrefix, Node<?> child) {
        this(tree, choice, commonPrefix, snowball, decidedPrefix);
        this.child = child;
    }

    /**
     * <pre>
     *  This is by far the most complicated function in this algorithm.
     *  The intuition is that this instance represents a series of consecutive
     *  unary snowball instances, and this function's purpose is convert one of
     *  these unary snowball instances into a binary snowball instance.
     *  There are 5 possible cases.
     *  
     *  1. None of these instances should be split, we should attempt to split
     *     a child
     * 
     *          For example, attempting to insert the value "00001" in this node:
     * 
     *                         +-------------------+ <-- This node will not be split
     *                         |                   |
     *                         |       0 0 0       |
     *                         |                   |
     *                         +-------------------+ <-- Pass the add to the child
     *                                   ^
     *                                   |
     * 
     *          Results in:
     * 
     *                         +-------------------+
     *                         |                   |
     *                         |       0 0 0       |
     *                         |                   |
     *                         +-------------------+ <-- With the modified child
     *                                   ^
     *                                   |
     * 
     *  2. This instance represents a series of only one unary instance and it
     *     must be split
     *     This will return a binary choice, with one child the same as my child,
     *     and another (possibly nil child) representing a new chain to the end of
     *     the hash
     * 
     *          For example, attempting to insert the value "1" in this tree:
     * 
     *                         +-------------------+
     *                         |                   |
     *                         |         0         |
     *                         |                   |
     *                         +-------------------+
     * 
     *          Results in:
     * 
     *                         +-------------------+
     *                         |         |         |
     *                         |    0    |    1    |
     *                         |         |         |
     *                         +-------------------+
     * 
     *  3. This instance must be split on the first bit
     *     This will return a binary choice, with one child equal to this instance
     *     with decidedPrefix increased by one, and another representing a new
     *     chain to the end of the hash
     * 
     *          For example, attempting to insert the value "10" in this tree:
     * 
     *                         +-------------------+
     *                         |                   |
     *                         |        0 0        |
     *                         |                   |
     *                         +-------------------+
     * 
     *          Results in:
     * 
     *                         +-------------------+
     *                         |         |         |
     *                         |    0    |    1    |
     *                         |         |         |
     *                         +-------------------+
     *                              ^         ^
     *                             /           \
     *              +-------------------+ +-------------------+
     *              |                   | |                   |
     *              |         0         | |         0         |
     *              |                   | |                   |
     *              +-------------------+ +-------------------+
     * 
     *  4. This instance must be split on the last bit
     *     This will modify this unary choice. The commonPrefix is decreased by
     *     one. The child is set to a binary instance that has a child equal to
     *     the current child and another child equal to a new unary instance to
     *     the end of the hash
     * 
     *          For example, attempting to insert the value "01" in this tree:
     * 
     *                         +-------------------+
     *                         |                   |
     *                         |        0 0        |
     *                         |                   |
     *                         +-------------------+
     * 
     *          Results in:
     * 
     *                         +-------------------+
     *                         |                   |
     *                         |         0         |
     *                         |                   |
     *                         +-------------------+
     *                                   ^
     *                                   |
     *                         +-------------------+
     *                         |         |         |
     *                         |    0    |    1    |
     *                         |         |         |
     *                         +-------------------+
     * 
     *  5. This instance must be split on an interior bit
     *     This will modify this unary choice. The commonPrefix is set to the
     *     interior bit. The child is set to a binary instance that has a child
     *     equal to this unary choice with the decidedPrefix equal to the interior
     *     bit and another child equal to a new unary instance to the end of the
     *     hash
     * 
     *          For example, attempting to insert the value "010" in this tree:
     * 
     *                         +-------------------+
     *                         |                   |
     *                         |       0 0 0       |
     *                         |                   |
     *                         +-------------------+
     * 
     *          Results in:
     * 
     *                         +-------------------+
     *                         |                   |
     *                         |         0         |
     *                         |                   |
     *                         +-------------------+
     *                                   ^
     *                                   |
     *                         +-------------------+
     *                         |         |         |
     *                         |    0    |    1    |
     *                         |         |         |
     *                         +-------------------+
     *                              ^         ^
     *                             /           \
     *              +-------------------+ +-------------------+
     *              |                   | |                   |
     *              |         0         | |         0         |
     *              |                   | |                   |
     *              +-------------------+ +-------------------+
     * </pre>
     */

    public Node<?> add(ID newChoice) {
        if (finalized()) {
            return this; // Only happens if the tree is finalized, or it's a leaf node
        }

        Integer index = ID.firstDifferenceSubset(this.decidedPrefix, this.commonPrefix, this.preference, newChoice);
        if (index == null) {
            // If the first difference doesn't exist, then this node shouldn't be
            // split
            if (this.child != null) {
                // Because this node will finalize before any children could
                // finalize, it must be that the newChoice will match my child's
                // prefix
                this.child = this.child.add(newChoice);
            }
            // if this.child is nil, then we are attempting to add the same choice into
            // the tree, which should be a noop
        } else {
            // The difference was found, so this node must be split

            int bit = this.preference.bit(index); // The currently preferred bit

            BinaryNode b = new BinaryNode(tree, index, snowball.extend(tree.parameters().betaRogue, bit), newChoice,
                    shouldReset, preference);

            UnaryNode newChild = new UnaryNode(tree, newChoice, ID.NumBits,
                    new UnarySnowball(this.tree.parameters().betaVirtuous), index + 1);

            if (this.decidedPrefix == this.commonPrefix - 1) {
                // This node was only voting over one bit. (Case 2. from above)
                b.setChild(bit, child);
                if (this.child != null) {
                    b.setChild(1 - bit, newChild);
                }
                return b;
            }
            if (index == this.decidedPrefix) {
                // This node was split on the first bit. (Case 3. from above)
                this.decidedPrefix++;
                b.setChild(bit, this);
                b.setChild(1 - bit, newChild);
                return b;
            }
            if (index == this.commonPrefix - 1) {
                // This node was split on the last bit. (Case 4. from above)
                this.commonPrefix--;
                b.setChild(bit, child);
                if (this.child != null) {
                    b.setChild(1 - bit, newChild);
                }
                this.child = b;
                return this;
            }
            // This node was split on an interior bit. (Case 5. from above)
            int originalDecidedPrefix = this.decidedPrefix;
            this.decidedPrefix = (index + 1);
            b.setChild(bit, this);
            b.setChild(1 - bit, newChild);
            return new UnaryNode(tree, preference, index, snowball.clone(), originalDecidedPrefix, b);
        }
        return this; // Do nothing, the choice was already rejected
    }

    @Override
    public int decidedPrefix() {
        return decidedPrefix;
    }

    public boolean finalized() {
        return snowball.finalized();
    }

    @Override
    public ID preference() {
        return preference;
    }

    @Override
    public Node<?> recordPoll(Bag votes, boolean reset) {
        // This ensures that votes for rejected colors are dropped
        votes = votes.filter(decidedPrefix, commonPrefix, preference);

        // If my parent didn't get enough votes previously, then neither did I
        if (reset) {
            snowball.recordUnsuccessfulPoll();
            shouldReset = true; // Make sure my child is also reset correctly
        }

        // If I got enough votes this time
        if (votes.size() >= tree.parameters().alpha) {
            snowball.recordSuccessfulPoll();

            if (child != null) {
                // We are guaranteed that commonPrefix will equal
                // child.DecidedPrefix(). Otherwise, there must have been a
                // decision under this node, which isn't possible because
                // beta1 <= beta2. That means that filtering the votes between
                // commonPrefix and child.DecidedPrefix() would always result in
                // the same set being returned.

                // If I'm now decided, return my child
                if (finalized()) {
                    return child.recordPoll(votes, shouldReset);
                }
                child = child.recordPoll(votes, shouldReset);
                // The child's preference may have changed
                preference = child.preference();
            }
            // Now that I have passed my votes to my child, I don't need to reset
            // them
            shouldReset = false;
        } else {
            // I didn't get enough votes, I must reset and my child must reset as
            // well
            snowball.recordUnsuccessfulPoll();
            shouldReset = true;
        }

        return this;
    }
}
