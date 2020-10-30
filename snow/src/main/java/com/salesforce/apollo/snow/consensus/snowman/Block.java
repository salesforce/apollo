/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https:opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowman;

import com.salesforce.apollo.snow.choices.Decidable;

/**
 * @author hal.hildebrand
 *
 */

/**
 * Block is a possible decision that dictates the next canonical block.
 * 
 * Blocks are guaranteed to be Verified, Accepted, and Rejected in topological
 * order. Specifically, if Verify is called, then the parent has already been
 * verified. If Accept is called, then the parent has already been accepted. If
 * Reject is called, the parent has already been accepted or rejected.
 * 
 * If the status of the block is Unknown, ID is assumed to be able to be called.
 * If the status of the block is Accepted or Rejected; Parent, Verify, Accept,
 * and Reject will never be called.
 */
public interface Block extends Decidable {
    /**
     * Parent returns the block that this block points to.
     * 
     * If the parent block is not known, a Block should be returned with the status
     * Unknown.
     */
    Block parent();

    /**
     * Verify that the state transition this block would make if accepted is valid.
     * If the state transition is invalid, IllegalStateException should be thrown.
     * 
     * It is guaranteed that the Parent has been successfully verified.
     **/
    void verify() throws IllegalStateException;

    /**
     * Bytes returns the binary representation of this block.
     * 
     * This is used for sending blocks to peers. The bytes should be able to be
     * parsed into the same block on another node.
     */
    byte[] bytes();

    void addChild(Block blk);
}
