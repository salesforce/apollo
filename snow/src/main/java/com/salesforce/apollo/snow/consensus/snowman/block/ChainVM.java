/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowman.block;

import com.salesforce.apollo.snow.consensus.snowman.Block;
import com.salesforce.apollo.snow.engine.common.VM;
import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 *
 */

//ChainVM defines the required functionality of a Snowman VM.
//
//A Snowman VM is responsible for defining the representation of state,
//the representation of operations on that state, the application of operations
//on that state, and the creation of the operations. Consensus will decide on
//if the operation is executed and the order operations are executed in.
//
//For example, suppose we have a VM that tracks an increasing number that
//is agreed upon by the network.
//The state is a single number.
//The operation is setting the number to a new, larger value.
//Applying the operation will save to the database the new value.
//The VM can attempt to issue a new number, of larger value, at any time.
//Consensus will ensure the network agrees on the number at every block height.
public interface ChainVM extends VM {
    // Attempt to create a new block from data contained in the VM.
    //
    // If the VM doesn't want to issue a new block, an error should be
    // returned.
    Block buildBlock();

    // Attempt to create a block from a stream of bytes.
    //
    // The block should be represented by the full byte array, without extra
    // bytes.
    Block parseBlock(byte[] content);

    // Attempt to load a block.
    //
    // If the block does not exist, then an error should be returned.
    Block getBlock(ID id);

    // Notify the VM of the currently preferred block.
    //
    // This should always be a block that has no children known to consensus.
    void setPreference(ID id);

    // LastAccepted returns the ID of the last accepted block.
    //
    // If no blocks have been accepted by consensus yet, it is assumed there is
    // a definitionally accepted block, the Genesis block, that will be
    // returned.
    ID LastAccepted();
}
