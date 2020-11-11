/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import com.salesfoce.apollo.consortium.proto.Block;
import com.salesforce.apollo.protocols.HashKey;

public class CurrentBlock {
    private final Block   block;
    private final HashKey hash;

    CurrentBlock(HashKey hash, Block block) {
        this.hash = hash;
        this.block = block;
    }

    public Block getBlock() {
        return block;
    }

    public HashKey getHash() {
        return hash;
    }
}
