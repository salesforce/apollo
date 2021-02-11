/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.support;

import com.salesfoce.apollo.consortium.proto.Block;
import com.salesforce.apollo.consortium.CollaboratorContext;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;

public class HashedBlock {
    public final Block   block;
    public final HashKey hash;

    public HashedBlock(Block block) {
        this(new HashKey(Conversion.hashOf(block.toByteString())), block);
    }

    public HashedBlock(HashKey hash, Block block) {
        this.hash = hash;
        this.block = block;
    }

    public long height() {
        return CollaboratorContext.height(block);
    }
}
