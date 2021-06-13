/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.support;

import com.salesfoce.apollo.consortium.proto.Block;
import com.salesforce.apollo.consortium.CollaboratorContext;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;

public class HashedBlock {
    public final Block  block;
    public final Digest hash;

    public HashedBlock(DigestAlgorithm digestAlgorithm, Block block) {
        this(digestAlgorithm.digest(block.toByteString()), block);
    }

    public HashedBlock(Digest hash, Block block) {
        this.hash = hash;
        this.block = block;
    }

    public long height() {
        return CollaboratorContext.height(block);
    }
}
