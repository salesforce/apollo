/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.support;

import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesforce.apollo.consortium.CollaboratorContext;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;

/**
 * @author hal.hildebrand
 *
 */
public class HashedCertifiedBlock {
    public final CertifiedBlock block;
    public final Digest         hash;

    public HashedCertifiedBlock(DigestAlgorithm digestAlgorithm, CertifiedBlock block) {
        this(digestAlgorithm.digest(block.getBlock().toByteString()), block);
    }

    private HashedCertifiedBlock(Digest hash, CertifiedBlock block) {
        this.hash = hash;
        this.block = block;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof HashedCertifiedBlock) {
            return hash.equals(((HashedCertifiedBlock) obj).hash);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return hash.hashCode();
    }

    public long height() {
        return CollaboratorContext.height(block);
    }

    public String toString() {
        return "cb[" + hash.toString() + "]";
    }
}
