/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.support;

import com.salesforce.apollo.choam.proto.CertifiedBlock;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import org.joou.ULong;

/**
 * @author hal.hildebrand
 */
public class HashedCertifiedBlock extends HashedBlock {
    public final CertifiedBlock certifiedBlock;

    public HashedCertifiedBlock(DigestAlgorithm digestAlgorithm, CertifiedBlock block) {
        this(digestAlgorithm.digest(block.getBlock().toByteString()), block);
    }

    HashedCertifiedBlock(Digest hash) {
        super(hash);
        this.certifiedBlock = null;
    }

    private HashedCertifiedBlock(Digest hash, CertifiedBlock block) {
        super(hash, block.getBlock());
        this.certifiedBlock = block;
    }

    @Override
    public String toString() {
        return "cb" + hash.toString() + " height: " + height();
    }

    public static class NullBlock extends HashedCertifiedBlock {

        public NullBlock(DigestAlgorithm algo) {
            super(algo.getOrigin());
        }

        @Override
        public int compareTo(HashedBlock o) {
            if (this == o) {
                return 0;
            }
            return -1;
        }

        @Override
        public ULong height() {
            return null;
        }
    }
}
