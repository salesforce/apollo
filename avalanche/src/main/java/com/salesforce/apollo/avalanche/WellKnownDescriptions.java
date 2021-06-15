/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;

/**
 * @author hhildebrand
 */
public enum WellKnownDescriptions {
    BYTE_CONTENT {
        @Override
        public Digest toHash() {
            return UNCONSTRAINED_HASH;
        }
    },
    GENESIS {

        @Override
        public Digest toHash() {
            return GENESIS_HASH;
        }
    };

    private final static Digest GENESIS_HASH = new Digest(DigestAlgorithm.DEFAULT, new byte[32]);
    private final static Digest UNCONSTRAINED_HASH;

    static {
        byte[] bytes = new byte[32];
        bytes[31] = 1;
        UNCONSTRAINED_HASH = new Digest(DigestAlgorithm.DEFAULT, bytes);
    }

    abstract public Digest toHash();
}
