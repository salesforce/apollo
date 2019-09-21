/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche;

import com.salesforce.apollo.avro.HASH;

/**
 * @author hhildebrand
 */
public enum WellKnownDescriptions {
    BYTE_CONTENT {
        @Override
        public HASH toHash() {
            return UNCONSTRAINED_HASH;
        }
    },
    GENESIS {

        @Override
        public HASH toHash() {
            return GENESIS_HASH;
        }
    };
    private final static HASH GENESIS_HASH = new HASH(new byte[32]);
    private final static HASH UNCONSTRAINED_HASH;

    static {
        byte[] bytes = new byte[32];
        bytes[31] = 1;
        UNCONSTRAINED_HASH = new HASH(bytes);
    }

    abstract public HASH toHash();
}
