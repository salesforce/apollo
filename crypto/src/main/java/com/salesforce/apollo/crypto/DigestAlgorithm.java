/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.crypto;

/**
 * @author hal.hildebrand
 *
 */
public enum DigestAlgorithm {
    BLAKE2B_256 {
        @Override
        public int digestLength() {
            return 32;
        }
    },
    BLAKE2B_512 {

        @Override
        public int digestLength() {
            return 64;
        }

    },
    BLAKE2S_256 {
        @Override
        public int digestLength() {
            return 32;
        }

    },
    BLAKE3_256 {
        @Override
        public int digestLength() {
            return 32;
        }

    },
    BLAKE3_512 {
        @Override
        public int digestLength() {
            return 64;
        }

    },
    NONE {
        @Override
        public int digestLength() {
            return 0;
        }
    },
    SHA2_256 {
        @Override
        public int digestLength() {
            return 32;
        }

    },
    SHA2_512 {
        @Override
        public int digestLength() {
            return 64;
        }

    },
    SHA3_256 {
        @Override
        public int digestLength() {
            return 32;
        }

    },
    SHA3_512 {
        @Override
        public int digestLength() {
            return 64;
        }

    };

    public String algorithmName() {
        return name();
    }

    abstract public int digestLength();

}
