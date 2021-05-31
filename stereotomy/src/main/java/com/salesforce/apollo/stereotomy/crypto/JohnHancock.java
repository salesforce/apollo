/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.crypto;

import java.util.Arrays;
import java.util.Objects;

/**
 * A signature
 * 
 * @author hal.hildebrand
 *
 */
public class JohnHancock {
    final byte[]                     bytes;
    private final SignatureAlgorithm algorithm;

    public JohnHancock(SignatureAlgorithm algorithm, byte[] bytes) {
        this.algorithm = algorithm;
        this.bytes = bytes;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof JohnHancock)) {
            return false;
        }
        JohnHancock other = (JohnHancock) obj;
        return algorithm == other.algorithm && Arrays.equals(bytes, other.bytes);
    }

    public SignatureAlgorithm getAlgorithm() {
        return algorithm;
    }

    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(bytes);
        result = prime * result + Objects.hash(algorithm);
        return result;
    }
}
