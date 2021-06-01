/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.crypto;

import java.util.Arrays;
import java.util.Objects;

/**
 * A computed digest
 * 
 * @author hal.hildebrand
 *
 */
public class Digest {
    public static final Digest NONE = new Digest(DigestAlgorithm.NONE, new byte[0]);

    private final DigestAlgorithm algorithm;
    private final byte[]          bytes;

    public Digest(DigestAlgorithm algorithm, byte[] bytes) {
        this.algorithm = algorithm;
        this.bytes = bytes;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Digest)) {
            return false;
        }
        Digest other = (Digest) obj;
        return algorithm == other.algorithm && Arrays.equals(bytes, other.bytes);
    }

    public DigestAlgorithm getAlgorithm() {
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
