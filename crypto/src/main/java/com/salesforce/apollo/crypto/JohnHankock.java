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
public class JohnHankock {
    final byte[]                      bytes;
    private final SignatureAlgorithms algorithm;

    public JohnHankock(SignatureAlgorithms algorithm, byte[] bytes) {
        this.algorithm = algorithm;
        this.bytes = bytes;
    }

    public SignatureAlgorithms getAlgorithm() {
        return algorithm;
    }
}
