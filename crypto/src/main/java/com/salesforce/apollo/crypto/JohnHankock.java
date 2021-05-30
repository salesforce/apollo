/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.crypto;

/**
 * A signature
 * 
 * @author hal.hildebrand
 *
 */
public class JohnHankock {
    final byte[]                    bytes;
    public final SignatureAlgorithm algorithm;

    public JohnHankock(SignatureAlgorithm algorithm, byte[] bytes) {
        this.algorithm = algorithm;
        this.bytes = bytes;
    }
}
