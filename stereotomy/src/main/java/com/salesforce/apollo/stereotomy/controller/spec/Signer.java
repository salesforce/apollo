/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.controller.spec;

import static java.util.Objects.requireNonNull;

import java.security.PrivateKey;

import com.salesforce.apollo.stereotomy.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.crypto.SignatureAlgorithm;

/**
 * @author hal.hildebrand
 *
 */
public class Signer {
    private final SignatureAlgorithm algorithm;
    private final int                keyIndex;
    private final PrivateKey         privateKey;

    public Signer(int keyIndex, PrivateKey privateKey) {
        this.keyIndex = keyIndex;
        this.privateKey = requireNonNull(privateKey);
        algorithm = SignatureAlgorithm.lookup(privateKey);
    }

    public SignatureAlgorithm algorithm() {
        return algorithm;
    }

    public int keyIndex() {
        return keyIndex;
    }

    public JohnHancock sign(byte[] bytes) {
        return algorithm.sign(bytes, privateKey);
    }

}
