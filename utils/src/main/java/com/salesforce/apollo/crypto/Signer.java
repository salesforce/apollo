/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.crypto;

import static java.util.Objects.requireNonNull;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.PrivateKey;

import com.google.protobuf.ByteString;

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

    public JohnHancock sign(ByteBuffer... buffs) {
        return algorithm.sign(privateKey, buffs);
    }

    public JohnHancock sign(byte[]... bytes) {
        return algorithm.sign(privateKey, bytes);
    }

    public JohnHancock sign(ByteString... message) {
        return algorithm.sign(privateKey, message);
    }

    public JohnHancock sign(InputStream message) {
        return algorithm.sign(privateKey, message);
    }

}
