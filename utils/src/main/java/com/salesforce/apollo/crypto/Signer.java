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
import java.util.List;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.utils.BbBackedInputStream;

/**
 * @author hal.hildebrand
 *
 */
public interface Signer {

    class MockSigner implements Signer {

        @Override
        public SignatureAlgorithm algorithm() {
            return SignatureAlgorithm.DEFAULT;
        }

        @Override
        public int keyIndex() {
            return 0;
        }

        @Override
        public JohnHancock sign(byte[]... bytes) {
            return new JohnHancock(algorithm(), new byte[0]);
        }

        @Override
        public JohnHancock sign(ByteBuffer... buffs) {
            return new JohnHancock(algorithm(), new byte[0]);
        }

        @Override
        public JohnHancock sign(ByteString... message) {
            return new JohnHancock(algorithm(), new byte[0]);
        }

        @Override
        public JohnHancock sign(InputStream message) {
            return new JohnHancock(algorithm(), new byte[0]);
        }

        @Override
        public JohnHancock sign(List<ByteBuffer> buffers) {
            return new JohnHancock(algorithm(), new byte[0]);
        }
    }

    class SignerImpl implements Signer {
        private final SignatureAlgorithm algorithm;
        private final int                keyIndex;
        private final PrivateKey         privateKey;

        public SignerImpl(int keyIndex, PrivateKey privateKey) {
            this.keyIndex = keyIndex;
            this.privateKey = requireNonNull(privateKey);
            algorithm = SignatureAlgorithm.lookup(privateKey);
        }

        @Override
        public SignatureAlgorithm algorithm() {
            return algorithm;
        }

        @Override
        public int keyIndex() {
            return keyIndex;
        }

        @Override
        public JohnHancock sign(byte[]... bytes) {
            return algorithm.sign(privateKey, bytes);
        }

        @Override
        public JohnHancock sign(ByteBuffer... buffs) {
            return algorithm.sign(privateKey, buffs);
        }

        @Override
        public JohnHancock sign(ByteString... message) {
            return algorithm.sign(privateKey, message);
        }

        @Override
        public JohnHancock sign(InputStream message) {
            return algorithm.sign(privateKey, message);
        }

        @Override
        public JohnHancock sign(List<ByteBuffer> buffers) {
            return algorithm.sign(privateKey, BbBackedInputStream.aggregate(buffers));
        }
    }

    SignatureAlgorithm algorithm();

    int keyIndex();

    JohnHancock sign(byte[]... bytes);

    JohnHancock sign(ByteBuffer... buffs);

    JohnHancock sign(ByteString... message);

    JohnHancock sign(InputStream message);

    JohnHancock sign(List<ByteBuffer> buffers);

}
