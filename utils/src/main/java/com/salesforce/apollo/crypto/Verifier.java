/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.crypto;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.PublicKey;
import java.util.List;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.utils.BbBackedInputStream;

/**
 * Verifies a signature using a given key
 * 
 * @author hal.hildebrand
 *
 */
public interface Verifier {
    class DefaultVerifier implements Verifier {
        private final SignatureAlgorithm algo;
        private final PublicKey          key;

        public DefaultVerifier(PublicKey key) {
            this(SignatureAlgorithm.lookup(key), key);
        }

        public DefaultVerifier(SignatureAlgorithm algo, PublicKey key) {
            this.algo = algo;
            this.key = key;
        }

        public SignatureAlgorithm getAlgorithm() {
            return algo;
        }

        public PublicKey getKey() {
            return key;
        }

        @Override
        public PublicKey getPublicKey() {
            return key;
        }

        @Override
        public boolean verify(JohnHancock signature, InputStream message) {
            return algo.verify(key, signature, message);
        }

    }

    class MockVerifier implements Verifier {

        @Override
        public PublicKey getPublicKey() {
            return null;
        }

        @Override
        public boolean verify(JohnHancock signature, byte[]... message) {
            return true;
        }

        @Override
        public boolean verify(JohnHancock signature, ByteBuffer... message) {
            return true;
        }

        @Override
        public boolean verify(JohnHancock signature, ByteString... message) {
            return true;
        }

        @Override
        public boolean verify(JohnHancock signature, InputStream message) {
            return true;
        }

        @Override
        public boolean verify(JohnHancock signature, List<ByteBuffer> forSigning) {
            return true;
        }

    }

    PublicKey getPublicKey();

    default boolean verify(JohnHancock signature, byte[]... message) {
        return verify(signature, BbBackedInputStream.aggregate(message));
    }

    default boolean verify(JohnHancock signature, ByteBuffer... message) {
        return verify(signature, BbBackedInputStream.aggregate(message));
    }

    default boolean verify(JohnHancock signature, ByteString... message) {
        return verify(signature, BbBackedInputStream.aggregate(message));
    }

    boolean verify(JohnHancock signature, InputStream message);

    default boolean verify(JohnHancock signature, List<ByteBuffer> forSigning) {
        return verify(signature, BbBackedInputStream.aggregate(forSigning));
    }
}
