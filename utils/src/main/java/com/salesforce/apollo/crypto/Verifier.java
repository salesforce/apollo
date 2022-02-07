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
import java.util.Arrays;
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
        private final PublicKey[] keys;

        public DefaultVerifier(List<PublicKey> keys) {
            this((PublicKey[]) keys.toArray(new PublicKey[keys.size()]));
        }

        public DefaultVerifier(PublicKey key) {
            this(new PublicKey[] { key });
        }

        public DefaultVerifier(PublicKey[] keys) {
            this.keys = keys;
        }

        @Override
        public Filtered filtered(SigningThreshold threshold, JohnHancock signature, InputStream message) {
            return signature.filter(threshold, keys, message);
        }

        @Override
        public String toString() {
            return "V[" + Arrays.asList(keys).stream().map(k -> ":" + k.getEncoded()).toList() + "]";
        }

        @Override
        public boolean verify(JohnHancock signature, InputStream message) {
            return verify(SigningThreshold.unweighted(keys.length), signature, message);
        }

        @Override
        public boolean verify(SigningThreshold threshold, JohnHancock signature, InputStream message) {
            return signature.verify(threshold, keys, message);
        }
    }

    class MockVerifier implements Verifier {

        @Override
        public Filtered filtered(SigningThreshold threshold, JohnHancock signature, InputStream message) {
            return new Filtered(true, signature);
        }

        @Override
        public boolean verify(JohnHancock signature, InputStream message) {
            return true;
        }

        @Override
        public boolean verify(SigningThreshold threshold, JohnHancock signature, InputStream message) {
            return true;
        }

    }

    record Filtered(boolean verified, JohnHancock filtered) {}

    default Filtered filtered(SigningThreshold threshold, JohnHancock signature, byte[]... message) {
        return filtered(threshold, signature, BbBackedInputStream.aggregate(message));
    }

    default Filtered filtered(SigningThreshold threshold, JohnHancock signature, ByteBuffer... message) {
        return filtered(threshold, signature, BbBackedInputStream.aggregate(message));
    }

    default Filtered filtered(SigningThreshold threshold, JohnHancock signature, ByteString... message) {
        return filtered(threshold, signature, BbBackedInputStream.aggregate(message));
    }

    Filtered filtered(SigningThreshold threshold, JohnHancock signature, InputStream message);

    default Filtered filtered(SigningThreshold threshold, JohnHancock signature, List<ByteBuffer> forSigning) {
        return filtered(threshold, signature, BbBackedInputStream.aggregate(forSigning));
    }

    default Filtered filtered(SigningThreshold threshold, JohnHancock signature, String message) {
        return filtered(threshold, signature, BbBackedInputStream.aggregate(message.getBytes()));
    }

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

    default boolean verify(JohnHancock signature, String message) {
        return verify(signature, BbBackedInputStream.aggregate(message.getBytes()));
    }

    default boolean verify(SigningThreshold threshold, JohnHancock signature, byte[]... message) {
        return verify(threshold, signature, BbBackedInputStream.aggregate(message));
    }

    default boolean verify(SigningThreshold threshold, JohnHancock signature, ByteBuffer... message) {
        return verify(threshold, signature, BbBackedInputStream.aggregate(message));
    }

    default boolean verify(SigningThreshold threshold, JohnHancock signature, ByteString... message) {
        return verify(threshold, signature, BbBackedInputStream.aggregate(message));
    }

    boolean verify(SigningThreshold threshold, JohnHancock signature, InputStream message);

    default boolean verify(SigningThreshold threshold, JohnHancock signature, List<ByteBuffer> forSigning) {
        return verify(threshold, signature, BbBackedInputStream.aggregate(forSigning));
    }

    default boolean verify(SigningThreshold threshold, JohnHancock signature, String message) {
        return verify(threshold, signature, BbBackedInputStream.aggregate(message.getBytes()));
    }
}
