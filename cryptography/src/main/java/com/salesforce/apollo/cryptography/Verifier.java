/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.cryptography;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.utils.BbBackedInputStream;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Verifies a signature using a given key
 *
 * @author hal.hildebrand
 */
public interface Verifier {
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

    class DefaultVerifier implements Verifier {
        private final Map<Integer, PublicKey> keys;

        public DefaultVerifier(List<PublicKey> keys) {
            this(mapped(keys));
        }

        public DefaultVerifier(Map<Integer, PublicKey> keys) {
            this.keys = keys;
        }

        public DefaultVerifier(PublicKey key) {
            this(mapped(new PublicKey[] { key }));
        }

        public DefaultVerifier(PublicKey[] keys) {
            this(mapped(keys));
        }

        public static Map<Integer, PublicKey> mapped(List<PublicKey> list) {
            var mapped = new HashMap<Integer, PublicKey>();
            for (int i = 0; i < list.size(); i++) {
                mapped.put(i, list.get(i));
            }
            return mapped;
        }

        public static Map<Integer, PublicKey> mapped(PublicKey[] array) {
            var mapped = new HashMap<Integer, PublicKey>();
            for (int i = 0; i < array.length; i++) {
                mapped.put(i, array[i]);
            }
            return mapped;
        }

        @Override
        public Filtered filtered(SigningThreshold threshold, JohnHancock signature, InputStream message) {
            return signature.filter(threshold, keys, message);
        }

        @Override
        public String toString() {
            return "V[" + keys.values().stream().map(k -> ":" + DigestAlgorithm.DEFAULT.digest(k.getEncoded())).toList()
            + "]";
        }

        @Override
        public boolean verify(JohnHancock signature, InputStream message) {
            return verify(SigningThreshold.unweighted(keys.size()), signature, message);
        }

        @Override
        public boolean verify(SigningThreshold threshold, JohnHancock signature, InputStream message) {
            return signature.verify(threshold, keys, message);
        }
    }

    class MockVerifier implements Verifier {

        @Override
        public Filtered filtered(SigningThreshold threshold, JohnHancock signature, InputStream message) {
            return new Filtered(true, signature.signatureCount(), signature);
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

    record Filtered(boolean verified, int validating, JohnHancock filtered) {
    }
}
