/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.cryptography;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.utils.BbBackedInputStream;
import org.joou.ULong;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.PrivateKey;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Produces signatures using a given key
 *
 * @author hal.hildebrand
 */
public interface Signer {

    SignatureAlgorithm algorithm();

    default JohnHancock sign(byte[]... bytes) {
        return sign(BbBackedInputStream.aggregate(bytes));
    }

    default JohnHancock sign(ByteBuffer... buffs) {
        return sign(BbBackedInputStream.aggregate(buffs));
    }

    default JohnHancock sign(ByteString... message) {
        return sign(BbBackedInputStream.aggregate(message));
    }

    JohnHancock sign(InputStream message);

    default JohnHancock sign(List<ByteBuffer> buffers) {
        return sign(BbBackedInputStream.aggregate(buffers));
    }

    default JohnHancock sign(String msg) {
        return sign(BbBackedInputStream.aggregate(msg.getBytes()));
    }

    class MockSigner implements Signer {
        private final SignatureAlgorithm algorithm;
        private final ULong              sequenceNumber;

        public MockSigner(SignatureAlgorithm algorithm, ULong sequenceNumber) {
            this.algorithm = algorithm;
            this.sequenceNumber = sequenceNumber;
        }

        @Override
        public SignatureAlgorithm algorithm() {
            return algorithm;
        }

        @Override
        public JohnHancock sign(byte[]... bytes) {
            return new JohnHancock(algorithm(), new byte[algorithm().signatureLength()], sequenceNumber);
        }

        @Override
        public JohnHancock sign(ByteBuffer... buffs) {
            return new JohnHancock(algorithm(), new byte[algorithm().signatureLength()], sequenceNumber);
        }

        @Override
        public JohnHancock sign(ByteString... message) {
            return new JohnHancock(algorithm(), new byte[algorithm().signatureLength()], sequenceNumber);
        }

        @Override
        public JohnHancock sign(InputStream message) {
            return new JohnHancock(algorithm(), new byte[algorithm().signatureLength()], sequenceNumber);
        }

        @Override
        public JohnHancock sign(List<ByteBuffer> buffers) {
            return new JohnHancock(algorithm(), new byte[algorithm().signatureLength()], sequenceNumber);
        }
    }

    class SignerImpl implements Signer {
        private final SignatureAlgorithm algorithm;
        private final PrivateKey[]       privateKeys;
        private final ULong              sequenceNumber;

        public SignerImpl(List<PrivateKey> privateKeys, ULong sequenceNumber) {
            this(privateKeys.toArray(new PrivateKey[privateKeys.size()]), sequenceNumber);
        }

        public SignerImpl(PrivateKey privateKey, ULong sequenceNumber) {
            this(new PrivateKey[] { privateKey }, sequenceNumber);
        }

        public SignerImpl(PrivateKey[] privateKeys, ULong sequenceNumber) {
            assert privateKeys.length > 0;
            this.privateKeys = requireNonNull(privateKeys);
            algorithm = SignatureAlgorithm.lookup(privateKeys[0]);
            this.sequenceNumber = sequenceNumber;
        }

        @Override
        public SignatureAlgorithm algorithm() {
            return algorithm;
        }

        @Override
        public JohnHancock sign(InputStream message) {
            return algorithm.sign(sequenceNumber, privateKeys, message);
        }
    }

}
