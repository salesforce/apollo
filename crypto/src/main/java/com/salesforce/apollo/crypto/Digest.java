/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.crypto;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.ByteString;

/**
 * @author hal.hildebrand
 *
 */
public enum Digest {

    BLAKE2B_256 {

        @Override
        public DigestAlgorithm algorithm() {
            return DigestAlgorithm.BLAKE2B_256;
        }

    },
    BLAKE2B_512 {

        @Override
        public DigestAlgorithm algorithm() {
            return DigestAlgorithm.BLAKE2B_512;
        }

    },
    BLAKE2S_256 {

        @Override
        public DigestAlgorithm algorithm() {
            return DigestAlgorithm.BLAKE2S_256;
        }

    },
    BLAKE3_256 {

        @Override
        public DigestAlgorithm algorithm() {
            return DigestAlgorithm.BLAKE3_256;
        }

        @Override
        public byte[] hashOf(byte[] bytes, int len) {
            var digester = Blake3.newInstance();
            digester.update(bytes);
            return digester.digest(algorithm().digestLength());
        }

        @Override
        public byte[] hashOf(InputStream is) {
            var digester = Blake3.newInstance();
            byte[] buf = new byte[algorithm().digestLength()];
            try {
                for (int read = is.read(buf); read >= 0; read = is.read(buf)) {
                    digester.update(buf, 0, read);
                }
            } catch (IOException e) {
                throw new IllegalStateException("Error reading from buffers, cannot generate hash", e);
            }
            return digester.digest(algorithm().digestLength());
        }
    },
    BLAKE3_512 {

        @Override
        public DigestAlgorithm algorithm() {
            return DigestAlgorithm.BLAKE3_512;
        }

        @Override
        public byte[] hashOf(byte[] bytes, int len) {
            var digester = Blake3.newInstance();
            digester.update(bytes);
            return digester.digest(algorithm().digestLength());
        }

        @Override
        public byte[] hashOf(InputStream is) {
            var digester = Blake3.newInstance();
            byte[] buf = new byte[algorithm().digestLength()];
            try {
                for (int read = is.read(buf); read >= 0; read = is.read(buf)) {
                    digester.update(buf, 0, read);
                }
            } catch (IOException e) {
                throw new IllegalStateException("Error reading from buffers, cannot generate hash", e);
            }
            return digester.digest(algorithm().digestLength());
        }
    },
    NONE {
        @Override
        public DigestAlgorithm algorithm() {
            return DigestAlgorithm.NONE;
        }

        public byte[] hashOf(byte[] bytes, int len) {
            return EMPTY;
        }

        public byte[] hashOf(InputStream is) {
            return EMPTY;
        }
    },
    SHA2_256 {

        @Override
        public DigestAlgorithm algorithm() {
            return DigestAlgorithm.SHA2_256;
        }

    },

    SHA2_512 {

        @Override
        public DigestAlgorithm algorithm() {
            return DigestAlgorithm.SHA2_512;
        }

    },

    SHA3_256 {

        @Override
        public DigestAlgorithm algorithm() {
            return DigestAlgorithm.BLAKE2B_256;
        }

    },
    SHA3_512 {

        @Override
        public DigestAlgorithm algorithm() {
            return DigestAlgorithm.BLAKE2B_256;
        }

    };

    private static abstract class DigestCache<T> {
        private final Map<DigestAlgorithm, T> cache = new HashMap<>();

        public T lookup(DigestAlgorithm da) {
            return cache.computeIfAbsent(da, k -> create(k));
        }

        abstract protected T create(DigestAlgorithm da);
    }

    private static final byte[] EMPTY = new byte[0];

    private static final ThreadLocal<DigestCache<MessageDigest>> MESSAGE_DIGEST = ThreadLocal.withInitial(() -> new DigestCache<MessageDigest>() {
        protected MessageDigest create(DigestAlgorithm da) {
            try {
                return MessageDigest.getInstance(da.algorithmName());
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("Unable to retrieve " + da.algorithmName() + " Message Digest instance",
                        e);
            }
        }
    });

    abstract public DigestAlgorithm algorithm();

    public byte[] hashOf(byte[] bytes, int len) {
        MessageDigest md = lookupJCA();
        md.reset();
        md.update(bytes, 0, len);
        return md.digest();
    }

    public byte[] hashOf(ByteBuffer... buffers) {
        InputStream is = BbBackedInputStream.aggregate(buffers);
        return hashOf(is);
    }

    public byte[] hashOf(ByteString... byteString) {
        InputStream is = BbBackedInputStream.aggregate(byteString);
        return hashOf(is);
    }

    public byte[] hashOf(InputStream is) {
        MessageDigest md = lookupJCA();
        md.reset();
        byte[] buf = new byte[md.getDigestLength()];
        try {
            for (int read = is.read(buf); read >= 0; read = is.read(buf)) {
                md.update(buf, 0, read);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Error reading from buffers, cannot generate hash", e);
        }
        return md.digest();
    }

    private MessageDigest lookupJCA() {
        return MESSAGE_DIGEST.get().lookup(algorithm());
    }

}
