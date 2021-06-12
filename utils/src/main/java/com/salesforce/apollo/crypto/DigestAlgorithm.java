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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.utils.BbBackedInputStream;

/**
 * Enumerations of digest algorithms
 * 
 * @author hal.hildebrand
 *
 */
public enum DigestAlgorithm {

    BLAKE2B_256 {
        @Override
        public int digestLength() {
            return 32;
        }

    },
    BLAKE2B_512 {
        @Override
        public int digestLength() {
            return 64;
        }
    },
    BLAKE2S_256 {
        @Override
        public int digestLength() {
            return 32;
        }

    },
    BLAKE3_256 {
        @Override
        public int digestLength() {
            return 32;
        }

        @Override
        public byte[] hashOf(byte[] bytes, int len) {
            var digester = Blake3.newInstance();
            digester.update(bytes);
            return digester.digest(digestLength());
        }

        @Override
        public byte[] hashOf(InputStream is) {
            var digester = Blake3.newInstance();
            byte[] buf = new byte[digestLength()];
            try {
                for (int read = is.read(buf); read >= 0; read = is.read(buf)) {
                    digester.update(buf, 0, read);
                }
            } catch (IOException e) {
                throw new IllegalStateException("Error reading from buffers, cannot generate hash", e);
            }
            return digester.digest(digestLength());
        }
    },
    BLAKE3_512 {
        @Override
        public int digestLength() {
            return 64;
        }

        @Override
        public byte[] hashOf(byte[] bytes, int len) {
            var digester = Blake3.newInstance();
            digester.update(bytes);
            return digester.digest(digestLength());
        }

        @Override
        public byte[] hashOf(InputStream is) {
            var digester = Blake3.newInstance();
            byte[] buf = new byte[digestLength()];
            try {
                for (int read = is.read(buf); read >= 0; read = is.read(buf)) {
                    digester.update(buf, 0, read);
                }
            } catch (IOException e) {
                throw new IllegalStateException("Error reading from buffers, cannot generate hash", e);
            }
            return digester.digest(digestLength());
        }
    },
    NONE {
        @Override
        public int digestLength() {
            return 0;
        }

        @Override
        public Digest getLast() {
            return new Digest(this, EMPTY);
        }

        @Override
        public Digest getOrigin() {
            return new Digest(this, EMPTY);
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
        public int digestLength() {
            return 32;
        }

    },

    SHA2_512 {
        @Override
        public int digestLength() {
            return 64;
        }

    },

    SHA3_256 {
        @Override
        public int digestLength() {
            return 32;
        }

    },
    SHA3_512 {
        @Override
        public int digestLength() {
            return 32;
        }

    };

    private static class DigestCache {
        private final Map<DigestAlgorithm, MessageDigest> cache = new HashMap<>();

        public MessageDigest lookup(DigestAlgorithm da) {
            return cache.computeIfAbsent(da, k -> k.createJCA());
        }
    }

    public static final DigestAlgorithm DEFAULT = BLAKE3_256;

    private static final byte[]                   EMPTY          = new byte[0];
    private static final byte[]                   LAST_32;
    private static final byte[]                   LAST_64;
    private static final ThreadLocal<DigestCache> MESSAGE_DIGEST = ThreadLocal.withInitial(() -> new DigestCache());
    private static final byte[]                   ORIGIN_32      = new byte[32];
    private static final byte[]                   ORIGIN_64      = new byte[64];

    static {
        LAST_32 = new byte[32];
        Arrays.fill(LAST_32, (byte) 255);
        LAST_64 = new byte[64];
        Arrays.fill(LAST_32, (byte) 255);
    }

    public String algorithmName() {
        return name();
    }

    public Digest digest(byte[]... bytes) {
        return new Digest(this, hashOf(bytes));
    }

    public Digest digest(ByteBuffer... buffers) {
        return new Digest(this, hashOf(buffers));
    }

    public Digest digest(ByteString... bytes) {
        return new Digest(this, hashOf(bytes));
    }

    public Digest digest(InputStream buffers) {
        return new Digest(this, hashOf(buffers));
    }

    public Digest digest(List<ByteBuffer> buffers) {
        return digest(BbBackedInputStream.aggregate(buffers));
    }

    abstract public int digestLength();

    public Digest getLast() {
        return new Digest(this, digestLength() == 32 ? LAST_32 : LAST_64);
    }

    public Digest getOrigin() {
        return new Digest(this, digestLength() == 32 ? ORIGIN_32 : ORIGIN_64);
    }

    public byte[] hashOf(byte[]... buffers) {
        InputStream is = BbBackedInputStream.aggregate(buffers);
        return hashOf(is);
    }

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

    public byte[] hashOf(List<ByteBuffer> buffers) {
        InputStream is = BbBackedInputStream.aggregate(buffers);
        return hashOf(is);
    }

    protected MessageDigest createJCA() {
        try {
            return MessageDigest.getInstance(algorithmName(), ProviderUtils.getProviderBC());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(
                    "Unable to retrieve " + algorithmName() + " Message DigestAlgorithm instance", e);
        }
    }

    private MessageDigest lookupJCA() {
        return MESSAGE_DIGEST.get().lookup(this);
    }
}
