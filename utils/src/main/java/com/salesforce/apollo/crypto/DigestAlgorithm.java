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
 */
public enum DigestAlgorithm {

    BLAKE2B_256 {
        @Override
        public byte digestCode() {
            return 1;
        }

        @Override
        public int digestLength() {
            return 32;
        }

    },
    BLAKE2B_512 {
        @Override
        public byte digestCode() {
            return 2;
        }

        @Override
        public int digestLength() {
            return 64;
        }
    },
    BLAKE2S_256 {
        @Override
        public byte digestCode() {
            return 3;
        }

        @Override
        public int digestLength() {
            return 32;
        }

    },
    BLAKE3_256 {
        @Override
        public byte digestCode() {
            return 4;
        }

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
        public byte digestCode() {
            return 5;
        }

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
        public byte digestCode() {
            return 0;
        }

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

        @Override
        public byte[] hashOf(byte[] bytes, int len) {
            return EMPTY;
        }

        @Override
        public byte[] hashOf(InputStream is) {
            return EMPTY;
        }
    },
    SHA2_256 {
        @Override
        public byte digestCode() {
            return 6;
        }

        @Override
        public int digestLength() {
            return 32;
        }

    },

    SHA2_512 {
        @Override
        public byte digestCode() {
            return 7;
        }

        @Override
        public int digestLength() {
            return 64;
        }

    },

    SHA3_256 {
        @Override
        public byte digestCode() {
            return 8;
        }

        @Override
        public int digestLength() {
            return 32;
        }

    },
    SHA3_512 {
        @Override
        public byte digestCode() {
            return 9;
        }

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

    public static DigestAlgorithm fromDigestCode(byte code) {
        return switch (code) {
        case 0:
            yield NONE;
        case 1:
            yield BLAKE2B_256;
        case 2:
            yield BLAKE2B_512;
        case 3:
            yield BLAKE2S_256;
        case 4:
            yield BLAKE3_256;
        case 5:
            yield BLAKE3_512;
        case 6:
            yield SHA2_256;
        case 7:
            yield SHA2_512;
        case 8:
            yield SHA3_256;
        case 9:
            yield SHA3_512;
        default:
            throw new IllegalArgumentException("Unknown digest code: " + code);
        };
    }

    public String algorithmName() {
        return name();
    }

    public Digest digest(byte[]... bytes) {
        return new Digest(this, hashOf(bytes));
    }

    public Digest digest(byte[] buff, int length) {
        return new Digest(this, hashOf(buff, length));
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

    public Digest digest(String key) {
        return digest(key.getBytes());
    }

    abstract public byte digestCode();

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

    public int longLength() {
        return digestLength() / 8;
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
