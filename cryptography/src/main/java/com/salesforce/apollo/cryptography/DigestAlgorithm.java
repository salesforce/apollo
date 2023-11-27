/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.cryptography;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.utils.BbBackedInputStream;
import com.salesforce.apollo.utils.Entropy;
import org.bouncycastle.crypto.digests.Blake2bDigest;
import org.bouncycastle.crypto.digests.Blake2sDigest;
import org.bouncycastle.crypto.digests.Blake3Digest;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

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

        @Override
        public byte[] hashOf(byte[] bytes, int len) {
            final int dl = digestLength();
            Blake2bDigest digester = new Blake2bDigest(dl * 8);
            digester.update(bytes, 0, len);
            byte[] digest = new byte[dl];
            digester.doFinal(digest, 0);
            return digest;
        }

        @Override
        public byte[] hashOf(InputStream is) {
            final int dl = digestLength();
            Blake2bDigest digester = new Blake2bDigest(dl * 8);
            byte[] digest = new byte[dl];
            byte[] buf = new byte[dl];
            try {
                for (int read = is.read(buf); read >= 0; read = is.read(buf)) {
                    digester.update(buf, 0, read);
                }
            } catch (IOException e) {
                throw new IllegalStateException("Error reading from buffers, cannot generate hash", e);
            }
            digester.doFinal(digest, 0);
            return digest;
        }

    }, BLAKE2B_512 {
        @Override
        public byte digestCode() {
            return 2;
        }

        @Override
        public int digestLength() {
            return 64;
        }

        @Override
        public byte[] hashOf(byte[] bytes, int len) {
            final int dl = digestLength();
            Blake2bDigest digester = new Blake2bDigest(dl * 8);
            digester.update(bytes, 0, len);
            byte[] digest = new byte[dl];
            digester.doFinal(digest, 0);
            return digest;
        }

        @Override
        public byte[] hashOf(InputStream is) {
            final int dl = digestLength();
            Blake2bDigest digester = new Blake2bDigest(dl * 8);
            byte[] digest = new byte[dl];
            byte[] buf = new byte[dl];
            try {
                for (int read = is.read(buf); read >= 0; read = is.read(buf)) {
                    digester.update(buf, 0, read);
                }
            } catch (IOException e) {
                throw new IllegalStateException("Error reading from buffers, cannot generate hash", e);
            }
            digester.doFinal(digest, 0);
            return digest;
        }
    }, BLAKE2S_256 {
        @Override
        public byte digestCode() {
            return 3;
        }

        @Override
        public int digestLength() {
            return 32;
        }

        @Override
        public byte[] hashOf(byte[] bytes, int len) {
            final int dl = digestLength();
            Blake2sDigest digester = new Blake2sDigest(dl * 8);
            digester.update(bytes, 0, len);
            byte[] digest = new byte[dl];
            digester.doFinal(digest, 0);
            return digest;
        }

        @Override
        public byte[] hashOf(InputStream is) {
            final int dl = digestLength();
            Blake2sDigest digester = new Blake2sDigest(dl * 8);
            byte[] digest = new byte[dl];
            byte[] buf = new byte[dl];
            try {
                for (int read = is.read(buf); read >= 0; read = is.read(buf)) {
                    digester.update(buf, 0, read);
                }
            } catch (IOException e) {
                throw new IllegalStateException("Error reading from buffers, cannot generate hash", e);
            }
            digester.doFinal(digest, 0);
            return digest;
        }

    }, BLAKE3_256 {
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
            final int dl = digestLength();
            Blake3Digest digester = new Blake3Digest(dl);
            digester.update(bytes, 0, len);
            byte[] digest = new byte[dl];
            digester.doFinal(digest, 0);
            return digest;
        }

        @Override
        public byte[] hashOf(InputStream is) {
            final int dl = digestLength();
            Blake3Digest digester = new Blake3Digest(dl);
            byte[] digest = new byte[dl];
            byte[] buf = new byte[dl];
            try {
                for (int read = is.read(buf); read >= 0; read = is.read(buf)) {
                    digester.update(buf, 0, read);
                }
            } catch (IOException e) {
                throw new IllegalStateException("Error reading from buffers, cannot generate hash", e);
            }
            digester.doFinal(digest, 0);
            return digest;
        }
    }, BLAKE3_512 {
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
            final int dl = digestLength();
            Blake3Digest digester = new Blake3Digest(dl * 8);
            digester.update(bytes, 0, len);
            var digest = new byte[dl];
            digester.doFinal(digest, 0);
            return digest;
        }

        @Override
        public byte[] hashOf(InputStream is) {
            final int dl = digestLength();
            Blake3Digest digester = new Blake3Digest(dl * 8);
            byte[] buf = new byte[dl];
            try {
                for (int read = is.read(buf); read >= 0; read = is.read(buf)) {
                    digester.update(buf, 0, read);
                }
            } catch (IOException e) {
                throw new IllegalStateException("Error reading from buffers, cannot generate hash", e);
            }
            var digest = new byte[dl];
            digester.doFinal(digest, 0);
            return digest;
        }
    }, NONE {
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
    }, SHA2_256 {
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

    }, SHA3_512 {
        @Override
        public byte digestCode() {
            return 9;
        }

        @Override
        public int digestLength() {
            return 32;
        }

    };

    public static final  DigestAlgorithm          DEFAULT           = BLAKE2B_256;
    public static final  long                     MAX_UNSIGNED_LONG = -1L;
    private static final byte[]                   EMPTY             = new byte[0];
    private static final long[]                   LAST_32           = new long[4];
    private static final long[]                   LAST_64           = new long[8];
    private static final ThreadLocal<DigestCache> MESSAGE_DIGEST    = ThreadLocal.withInitial(() -> new DigestCache());
    private static final long[]                   ORIGIN_32         = new long[4];
    private static final long[]                   ORIGIN_64         = new long[8];

    static {
        Arrays.fill(LAST_32, MAX_UNSIGNED_LONG);
        Arrays.fill(LAST_64, MAX_UNSIGNED_LONG);
    }

    public static DigestAlgorithm fromDigestCode(int i) {
        return switch (i) {
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
                throw new IllegalArgumentException("Unknown digest code: " + i);
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

    public Digest random() {
        var hash = new long[longLength()];
        for (int i = 0; i < hash.length; i++) {
            hash[i] = Entropy.nextSecureLong();
        }
        return new Digest(digestCode(), hash);
    }

    public Digest random(Random random) {
        var hash = new long[longLength()];
        for (int i = 0; i < hash.length; i++) {
            hash[i] = random.nextLong();
        }
        return new Digest(digestCode(), hash);
    }

    public UUID toUUID(long[] hash) {
        assert hash != null;
        return switch (hash.length) {
            case 4 -> new UUID(hash[0] ^ hash[2], hash[1] ^ hash[3]);
            case 8 -> toUUID(new long[] { hash[0] ^ hash[2], hash[1] ^ hash[3], hash[4] ^ hash[6], hash[5] ^ hash[7] });
            default -> throw new IllegalArgumentException("invalid hash array size: " + hash.length);
        };
    }

    protected MessageDigest createJCA() {
        try {
            return MessageDigest.getInstance(algorithmName());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(
            "Unable to retrieve " + algorithmName() + " Message DigestAlgorithm instance", e);
        }
    }

    private MessageDigest lookupJCA() {
        return MESSAGE_DIGEST.get().lookup(this);
    }

    private static class DigestCache {
        private final Map<DigestAlgorithm, MessageDigest> cache = new HashMap<>();

        public MessageDigest lookup(DigestAlgorithm da) {
            return cache.computeIfAbsent(da, k -> k.createJCA());
        }
    }
}
