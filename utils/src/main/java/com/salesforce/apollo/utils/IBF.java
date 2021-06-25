/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.function.Function;

import com.salesforce.apollo.crypto.Blake3;
import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
abstract public class IBF<KeyType> implements Cloneable {
    public static class DigestIBF extends IBF<Digest> {
        private final long[] keySum;

        public DigestIBF(int d) {
            super(d);
            keySum = new long[0];
        }

        public DigestIBF(int d, int seed) {
            super(d, seed);
            keySum = new long[0];
        }

        public DigestIBF(int d, int seed, int k) {
            super(d, wrap(seed), k);
            keySum = new long[0];
        }

        @Override
        protected ByteBuffer keyBuffer() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        boolean cellIsNull(int i) {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        IBF<Digest> cloneEmpty() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        IBF<Digest>.Hasher<Digest> hasher(Digest key) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        int keyHash(int cell) {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        int keyHashOf(Digest key) {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        Digest keySum(int i) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        void setCell(int cell, Digest value) {
            // TODO Auto-generated method stub

        }

        @Override
        void xor(int cell, Digest key) {
            // TODO Auto-generated method stub

        }

        @Override
        Digest xorResult(int i, Digest idSum) {
            // TODO Auto-generated method stub
            return null;
        }

    }

    public static class IntIBF extends IBF<Integer> {
        public static int smear(int hashCode) {
            hashCode ^= (hashCode >>> 20) ^ (hashCode >>> 12);
            return hashCode ^ (hashCode >>> 7) ^ (hashCode >>> 4);
        }

        private final ByteBuffer keyBuffer = ByteBuffer.allocate(4);
        private final int[]      keySum;

        public IntIBF(int d) {
            super(d);
            keySum = new int[d];
        }

        public IntIBF(int d, ByteBuffer seed) {
            super(d, seed);
            keySum = new int[d];
        }

        public IntIBF(int d, ByteBuffer seed, int k) {
            super(d, seed, k);
            keySum = new int[d];
        }

        public IntIBF(int d, int seed) {
            super(d, seed);
            keySum = new int[d];
        }

        public IntIBF(int d, int seed, int k) {
            super(d, seed, k);
            keySum = new int[d];
        }

        @Override
        protected ByteBuffer keyBuffer() {
            return keyBuffer;
        }

        @Override
        boolean cellIsNull(int cell) {
            return keySum[cell] == 0;
        }

        @Override
        IBF<Integer> cloneEmpty() {
            return new IntIBF(cells(), seed, k);
        }

        @Override
        Hasher<Integer> hasher(Integer key) {
            return new IntHasher(key);
        }

        @Override
        int keyHash(int cell) {
            return hasher(keySum[cell]).identityHash();
        }

        @Override
        int keyHashOf(Integer key) {
            return hasher(key).identityHash();
        }

        @Override
        Integer keySum(int cell) {
            return keySum[cell];
        }

        @Override
        void setCell(int cell, Integer value) {
            keySum[cell] = value;
        }

        @Override
        void xor(int cell, Integer key) {
            keySum[cell] ^= key;
        }

        @Override
        Integer xorResult(int cell, Integer key) {
            return keySum[cell] ^ key;
        }

    }

    public static class LongIBF extends IBF<Long> {

        public static int smear(long value) {
            return IntIBF.smear((int) (value ^ (value >>> 32)));
        }

        private final ByteBuffer keyBuffer = ByteBuffer.allocate(8);
        private final long[]     keySum;

        public LongIBF(int d) {
            super(d);
            keySum = new long[d];
        }

        public LongIBF(int d, ByteBuffer seed) {
            super(d, seed);
            keySum = new long[d];
        }

        public LongIBF(int d, ByteBuffer seed, int k) {
            super(d, seed, k);
            keySum = new long[d];
        }

        public LongIBF(int d, int seed) {
            super(d, seed);
            keySum = new long[d];
        }

        public LongIBF(int d, int seed, int k) {
            super(d, seed, k);
            keySum = new long[d];
        }

        @Override
        protected ByteBuffer keyBuffer() {
            return keyBuffer;
        }

        @Override
        boolean cellIsNull(int cell) {
            return keySum[cell] == 0;
        }

        @Override
        IBF<Long> cloneEmpty() {
            return new LongIBF(cells(), seed, k);
        }

        @Override
        IBF<Long>.Hasher<Long> hasher(Long key) {
            return new LongHasher(key, seed);
        }

        @Override
        int keyHash(int cell) {
            return hasher(keySum[cell]).identityHash();
        }

        @Override
        int keyHashOf(Long key) {
            return hasher(key).identityHash();
        }

        @Override
        Long keySum(int cell) {
            return keySum[cell];
        }

        @Override
        void setCell(int cell, Long value) {
            keySum[cell] = value;
        }

        @Override
        void xor(int cell, Long key) {
            keySum[cell] ^= key;
        }

        @Override
        Long xorResult(int cell, Long key) {
            return keySum[cell] ^ key;
        }

    }

    abstract class DigesterHasher<M> extends Hasher<M> {

        private final ByteBuffer keyBuffer;

        DigesterHasher(M key) {
            assert key != null;
            keyBuffer = bufferFor(key);
        }

        @Override
        void add(int cells, M key, IBF<M> ibf, int idHash) {
            process(hash -> {
                ibf.add(hash, key, idHash);
            });
        }

        ByteBuffer buffer(int i) {
            ByteBuffer buff = keyBuffer();
            buff.position(0);
            buff.putInt(i);
            buff.flip();
            return buff;
        }

        ByteBuffer buffer(long l) {
            ByteBuffer buff = keyBuffer();
            buff.position(0);
            buff.putLong(l);
            buff.flip();
            return buff;
        }

        ByteBuffer buffer(long[] hash) {
            return null;
        }

        abstract ByteBuffer bufferFor(M key);

        @Override
        boolean contains(int cells, M key, IBF<M> ibf) {
            return process(hash -> {
                if (ibf.count[hash] == 0) {
                    return false;
                }
                return true;
            });

        }

        @Override
        void decode(int cells, M s, int count, IBF<M> ibf, Queue<Integer> pure, int keyHash) {
            process(hash -> {
                ibf.xor(hash, s);
                ibf.hashSum[hash] ^= keyHash;
                ibf.count[hash] -= count;
                if (ibf.isPure(hash)) {
                    pure.add(hash);
                }
            });
        }

        @Override
        int identityHash() {
            var digester = Blake3.newInstance();
            ByteBuffer idSeed = ByteBuffer.allocate(4);
            idSeed.putInt(207);
            digester.update(idSeed.array());
            digester.update(seed.array());

            byte[] digest;
            digester.update(seed.array());
            digester.update(keyBuffer.array());
            digest = digester.digest(32);

            int h = 0;
            for (int j = 0; j < 4; j++) {
                h <<= 8;
                h |= ((int) digest[j]) & 0xFF;
            }
            return h;
        }

        void process(Consumer<Integer> processor) {
            process(hash -> {
                processor.accept(hash);
                return true;
            });
        }

        boolean process(Function<Integer, Boolean> processor) {
            int index = 0;
            byte salt = 0;
            var digester = Blake3.newInstance();
            digester.update(seed.array());
            int cellCount = cells();
            BitSet locations = new BitSet(cellCount);
            while (index < k) {
                byte[] digest;
                digester.update(new byte[] { salt });
                salt++;
                digester.update(keyBuffer.array());
                digest = digester.digest(32);

                for (int i = 0; i < digest.length / 4 && index < k; i++) {
                    int h = 0;
                    for (int j = (i * 4); j < (i * 4) + 4; j++) {
                        h <<= 8;
                        h |= ((int) digest[j]) & 0xFF;
                    }
                    int location = Math.abs(h) % cellCount;
                    if (!locations.get(location)) {
                        locations.set(location);
                        processor.apply(location);
                        index++;
                    }
                }
            }
            return true;
        }
    }

    final class DigestHasher extends Hasher<Digest> {

        DigestHasher(Digest key, int seed) {
            // TODO
        }

        @Override
        void add(int cells, Digest key, IBF<Digest> ibf, int idHash) {
            // TODO Auto-generated method stub

        }

        @Override
        boolean contains(int cells, Digest key, IBF<Digest> invertableBloomFilter) {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        void decode(int cell, Digest s, int count, IBF<Digest> ibf, Queue<Integer> pure, int keyHash) {
            // TODO Auto-generated method stub

        }

        @Override
        int identityHash() {
            // TODO Auto-generated method stub
            return 0;
        }
    }

    abstract class Hasher<M> {

        abstract void add(int cells, M key, IBF<M> ibf, int keyHash);

        abstract boolean contains(int cells, M key, IBF<M> ibf);

        abstract void decode(int cells, M s, int count, IBF<M> ibf, Queue<Integer> pure, int keyHash);

        abstract int identityHash();
    }

    final class IntHasher extends TwisterHasher<Integer> {

        IntHasher(int key) {
            super(key);
        }

        @Override
        void processIt(Integer key) {
            process(key);
        }

    }

    final class LongHasher extends TwisterHasher<Long> {
        LongHasher(long key, ByteBuffer seed) {
            super(key);
        }

        @Override
        void processIt(Long key) {
            process(key);
        }

    }

    abstract class TwisterHasher<M> extends Hasher<M> {
        private static final long C1         = 0x87c37b91114253d5L;
        private static final long C2         = 0x4cf5ad432745937fL;
        private static final long CHUNK_SIZE = 16;

        long h1;
        long h2;
        int  length;

        TwisterHasher(M key) {
            seed.position(0);
            int s = seed.getInt();
            this.h1 = s;
            this.h2 = IntIBF.smear(s);
            processIt(key);
            makeHash();
        }

        void process(Consumer<Integer> processor) {
            process(hash -> {
                processor.accept(hash);
                return true;
            });
        }

        boolean process(Function<Integer, Boolean> processor) {
            long combinedHash = h1;
            int cellCount = cells();
            BitSet locations = new BitSet(cellCount);
            int index = 0;
            while (index < k) {
                int location = (int) (Math.abs(combinedHash) % cellCount);
                if (!locations.get(location)) {
                    locations.set(location);
                    processor.apply(location);
                    index++;
                }
                combinedHash += h2;
            }
            return true;
        }

        @Override
        void add(int cells, M key, IBF<M> ibf, int idHash) {
            process(hash -> {
                ibf.add(hash, key, idHash);
            });
        }

        void bmix64(long k1, long k2) {
            h1 ^= mixK1(k1);

            h1 = Long.rotateLeft(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;

            h2 ^= mixK2(k2);

            h2 = Long.rotateLeft(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }

        @Override
        boolean contains(int cells, M key, IBF<M> ibf) {
            return process(hash -> {
                if (ibf.count[hash] == 0) {
                    return false;
                }
                return true;
            });

        }

        @Override
        void decode(int cells, M s, int count, IBF<M> ibf, Queue<Integer> pure, int keyHash) {
            process(hash -> {
                ibf.xor(hash, s);
                ibf.hashSum[hash] ^= keyHash;
                ibf.count[hash] -= count;
                if (ibf.isPure(hash)) {
                    pure.add(hash);
                }
            });
        }

        long fmix64(long k) {
            k ^= k >>> 33;
            k *= 0xff51afd7ed558ccdL;
            k ^= k >>> 33;
            k *= 0xc4ceb9fe1a85ec53L;
            k ^= k >>> 33;
            return k;
        }

        @Override
        int identityHash() {
            process(207);
            makeHash();
            return (int) (h1 & Integer.MAX_VALUE);
        }

        void makeHash() {
            h1 ^= length;
            h2 ^= length;

            h1 += h2;
            h2 += h1;

            h1 = fmix64(h1);
            h2 = fmix64(h2);

            h1 += h2;
            h2 += h1;
        }

        long mixK1(long k1) {
            k1 *= C1;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= C2;
            return k1;
        }

        long mixK2(long k2) {
            k2 *= C2;
            k2 = Long.rotateLeft(k2, 33);
            k2 *= C1;
            return k2;
        }

        void process(int i) {
            h1 ^= mixK1(0);
            h2 ^= mixK2(i);
            length += CHUNK_SIZE / 4;
        }

        void process(long l) {
            h1 ^= mixK1(l);
            h2 ^= mixK2(0);
            length += CHUNK_SIZE / 2;
        }

        void process(long[] hash) {
            for (int i = 0; i < hash.length / 2; i += 2) {
                bmix64(hash[i], hash[i + 1]);
                length += CHUNK_SIZE;
            }
            if ((hash.length & 1) != 0) {
                process(hash[hash.length - 1]);
            }
        }

        abstract void processIt(M key);
    }

    record Decode<K> (boolean success, List<K> added, List<K> missing) {
    }

    private static final int        DEFAULT_K        = 3;
    private static final ByteBuffer GOOD_RANDOM_SEED = wrap((int) System.currentTimeMillis());

    static ByteBuffer wrap(int i) {
        ByteBuffer seed = ByteBuffer.allocate(4);
        seed.putInt(IntIBF.smear(i));
        seed.flip();
        return seed;
    }

    int[]            count;
    int[]            hashSum;
    final int        k;
    final ByteBuffer seed;
    int              size;

    public IBF(int d) {
        this(d, GOOD_RANDOM_SEED, DEFAULT_K);
    }

    public IBF(int d, ByteBuffer seed) {
        this(d, seed, DEFAULT_K);
    }

    public IBF(int d, ByteBuffer seed, int k) {
        assert seed != null;
        count = new int[d];
        hashSum = new int[d];
        size = 0;
        this.seed = seed;
        this.k = k;
    }

    public IBF(int d, int seed) {
        this(d, wrap(seed), DEFAULT_K);
    }

    public IBF(int d, int seed, int k) {
        this(d, wrap(seed), k);
    }

    public void add(KeyType key) {
        int idHash = keyHashOf(key);
        hasher(key).add(cells(), key, this, idHash);
        size++;
    }

    public int cells() {
        return count.length;
    }

    public IBF<KeyType> clone() {
        try {
            @SuppressWarnings("unchecked")
            IBF<KeyType> clone = (IBF<KeyType>) super.clone();
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException(e);
        }
    }

    public boolean contains(KeyType key) {
        return hasher(key).contains(cells(), key, this);
    }

    public Decode<KeyType> decode(IBF<KeyType> ibf) {
        List<KeyType> add = new ArrayList<>();
        List<KeyType> miss = new ArrayList<>();
        Queue<Integer> pure = new LinkedList<>();

        for (int i = 0; i < cells(); i++) {
            if (ibf.isPure(i))
                pure.add(i);
        }

        while (!pure.isEmpty()) {
            int i = pure.poll();
            if (!ibf.isPure(i)) {
                continue;
            }
            KeyType sum = ibf.keySum(i);
            int keyHash = keyHashOf(sum);
            int cnt = ibf.count[i];
            if (cnt > 0)
                add.add(sum);
            else
                miss.add(sum);
            hasher(sum).decode(cells(), sum, cnt, ibf, pure, keyHash);
        }

        for (int i = 0; i < ibf.cells(); i++) {
            if (ibf.hashSum[i] != 0 || ibf.count[i] != 0)
                return new Decode<>(false, add, miss);
        }
        return new Decode<>(true, add, miss);
    }

    public int getSize() {
        return size;
    }

    public IBF<KeyType> subtract(IBF<KeyType> b) {
        IBF<KeyType> resultant = cloneEmpty();
        for (int i = 0; i < cells(); i++) {
            resultant.setCell(i, xorResult(i, b.keySum(i)));
            resultant.hashSum[i] = hashSum[i] ^ b.hashSum[i];
            resultant.count[i] = count[i] - b.count[i];
        }
        return resultant;
    }

    protected abstract ByteBuffer keyBuffer();

    void add(int cell, KeyType key, int hash) {
        xor(cell, key);
        hashSum[cell] ^= hash;
        count[cell]++;
    }

    abstract boolean cellIsNull(int i);

    abstract IBF<KeyType> cloneEmpty();

    void delete(int cell, KeyType key, int hash) {
        xor(cell, key);
        hashSum[cell] ^= hash;
        count[cell]--;
    }

    abstract Hasher<KeyType> hasher(KeyType key);

    boolean isPure(int cell) {
        boolean countCorrect = count[cell] == -1 || count[cell] == 1;
        if (countCorrect) {
            int hash = keyHash(cell);
            int current = hashSum[cell];
            return hash == current;
        }
        return false;
//        return (countCorrect && keyHash(cell) == hashSum[cell]);
    }

    abstract int keyHash(int cell);

    abstract int keyHashOf(KeyType key);

    abstract KeyType keySum(int i);

    abstract void setCell(int cell, KeyType value);

    abstract void xor(int cell, KeyType key);

    abstract KeyType xorResult(int i, KeyType idSum);
}
