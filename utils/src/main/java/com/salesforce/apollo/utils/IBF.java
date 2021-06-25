/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
abstract public class IBF<KeyType> implements Cloneable {

    public static class IntIBF extends IBF<Integer> {
        public static int smear(int hashCode) {
            hashCode ^= (hashCode >>> 20) ^ (hashCode >>> 12);
            return hashCode ^ (hashCode >>> 7) ^ (hashCode >>> 4);
        }

        private final int[] keySum;

        public IntIBF(int d) {
            super(d);
            keySum = new int[d * 2];
        }

        public IntIBF(int d, int seed) {
            super(d, seed);
            keySum = new int[d * 2];
        }

        public IntIBF(int d, int seed, int k) {
            super(d, seed, k);
            keySum = new int[d * 2];
        }

        @Override
        boolean cellIsNull(int cell) {
            return keySum[cell] == 0;
        }

        @Override
        IBF<Integer> cloneEmpty() {
            return new IntIBF(cells() / 2, seed, k);
        }

        @Override
        Hasher<Integer> hasher(Integer key) {
            return new IntHasher(key, seed);
        }

        @Override
        int keyHash(int cell) {
            return smear(keySum[cell]);
        }

        @Override
        int keyHash(Integer key) {
            return smear(key);
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

        private final long[] keySum;

        public LongIBF(int d) {
            super(d);
            keySum = new long[d * 2];
        }

        public LongIBF(int d, int seed) {
            super(d, seed);
            keySum = new long[d * 2];
        }

        public LongIBF(int d, int seed, int k) {
            super(d, seed, k);
            keySum = new long[d * 2];
        }

        @Override
        boolean cellIsNull(int cell) {
            return keySum[cell] == 0;
        }

        @Override
        IBF<Long> cloneEmpty() {
            return new LongIBF(seed, cells(), k);
        }

        @Override
        IBF<Long>.Hasher<Long> hasher(Long key) {
            return new LongHasher(key, seed);
        }

        @Override
        int keyHash(int cell) {
            return Long.hashCode(keySum[cell]);
        }

        @Override
        int keyHash(Long key) {
            return key.hashCode();
        }

        @Override
        Long keySum(int cell) {
            return keySum[cell];
        }

        @Override
        void setCell(int cell, Long value) {
            // TODO Auto-generated method stub

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
    }

    abstract class Hasher<M> {

        abstract void add(int cells, M key, IBF<M> ibf, int keyHash);

        abstract boolean contains(int cells, M key, IBF<M> ibf);

        abstract void decode(int cells, M s, int count, IBF<M> ibf, Queue<Integer> pure, int keyHash);
    }

    final class IntHasher extends TwisterHasher<Integer> {

        IntHasher(Integer key, int seed) {
            super(key, seed);
        }

        @Override
        void processIt(Integer key) {
            process(key);
        }

    }

    final class LongHasher extends TwisterHasher<Long> {

        LongHasher(Long key, int seed) {
            super(key, seed);
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

        TwisterHasher(M key, int seed) {
            this.h1 = seed;
            this.h2 = seed;
            processIt(key);
            makeHash();
        }

        @Override
        void add(int cells, M key, IBF<M> ibf, int idHash) {
            long combinedHash = h1;
            for (int i = 0; i < k; i++) {
                ibf.add(((int) (combinedHash & Integer.MAX_VALUE)) % cells, key, idHash);
                combinedHash += h2;
            }
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
            long combinedHash = h1;
            for (int i = 0; i < k; i++) {
                int index = ((int) (combinedHash & Integer.MAX_VALUE)) % cells;
                if (!ibf.cellIsNull(index) && ibf.count[index] == 0) {
                    return false;
                }
                combinedHash += h2;
            }
            return true;

        }

        @Override
        void decode(int cells, M s, int count, IBF<M> ibf, Queue<Integer> pure, int keyHash) {
            long combinedHash = h1;
            for (int i = 0; i < k; i++) {
                int index = ((int) (combinedHash & Integer.MAX_VALUE)) % cells;
                ibf.xor(index, s);
                ibf.hashSum[index] ^= keyHash;
                ibf.count[index] -= count;
                if (ibf.isPure(index)) {
                    pure.add(index);
                }
                combinedHash += h2;
            }
        }

        long fmix64(long k) {
            k ^= k >>> 33;
            k *= 0xff51afd7ed558ccdL;
            k ^= k >>> 33;
            k *= 0xc4ceb9fe1a85ec53L;
            k ^= k >>> 33;
            return k;
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
            length += 2;
            h2 ^= mixK2(i);
            length += CHUNK_SIZE / 4;
        }

        void process(long l) {
            h1 ^= mixK1(l);
            length += 4;
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

    /**
     * 
     */
    private static final int DEFAULT_K = 3;

    /**
     * 
     */
    private static final int GOOD_RANDOM_SEED = (int) System.currentTimeMillis();

    int[]     count;
    int[]     hashSum;
    final int k;
    final int seed;
    int       size;

    public IBF(int d) {
        this(d, GOOD_RANDOM_SEED, DEFAULT_K);
    }

    public IBF(int d, int seed) {
        this(d, seed, DEFAULT_K);
    }

    public IBF(int d, int seed, int k) {
        int expansion = d * 2;
        count = new int[expansion];
        hashSum = new int[expansion];
        size = 0;
        this.seed = (int) (seed & Integer.MAX_VALUE);
        this.k = k;
    }

    public void add(KeyType key) {
        int idHash = keyHash(key);
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

    public Pair<List<KeyType>, List<KeyType>> decode(IBF<KeyType> ibf) {
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
            int keyHash = keyHash(sum);
            int cnt = ibf.count[i];
            if (cnt > 0)
                add.add(sum);
            else
                miss.add(sum);
            hasher(sum).decode(cells(), sum, cnt, ibf, pure, keyHash);
        }

        for (int i = 0; i < ibf.cells(); i++) {
            if (ibf.hashSum[i] != 0 || ibf.count[i] != 0)
                return null;
        }
        return new Pair<>(add, miss);
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

    abstract int keyHash(KeyType key);

    abstract KeyType keySum(int i);

    abstract void setCell(int cell, KeyType value);

    abstract void xor(int cell, KeyType key);

    abstract KeyType xorResult(int i, KeyType idSum);
}
