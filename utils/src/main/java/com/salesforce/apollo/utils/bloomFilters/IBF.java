/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils.bloomFilters;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import com.salesfoce.apollo.utils.proto.IBiff;
import com.salesfoce.apollo.utils.proto.IBiffCommon;
import com.salesfoce.apollo.utils.proto.IBiffCommon.Builder;
import com.salesfoce.apollo.utils.proto.IntIBiff;
import com.salesfoce.apollo.utils.proto.LongIBiff;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;

/**
 * Invertible Bloom Filter. Optimized for use in efficient set reconciliation.
 * 
 * @author hal.hildebrand
 *
 */
abstract public class IBF<KeyType> implements Cloneable {

    public static class DigestIBF extends IBF<Digest> {
        static Hash<Digest> newHash(long seed, int m, int k) {
            return new Hash<Digest>(seed, m, k) {
                @Override
                Hasher<Digest> newHasher() {
                    return new DigestHasher();
                }
            };
        }

        private final long[] keySum;

        public DigestIBF(DigestAlgorithm d, long seed, int m) {
            this(d, seed, m, DEFAULT_K);
        }

        public DigestIBF(DigestAlgorithm d, long seed, int m, int k) {
            this(newHash(seed, m, k), d);
        }

        public DigestIBF(Hash<Digest> h, DigestAlgorithm digestAlgorithm) {
            super(h);
            keySum = new long[h.m * digestAlgorithm.longLength()];
        }

        public DigestIBF(IntIBiff ibf) {
            super(ibf.getCommon(), newHash(ibf.getCommon().getSeed(), ibf.getCommon().getK(), ibf.getCommon().getM()));
            keySum = new long[ibf.getKSumCount()];
            int index = 0;
            for (long i : ibf.getKSumList()) {
                keySum[index++] = i;
            }
        }

        private DigestIBF(Hash<Digest> h, int expandedLength) {
            super(h);
            keySum = new long[expandedLength];
        }

        @Override
        public IBiff toIBiff() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        IBF<Digest> cloneEmpty() {
            return new DigestIBF(h, keySum.length);
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
        static Hash<Integer> newHash(long seed, int m, int k) {
            return new Hash<Integer>(seed, m, k) {
                @Override
                Hasher<Integer> newHasher() {
                    return new IntHasher();
                }
            };
        }

        private final int[] keySum;

        public IntIBF(Hash<Integer> h) {
            super(h);
            keySum = new int[h.m];
        }

        public IntIBF(IntIBiff ibf) {
            super(ibf.getCommon(), newHash(ibf.getCommon().getSeed(), ibf.getCommon().getK(), ibf.getCommon().getM()));
            keySum = new int[ibf.getKSumCount()];
            int index = 0;
            for (int i : ibf.getKSumList()) {
                keySum[index++] = i;
            }
        }

        public IntIBF(long seed, int m) {
            this(seed, m, DEFAULT_K);
        }

        public IntIBF(long seed, int m, int k) {
            this(newHash(seed, m, k));
        }

        @Override
        public IBiff toIBiff() {
            return IBiff.newBuilder().setInteger(toIntIBiff()).build();
        }

        public IntIBiff toIntIBiff() {
            IntIBiff.Builder builder = IntIBiff.newBuilder().setCommon(toCommon());
            for (int i : keySum) {
                builder.addKSum(i);
            }
            return builder.build();
        }

        @Override
        IBF<Integer> cloneEmpty() {
            return new IntIBF(h);
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

        static Hash<Long> newHash(long seed, int m, int k) {
            return new Hash<Long>(seed, m, k) {
                @Override
                Hasher<Long> newHasher() {
                    return new LongHasher();
                }
            };
        }

        private final long[] keySum;

        public LongIBF(Hash<Long> h) {
            super(h);
            keySum = new long[h.m];
        }

        public LongIBF(IntIBiff ibf) {
            super(ibf.getCommon(), newHash(ibf.getCommon().getSeed(), ibf.getCommon().getK(), ibf.getCommon().getM()));
            keySum = new long[ibf.getKSumCount()];
            int index = 0;
            for (long i : ibf.getKSumList()) {
                keySum[index++] = i;
            }
        }

        public LongIBF(long seed, int m) {
            this(seed, m, DEFAULT_K);
        }

        public LongIBF(long seed, int m, int k) {
            this(newHash(seed, m, k));
        }

        @Override
        public IBiff toIBiff() {
            return IBiff.newBuilder().setLong(toLongIBiff()).build();
        }

        public LongIBiff toLongIBiff() {
            LongIBiff.Builder builder = LongIBiff.newBuilder().setCommon(toCommon());
            for (long l : keySum) {
                builder.addKSum(l);
            }
            return builder.build();
        }

        @Override
        IBF<Long> cloneEmpty() {
            return new LongIBF(h);
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

    public record Decode<K> (boolean success, List<K> added, List<K> missing) {
    }

    private static final int DEFAULT_K = 3;

    int[] count;

    final Hash<KeyType> h;
    int[]               hashSum;
    int                 size;

    public IBF(Hash<KeyType> h) {
        this.h = h;
        count = new int[h.m];
        hashSum = new int[h.m];
        size = 0;
    }

    public IBF(IBiffCommon common, Hash<KeyType> h) {
        this.h = h;

        count = new int[common.getCountCount()];
        int index = 0;
        for (int c : common.getCountList()) {
            count[index++] = c;
        }

        hashSum = new int[common.getHashSumCount()];
        index = 0;
        for (int hs : common.getHashSumList()) {
            hashSum[index++] = hs;
        }

        size = common.getSize();
    }

    public void add(KeyType key) {
        int idHash = keyHashOf(key);
        for (int hash : h.hashes(key)) {
            add(hash, key, idHash);
        }
        size++;
    }

    @Override
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
        for (int hash : h.hashes(key)) {
            if (count[hash] == 0) {
                return false;
            }
        }
        return true;
    }

    public Decode<KeyType> decode(IBF<KeyType> ibf) {
        List<KeyType> add = new ArrayList<>();
        List<KeyType> miss = new ArrayList<>();
        Queue<Integer> pure = new LinkedList<>();

        for (int i = 0; i < h.m; i++) {
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
            decode(h.m, sum, cnt, ibf, pure, keyHash);
        }

        for (int i = 0; i < h.m; i++) {
            if (ibf.hashSum[i] != 0 || ibf.count[i] != 0)
                return new Decode<>(false, add, miss);
        }
        return new Decode<>(true, add, miss);
    }

    public <T> IBF<T> from(IBiff ibiff) {
        if (ibiff.hasInteger()) {
            @SuppressWarnings("unchecked")
            IBF<T> ibf = (IBF<T>) new IntIBF(ibiff.getInteger());
            return ibf;
        }
        if (ibiff.hasLong()) {
            @SuppressWarnings("unchecked")
            IBF<T> ibf = (IBF<T>) new LongIBF(ibiff.getInteger());
            return ibf;
        }
        if (ibiff.hasDigest()) {
            @SuppressWarnings("unchecked")
            IBF<T> ibf = (IBF<T>) new DigestIBF(ibiff.getInteger());
            return ibf;
        }
        throw new IllegalArgumentException("Unknown IBF type");
    }

    public int getM() {
        return h.m;
    }

    public int getSize() {
        return size;
    }

    public boolean isPure(int cell) {
        boolean countCorrect = count[cell] == -1 || count[cell] == 1;
        if (countCorrect) {
            int hash = keyHash(cell);
            int current = hashSum[cell];
            return hash == current;
        }
        return false;
    }

    /**
     * Warning: key must have been already inserted for this logic to work correctly
     */
    public void remove(KeyType key) {
        int idHash = keyHashOf(key);
        for (int hash : h.hashes(key)) {
            delete(hash, key, idHash);
        }
        size--;
    }

    public IBF<KeyType> subtract(IBF<KeyType> b) {
        IBF<KeyType> resultant = cloneEmpty();
        for (int i = 0; i < h.m; i++) {
            resultant.setCell(i, xorResult(i, b.keySum(i)));
            resultant.hashSum[i] = hashSum[i] ^ b.hashSum[i];
            resultant.count[i] = count[i] - b.count[i];
        }
        return resultant;
    }

    abstract public IBiff toIBiff();

    protected IBiffCommon toCommon() {
        Builder builder = IBiffCommon.newBuilder().setSize(size).setM(h.m).setK(h.k);
        for (int i : count) {
            builder.addCount(i);
        }
        for (int i : hashSum) {
            builder.addHashSum(i);
        }
        return builder.build();
    }

    void add(int cell, KeyType key, int hash) {
        xor(cell, key);
        hashSum[cell] ^= hash;
        count[cell]++;
    }

    abstract IBF<KeyType> cloneEmpty();

    void decode(int cells, KeyType key, int count, IBF<KeyType> ibf, Queue<Integer> pure, int keyHash) {
        for (int hash : h.hashes(key)) {
            ibf.xor(hash, key);
            ibf.hashSum[hash] ^= keyHash;
            ibf.count[hash] -= count;
            if (ibf.isPure(hash)) {
                pure.add(hash);
            }
        }
    }

    void delete(int cell, KeyType key, int hash) {
        xor(cell, key);
        hashSum[cell] ^= hash;
        count[cell]--;
    }

    int keyHash(int cell) {
        return h.identityHash(keySum(cell));
    }

    int keyHashOf(KeyType key) {
        return h.identityHash(key);
    }

    abstract KeyType keySum(int i);

    abstract void setCell(int cell, KeyType value);

    abstract void xor(int cell, KeyType key);

    abstract KeyType xorResult(int i, KeyType idSum);
}
