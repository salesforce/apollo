/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils.bloomFilters;

import java.util.Collection;

import org.apache.commons.math3.stat.Frequency;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.utils.bloomFilters.IBF.Decode;
import com.salesforce.apollo.utils.bloomFilters.IBF.DigestIBF;
import com.salesforce.apollo.utils.bloomFilters.IBF.IntIBF;
import com.salesforce.apollo.utils.bloomFilters.IBF.LongIBF;

/**
 * A difference estimator based on Invertible Bloom Filters. Provides an
 * estimate of the expected cadinality of differences between two gossiping
 * parters replicating state
 * 
 * @author hal.hildebrand
 *
 * @param <T>
 */
public abstract class StrataEstimator<T> {

    public static class DigestStrataEstimator extends StrataEstimator<Digest> {
        private final DigestAlgorithm digestAlgorithm;

        public DigestStrataEstimator(DigestAlgorithm digestAlgorithm, int seed) {
            super(seed);
            this.digestAlgorithm = digestAlgorithm;
        }

        public DigestStrataEstimator(DigestAlgorithm digestAlgorithm, int seed, int L) {
            super(seed, L);
            this.digestAlgorithm = digestAlgorithm;
        }

        @Override
        IBF<Digest> newIBF(long seed) {
            return new DigestIBF(digestAlgorithm, seed, 80);
        }

    }

    public static class IntStrataEstimator extends StrataEstimator<Integer> {

        public IntStrataEstimator(long seed) {
            super(seed);
        }

        public IntStrataEstimator(long seed, int L) {
            super(seed, L);
        }

        @Override
        IBF<Integer> newIBF(long seed) {
            return new IntIBF(seed, 80);
        }

    }

    public static class LongStrataEstimator extends StrataEstimator<Long> {

        public LongStrataEstimator(int seed) {
            super(seed);
        }

        public LongStrataEstimator(int seed, int L) {
            super(seed, L);
        }

        @Override
        IBF<Long> newIBF(long seed) {
            return new LongIBF(seed, 200, 4);
        }

    }

    private static final int L = 32;

    private final IBF<T>[] ibfs;

    public StrataEstimator(long seed) {
        this(seed, L);
    }

    @SuppressWarnings("unchecked")
    public StrataEstimator(long seed, int L) {
        ibfs = (IBF<T>[]) new IntIBF[L];
        for (int i = 0; i < ibfs.length; i++) {
            ibfs[i] = newIBF(seed);
        }
    }

    public int decode(StrataEstimator<T> se) {
        @SuppressWarnings("rawtypes")
        IBF[] ibfs2 = se.ibfs;
        int count = 0;
        for (int i = ibfs.length - 1; i >= -1; i--) {
            if (i < 0)
                return count * (int) Math.pow(2, i + 1); 
            @SuppressWarnings("unchecked")
            Decode<T> decResult = ibfs[i].subtract(ibfs2[i]).decode();
            if (decResult == null)
                return count * (int) Math.pow(2, i + 1);
            count += decResult.added().size() + decResult.missing().size();
        }
        return count;
    }

    public IBF<T>[] encode(Collection<T> s) {
        Frequency frequency = new Frequency();
        s.forEach(element -> {
            int hash = ibfs[0].keyHashOf(element);
            int i = Math.min(ibfs.length - 1, Integer.numberOfTrailingZeros(hash));
            frequency.addValue(i);
            ibfs[i].add(element);
        });
        System.out.println();
        System.out.println(frequency);
        return ibfs;
    }

    public IBF<T>[] getIbfs() {
        return ibfs;
    }

    abstract IBF<T> newIBF(long seed);
}
