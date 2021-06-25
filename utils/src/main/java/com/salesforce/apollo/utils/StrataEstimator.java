package com.salesforce.apollo.utils;

import java.util.List;
import java.util.function.Function;

import com.salesforce.apollo.utils.IBF.IntIBF;

public class StrataEstimator {
    private static final int L = 32;// Is U the hash range? the ith partition covers 1/2^(i+1) of U

    private final Function<Integer, Integer> hasher;
    private final IntIBF[]                   ibfs;

    public StrataEstimator(int seed, Function<Integer, Integer> hasher) {
        this(seed, hasher, L);
    }

    public StrataEstimator(int seed, Function<Integer, Integer> hasher, int L) {
        this.hasher = hasher;
        ibfs = new IntIBF[L];
        for (int i = 0; i < ibfs.length; i++) {
            // ?? how to determine the approximate size of the ibfs[i]?
            ibfs[i] = new IntIBF(100, seed);
        }
    }

    public int decode(StrataEstimator se) {
        @SuppressWarnings("rawtypes")
        IBF[] ibfs2 = se.ibfs;
        int count = 0;
        for (int i = ibfs.length - 1; i >= -1; i--) {
            if (i < 0)
                return count * (int) Math.pow(2, i + 1);
            @SuppressWarnings("unchecked")
            IBF<Integer> subResult = ibfs[i].subtract(ibfs2[i]);
            Pair<List<Integer>, List<Integer>> decResult = ibfs[i].decode(subResult);
            if (decResult == null)
                return count * (int) Math.pow(2, i + 1);
            count += decResult.a.size() + decResult.b.size();
        }
        return count;
    }

    public IntIBF[] encode(int[] s) {
        for (int element : s) {
            int i = Math.min(ibfs.length -1,  Integer.numberOfTrailingZeros(hasher.apply(element)));
            ibfs[i].add(element);
        }
        return ibfs;
    }

    public IBF.IntIBF[] getIbfs() {
        return ibfs;
    }

}
