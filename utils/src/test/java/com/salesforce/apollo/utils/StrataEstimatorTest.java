package com.salesforce.apollo.utils;

import java.util.Random;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.utils.IBF.IntIBF;

public class StrataEstimatorTest {

    @Test
    public void testDecode() throws Exception {
        Random r = new Random(0x1638);

        final int cardinality = 10000;
        final int expectedDiffs = 10;
        int s1[] = new int[cardinality];
        int s2[] = new int[cardinality];

        for (int i = 0; i < cardinality - expectedDiffs; i++) {
            int val = r.nextInt(99999999);
            s1[i] = val;
            s2[i] = val;
        }

        for (int i = cardinality - expectedDiffs; i < cardinality; i++) {
            int val = r.nextInt(99999999);
            s1[i] = val;
        }

        for (int i = cardinality - expectedDiffs; i < cardinality; i++) {
            int val = r.nextInt(99999999);
            s2[i] = val;
        }

        int seed = r.nextInt();
        StrataEstimator se1 = new StrataEstimator(seed, i -> IntIBF.smear(i));
        StrataEstimator se2 = new StrataEstimator(seed, i -> IntIBF.smear(i));
        se1.encode(s1);
        se2.encode(s2);
        assert (expectedDiffs * 2 == se1.decode(se2));

    }

}
