package com.salesforce.apollo.utils.bloomFilters;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.utils.bloomFilters.StrataEstimator.IntStrataEstimator;

public class StrataEstimatorTest {

    @Test
    public void testDecode() throws Exception {
        Random r = new Random(0x1638);

        final int cardinality = 10000;
        final int expectedDiffs = 10;
        List<Integer> s1 = new ArrayList<>();
        List<Integer> s2 = new ArrayList<>();

        for (int i = 0; i < cardinality - expectedDiffs; i++) {
            int val = r.nextInt(99999999);
            s1.add(val);
            s2.add(val);
        }

        for (int i = cardinality - expectedDiffs; i < cardinality; i++) {
            int val = r.nextInt(99999999);
            s1.add(i, val);
        }

        for (int i = cardinality - expectedDiffs; i < cardinality; i++) {
            int val = r.nextInt(99999999);
            s2.add(i, val);
        }

        int seed = r.nextInt();
        StrataEstimator<Integer> se1 = new IntStrataEstimator(seed);
        StrataEstimator<Integer> se2 = new IntStrataEstimator(seed);
        se1.encode(s1);
        se2.encode(s2);
        assertEquals(expectedDiffs * 2, se1.decode(se2));

    }

}
