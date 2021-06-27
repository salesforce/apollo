package com.salesforce.apollo.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.utils.bloomFilters.IBF;
import com.salesforce.apollo.utils.bloomFilters.IBF.Decode;
import com.salesforce.apollo.utils.bloomFilters.IBF.IntIBF;
import com.salesforce.apollo.utils.bloomFilters.StrataEstimator;
import com.salesforce.apollo.utils.bloomFilters.StrataEstimator.IntStrataEstimator;

public class BenchmarkTest {

    static final int DIFF_SIZE    = 10;
    static final int SAMPLE_RANGE = Integer.MAX_VALUE; // range of sampled ints
    static final int TEST_SIZE    = 1_000_000;         // Number of elements to test

    public static void printInfo(String info) {
        System.out.println(info);
    }

    public static void printStat(long start, long end) {
        double diff = (end - start) / 1_000.0;
        System.out.println(diff + "s, " + (TEST_SIZE / diff) + " elements/s");
    }

    private Random        entropy;
    private Set<Integer>  s1;
    private List<Integer> s1_diff;
    private Set<Integer>  s2;
    private List<Integer> s2_diff;
    private int           seed;

    @BeforeEach
    public void before() {
        entropy = new Random(0x1638);
        seed = entropy.nextInt();

        s1 = new HashSet<>();
        s2 = new HashSet<>();
        s1_diff = new ArrayList<>();
        s2_diff = new ArrayList<>();
        while (s1.size() < TEST_SIZE - DIFF_SIZE) {
            int val = entropy.nextInt(SAMPLE_RANGE);
            s1.add(val);
            s2.add(val);
        }

        while (s1.size() < TEST_SIZE) {
            int val = entropy.nextInt(SAMPLE_RANGE);
            if (s1.add(val)) {
                s1_diff.add(val);
            }
        }

        for (int i = TEST_SIZE - DIFF_SIZE; s2.size() < TEST_SIZE; i++) {
            int val = entropy.nextInt(SAMPLE_RANGE);
            s2.add(val);
            s2_diff.add(i - (TEST_SIZE - DIFF_SIZE), val);
        }
        Collections.sort(s1_diff);
        Collections.sort(s2_diff);
    }

    @Test
    public void bench() {

        StrataEstimator<Integer> se1 = new IntStrataEstimator(seed);
        StrataEstimator<Integer> se2 = new IntStrataEstimator(seed);

        System.out.println("=========benchmark start==========");
        System.out.print("StrataEstimator.encode(): ");
        long start_se_encode = System.currentTimeMillis();
        se1.encode(s1);
        long end_se_encode = System.currentTimeMillis();
        printStat(start_se_encode, end_se_encode);

        se2.encode(s2);

        System.out.print("StrataEstimator.decode(): ");
        long start_se_decode = System.currentTimeMillis();
        int diff = se1.decode(se2);
        long end_se_decode = System.currentTimeMillis();
        printStat(start_se_decode, end_se_decode);

        printInfo("the size of the set diffence:" + DIFF_SIZE * 2 + ",the result of estimating:" + diff);

        System.out.println("==========");
        // invertible bloom filter

        IntIBF b1 = new IntIBF(seed, (int) (diff * 2));
        IntIBF b2 = new IntIBF(seed, (int) (diff * 2));

        // Add elements
        System.out.print("ibf.add(): ");
        long start_add = System.currentTimeMillis(); 
        s1.forEach(element -> b1.add(element));
        long end_add = System.currentTimeMillis();
        printStat(start_add, end_add);

        // Check for existing elements with contains()
        System.out.print("ibf.contains(), existing: ");
        long start_contains = System.currentTimeMillis();
        s1.forEach(element -> b1.contains(element));
        long end_contains = System.currentTimeMillis();
        printStat(start_contains, end_contains);

        // subtract invertible bloom filter
        s2.forEach(element -> b2.add(element));
        System.out.print("ibf.subtract()");
        long start_subtract = System.currentTimeMillis();
        IBF<Integer> res = b1.subtract(b2);
        long end_subtract = System.currentTimeMillis();
        printStat(start_subtract, end_subtract);

        // decode the result of the subtract operation
        System.out.print("ibf.decode()");
        long start_decode = System.currentTimeMillis();
        Decode<Integer> decodeResult = b1.decode(res);
        long end_decode = System.currentTimeMillis();
        printStat(start_decode, end_decode);

        System.out.println("=========benchmark end==========");

        // judge whether or not the result of the decode is right
        assertNotNull(decodeResult, "No decode result");
        assertEquals(DIFF_SIZE, decodeResult.added().size(), "Incorrect differences added");
        assertEquals(DIFF_SIZE, decodeResult.missing().size(), "Incorrect differences missing");
        Collections.sort(decodeResult.added());
        Collections.sort(decodeResult.missing());
        Collections.sort(s1_diff);
        Collections.sort(s2_diff);
        for (int i = 0; i < DIFF_SIZE; i++) {
            String str = s1_diff.get(i) + ":" + decodeResult.added().get(i) + "," + s2_diff.get(i) + ":"
                    + decodeResult.missing().get(i);
            printInfo(str);
            assertEquals(s1_diff.get(i), decodeResult.added().get(i), "S1 diff does not match decode added result");
            assertEquals(s2_diff.get(i), decodeResult.missing().get(i), "S2 diff does not match decode missing result");
        }
        printInfo("decode success");
    }

    @Test
    public void benchPerf() {
        for (int i = 0; i < 100; i++) {
            bench();
        }
    }
}
