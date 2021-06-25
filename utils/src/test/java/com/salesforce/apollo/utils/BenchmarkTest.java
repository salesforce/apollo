package com.salesforce.apollo.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.utils.IBF.IntIBF;

public class BenchmarkTest {

    static final int TEST_SIZE = 1000000;// Number of elements to test
    static final int DIFF_SIZE = 10;

    public static void printStat(long start, long end) {
        double diff = (end - start) / 1000.0;
        System.out.println(diff + "s, " + (TEST_SIZE / diff) + " elements/s");
    }

    public static void printInfo(String info) {
        System.out.println(info);
    }

    @Test
    public void bench() {

        final Random r = new Random();

        // Generate elements first
        int s1[] = new int[TEST_SIZE];
        int s2[] = new int[TEST_SIZE];
        int s1_diff[] = new int[DIFF_SIZE];
        int s2_diff[] = new int[DIFF_SIZE];
        for (int i = 0; i < TEST_SIZE - DIFF_SIZE; i++) {
            int val = r.nextInt(100000);
            s1[i] = val;
            s2[i] = val;
        }

        for (int i = TEST_SIZE - DIFF_SIZE; i < TEST_SIZE; i++) {
            int val = r.nextInt(100000);
            s1[i] = val;
            s1_diff[i - (TEST_SIZE - DIFF_SIZE)] = val;
        }

        for (int i = TEST_SIZE - DIFF_SIZE; i < TEST_SIZE; i++) {
            int val = r.nextInt(100000);
            s2[i] = val;
            s2_diff[i - (TEST_SIZE - DIFF_SIZE)] = val;
        }

        // strataEstimator
        int seed = r.nextInt();
        StrataEstimator se1 = new StrataEstimator(seed, i -> IntIBF.smear(i));
        StrataEstimator se2 = new StrataEstimator(seed, i -> IntIBF.smear(i));

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

        IntIBF b1 = new IntIBF((int) (diff * 2), seed);
        IntIBF b2 = new IntIBF((int) (diff * 2), seed);

        // Add elements
        System.out.print("ibf.add(): ");
        long start_add = System.currentTimeMillis();
        for (int i = 0; i < TEST_SIZE; i++) {
            b1.add(s1[i]);
        }
        long end_add = System.currentTimeMillis();
        printStat(start_add, end_add);

        // Check for existing elements with contains()
        System.out.print("ibf.contains(), existing: ");
        long start_contains = System.currentTimeMillis();
        for (int i = 0; i < TEST_SIZE; i++) {
            b1.contains(s1[i]);
        }
        long end_contains = System.currentTimeMillis();
        printStat(start_contains, end_contains);

        // subtract invertible bloom filter
        for (int i = 0; i < TEST_SIZE; i++) {
            b2.add(s2[i]);
        }
        System.out.print("ibf.subtract()");
        long start_subtract = System.currentTimeMillis();
        IBF<Integer> res = b1.subtract(b2);
        long end_subtract = System.currentTimeMillis();
        printStat(start_subtract, end_subtract);

        // decode the result of the subtract operation
        System.out.print("ibf.decode()");
        long start_decode = System.currentTimeMillis();
        Pair<List<Integer>, List<Integer>> decodeResult = b1.decode(res);
        long end_decode = System.currentTimeMillis();
        printStat(start_decode, end_decode);

        System.out.println("=========benchmark end==========");

        // judge whether or not the result of the decode is right
        if (decodeResult == null || decodeResult.a.size() != DIFF_SIZE || decodeResult.b.size() != DIFF_SIZE) {
            System.out.println("decode error");
            return;
        } else {
            Collections.sort(decodeResult.a);
            Collections.sort(decodeResult.b);
            Arrays.sort(s1_diff);
            Arrays.sort(s2_diff);
            for (int i = 0; i < DIFF_SIZE; i++) {
                String str = s1_diff[i] + ":" + decodeResult.a.get(i) + "," + s2_diff[i] + ":" + decodeResult.b.get(i);
                printInfo(str);
                if (s1_diff[i] != decodeResult.a.get(i) || s2_diff[i] != decodeResult.b.get(i)) {
                    System.out.println("decode error");
                    return;
                }
            }
        }
        printInfo("decode success");
    }
}
