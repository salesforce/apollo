package com.salesforce.apollo.protocols;

import java.security.SecureRandom;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.commons.math3.distribution.ChiSquaredDistribution;
import org.apache.commons.math3.exception.OutOfRangeException;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.inference.ChiSquareTest;

import com.salesforce.apollo.protocols.BloomFilter.HashFunction;

public class BFHashUniformity {
    public static long seed = 324234234;

    public static void main(String[] args) {

        // ByteBuffer bytes = ByteBuffer.wrap(bytes, off,
        // len).order(ByteOrder.LITTLE_ENDIAN);
        // HashProvider.murmur3BB(bytes);

        testHashing();
    }

    public static void testHashing() {
        SecureRandom random = new SecureRandom(new byte[] { 0, 6, 4, 7, 2, 1 });
        int hashesPerRound = 1_000_000;
        byte[] bytes = new byte[32];
        random.nextBytes(bytes);
        HashKey initial = new HashKey(bytes);
        long n = 10;
        double p = 0.1;
        int rounds = 100;
        double alpha = 0.05;

        StringBuilder log = new StringBuilder();
        HashFunction hf = new HashFunction(initial, n, p);
        testHashDistribution(random, hf, hashesPerRound, rounds, alpha, log);
        System.out.println(log.toString());
    }

    public static void testHashDistribution(SecureRandom entropy, HashFunction hf, int hashesPerRound, int rounds,
                                            double alpha, StringBuilder log) {

        List<List<HashKey>> hashData = generate(entropy, hashesPerRound, rounds);

        double crit = criticalVal(hf.getM(), alpha);
        System.out.println("The critical Chi-Squared-Value for alpha = " + alpha + " is " + crit);

        // Format the data for processing in Mathematica
        StringBuilder chi = new StringBuilder();
        StringBuilder pvalue = new StringBuilder();

        testDistribution(hf, hashesPerRound, rounds, hashData, chi, pvalue);

        // Remove trailing comma
        String chiMathematica = chi.toString();
        chiMathematica = chiMathematica.substring(0, chiMathematica.length() - 1);
        String pValueMathematica = pvalue.toString();
        pValueMathematica = pValueMathematica.substring(0, pValueMathematica.length() - 1);

        log.append("\n");

        log.append("data" + hashesPerRound + "h" + hf.getM() + "size" + hf.getK() + "hashes" + " = { " + chiMathematica
                + "};\n");
        log.append("\n");
        log.append("crit: " + hashesPerRound + " h:" + hf.getM() + " size:" + hf.getK() + " hashes"
                + ", \"Chi-Squared Statistic\"]\n");

        log.append("\n");
        log.append("pdata: " + hashesPerRound + " h: " + hf.getM() + " size: " + hf.getK() + " hashes= { "
                + pValueMathematica + "};\n");
        log.append("\n");
        log.append("pcrit: " + hashesPerRound + " h: " + hf.getM() + " size: " + hf.getK() + " hashes = " + alpha
                + ";\n");
        log.append("\n");
        log.append("show[pdata" + hashesPerRound + "h" + hf.getM() + "size" + hf.getK() + "hashes" + ", pcrit"
                + hashesPerRound + "h" + hf.getM() + "size" + hf.getK() + "hashes" + ", \"p-Value\"]\n");

    }

    public static List<List<HashKey>> generate(SecureRandom entropy, int hashesPerRound, int rounds) {

        List<List<HashKey>> hashData = new ArrayList<>(rounds);
        for (int j = 0; j < rounds; j++) {
            hashData.add(new ArrayList<>(hashesPerRound));
        }
        for (int i = 0; i < rounds; i++) {
            List<HashKey> data = hashData.get(i);
            Set<HashKey> seen = new HashSet<>(hashesPerRound);
            for (int j = 0; j < hashesPerRound; j++) {
                byte[] bytes = new byte[32];
                entropy.nextBytes(bytes);
                HashKey next = new HashKey(bytes);
                if (seen.add(next))
                    data.add(next);
            }

        }
        return hashData;
    }

    public static void plotHistogram(long[] histogram, String name) {
        System.out.println("Histogram for " + name + ":");
        long sum = 0;
        for (long bar : histogram) {
            sum += bar;
        }
        int index = 0;
        for (long bar : histogram) {
            double deviation = Math.abs(bar * 1.0 / sum - 1.0 / histogram.length);
            System.out.println("Index " + index++ + ": " + bar + ", Deviation from expectation: " + deviation);
        }
    }

    public static void testDistribution(HashFunction hf, int hashesPerRound, int rounds, List<List<HashKey>> hashData,
                                        StringBuilder chi, StringBuilder pvalue) {
        DescriptiveStatistics pValues = new DescriptiveStatistics();
        DescriptiveStatistics xs = new DescriptiveStatistics();

        int hashRounds = hashesPerRound / hf.getK();

        for (int i = 0; i < rounds; i++) {
            List<HashKey> data = hashData.get(i);
            long[] observed = new long[hf.getM()];
            for (int j = 0; j < hashRounds; j++) {
                int[] hashes = hf.hash(data.get(j));
                for (int h : hashes) {
                    observed[h]++;
                }
            }

            double[] expected = new double[hf.getM()];
            for (int j = 0; j < hf.getM(); j++) {
                expected[j] = hashesPerRound * 1.0 / hf.getM();
            }

            ChiSquareTest cs = new ChiSquareTest();
            try {
                pValues.addValue(cs.chiSquareTest(expected, observed));
                xs.addValue(cs.chiSquare(expected, observed));
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        String result = "Hashes = " + hashesPerRound + ", size = " + hf.getM() + ", hashes = " + hf.getK()
                + ", rounds = " + rounds;
        // System.out.println(result);

        chi.append(stringify(xs.getValues()));
        pvalue.append("(*" + result + "*)");
        pvalue.append(stringify(pValues.getValues()));
    }

    // Get rid of the scientific 0.34234E8 notation
    public static String stringify(double[] values) {
        NumberFormat f = NumberFormat.getInstance(Locale.ENGLISH);
        f.setGroupingUsed(false);
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (double val : values) {
            sb.append(f.format(val));
            sb.append(",");
        }
        return sb.substring(0, sb.length() - 1) + "},";
    }

    public static double criticalVal(int hashCount, double alpha) {
        ChiSquaredDistribution d = new ChiSquaredDistribution(hashCount - 1);
        try {
            return d.inverseCumulativeProbability(1 - alpha);
        } catch (OutOfRangeException e) {
            return Double.MIN_VALUE;
        }
    }

}
