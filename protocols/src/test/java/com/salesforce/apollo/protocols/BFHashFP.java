package com.salesforce.apollo.protocols;

import java.security.SecureRandom;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.salesforce.apollo.protocols.BloomFilter.HashFunction;

/**
 * Allows to count actual False Positives.
 */
public class BFHashFP {

    public static void main(String[] args) {
        testHashing();
    }

    public static void testHashing() {
        int inserts = 3000;
        double fpp = 0.001;

        testFalsePositives(inserts, fpp);
    }

    public static void testFalsePositives(int inserts, double p) {
        SecureRandom random = new SecureRandom(new byte[] { 0, 6, 4, 7, 2, 1 });
        byte[] bytes = new byte[32];
        random.nextBytes(bytes);
        HashKey seed = new HashKey(bytes);
        testFP(seed, inserts, p, random);
    }

    public static void testFP(HashKey seed, int n, double p, SecureRandom entropy) {
        List<HashKey> hashData = BFHashUniformity.generate(entropy, n, 1).get(0);
        List<HashKey> probeData = BFHashUniformity.generate(entropy, n * 3, 1).get(0);

        BloomFilter b = new BloomFilter(new HashFunction(seed, n, p));
        int inserts = hashData.size();
        int fps = 0;
        Set<HashKey> seen = new HashSet<>();
        long start = System.nanoTime();
        for (HashKey current : hashData) {
            if (b.contains(current) && !seen.contains(current)) {
                fps++;
            }

            b.add(current);
            seen.add(current);
        }
        long end = System.nanoTime();
        double speed = (1.0 * end - start) / 1000000;

        int totalfps = 0;
        int probed = 0;
        for (HashKey current : probeData) {
            if (probed > inserts)
                break;
            if (!seen.contains(current)) {
                probed++;
                if (b.contains(current)) {
                    totalfps++;
                }
            }
        }

        double fpRate = 100.0 * fps / inserts;
        double totalFpRate = 100.0 * totalfps / inserts;
        System.out.println("speed: " + speed + "ms, false postivies: " + String.format("%1$,.3f", fpRate)
                + ", final FP-Rate:" + String.format("%1$,.3f", totalFpRate));
    }
}
