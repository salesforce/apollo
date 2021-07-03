/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils.bloomFilters;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.SecureRandom;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.utils.Utils;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.BytesBloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;

/**
 * @author hal.hildebrand
 *
 */
public class BloomFilterTest {

    @Test
    public void smoke() throws Exception {
        int max = 1_000_000;
        double target = 0.000125;
        BloomFilter<Digest> biff = new DigestBloomFilter(666, max, target);

        SecureRandom random = Utils.secureEntropy();
        List<Digest> added = new ArrayList<>();
        for (int i = 0; i < max; i++) {
            byte[] hash = new byte[DigestAlgorithm.DEFAULT.digestLength()];
            random.nextBytes(hash);
            Digest d = new Digest(DigestAlgorithm.DEFAULT, hash);
            added.add(d);
            biff.add(d);
        }

        for (Digest d : added) {
            assertTrue(biff.contains(d));
        }

        List<Digest> failed = new ArrayList<>();
        int unknownSample = max * 4;

        for (int i = 0; i < unknownSample; i++) {
            byte[] hash = new byte[DigestAlgorithm.DEFAULT.digestLength()];
            random.nextBytes(hash);
//            if (i % 80_000 == 0) {
//                System.out.println();
//            }
//            if (i % 1000 == 0) {
//                System.out.print('.');
//            }
            Digest d = new Digest(DigestAlgorithm.DEFAULT, hash);
            if (biff.contains(d)) {
                failed.add(d);
            }
        }
        System.out.println();
        double failureRate = (double) failed.size() / (double) unknownSample;
        DecimalFormat format = new DecimalFormat("#.#############");
        double targetWithSlop = target + (target * 0.05);
        System.out.print("Target failure rate: " + format.format(target) + " measured: " + format.format(failureRate)
                + "; failed: " + failed.size() + " out of " + unknownSample + " random probes");
        assertTrue(targetWithSlop >= failureRate);
    }

    @Test
    public void smokeBytes() throws Exception {
        int max = 1_000_000;
        double target = 0.000125;
        BloomFilter<byte[]> biff = new BytesBloomFilter(666, max, target);
        byte[] hash = new byte[32];
        Random random = new Random(0x1638567);

        List<byte[]> added = new ArrayList<>();
        for (int i = 0; i < max; i++) {
            random.nextBytes(hash);
            added.add(hash);
            biff.add(hash);
        }

        for (byte[] d : added) {
            assertTrue(biff.contains(d));
        }

        List<byte[]> failed = new ArrayList<>();
        int unknownSample = max * 4;

        for (int i = 0; i < unknownSample; i++) {
            random.nextBytes(hash);
            if (biff.contains(hash)) {
                failed.add(hash);
            }
        }
        System.out.println();
        double failureRate = (double) failed.size() / (double) unknownSample;
        DecimalFormat format = new DecimalFormat("#.#############");
        double targetWithSlop = target + (target * 0.05);
        System.out.print("Target failure rate: " + format.format(target) + " measured: " + format.format(failureRate)
                + "; failed: " + failed.size() + " out of " + unknownSample + " random probes");
        assertTrue(targetWithSlop >= failureRate);

    }
}
