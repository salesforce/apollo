/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.random.BitsStreamGenerator;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.utils.BloomFilter.DigestBloomFilter;

/**
 * @author hal.hildebrand
 *
 */
public class BloomFilterTest {

    @Test
    public void smoke() throws Exception {
        int max = 1_000_000;
        double target = 0.125;
        BloomFilter<Digest> biff = new DigestBloomFilter(666, max, target);

        BitsStreamGenerator random = Utils.bitStreamEntropy();
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
            if (i % 80_000 == 0) {
                System.out.println();
            }
            if (i % 1000 == 0) {
                System.out.print('.');
            }
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

}
