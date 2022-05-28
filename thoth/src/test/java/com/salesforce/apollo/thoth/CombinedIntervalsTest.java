/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class CombinedIntervalsTest {

    @Test
    public void smoke() {
        List<KeyInterval> intervals = new ArrayList<>();

        intervals.add(new KeyInterval(Digest.normalized(DigestAlgorithm.DEFAULT, new byte[] { 0, (byte) 200 }),
                                      Digest.normalized(DigestAlgorithm.DEFAULT, new byte[] { 0, (byte) 241 })));
        intervals.add(new KeyInterval(Digest.normalized(DigestAlgorithm.DEFAULT, new byte[] { 0, 50 }),
                                      Digest.normalized(DigestAlgorithm.DEFAULT, new byte[] { 0, 75 })));
        intervals.add(new KeyInterval(Digest.normalized(DigestAlgorithm.DEFAULT, new byte[] { 0, 50 }),
                                      Digest.normalized(DigestAlgorithm.DEFAULT, new byte[] { 0, 90 })));
        intervals.add(new KeyInterval(Digest.normalized(DigestAlgorithm.DEFAULT, new byte[] { 0, 25 }),
                                      Digest.normalized(DigestAlgorithm.DEFAULT, new byte[] { 0, 49 })));
        intervals.add(new KeyInterval(Digest.normalized(DigestAlgorithm.DEFAULT, new byte[] { 0, 25 }),
                                      Digest.normalized(DigestAlgorithm.DEFAULT, new byte[] { 0, 49 })));
        intervals.add(new KeyInterval(Digest.normalized(DigestAlgorithm.DEFAULT, new byte[] { 0, (byte) 128 }),
                                      Digest.normalized(DigestAlgorithm.DEFAULT, new byte[] { 0, (byte) 175 })));
        CombinedIntervals combined = new CombinedIntervals(intervals);
        List<KeyInterval> compressed = combined.getIntervals();
        System.out.println(compressed);
        assertEquals(4, compressed.size());
    }
}
