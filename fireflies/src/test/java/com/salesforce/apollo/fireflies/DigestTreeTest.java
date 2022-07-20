/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Comparator;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;

/**
 * @author hal.hildebrand
 *
 */
public class DigestTreeTest {

    @Test
    public void iteration() throws Exception {
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });

        var digests = new ArrayList<Digest>();
        for (var i = 0; i < 1023; i++) {
            digests.add(new Digest(DigestAlgorithm.DEFAULT, new long[] { entropy.nextLong(), entropy.nextLong(),
                                                                         entropy.nextLong(), entropy.nextLong() }));
        }

        digests.sort(Comparator.naturalOrder());

        var merkle = new DigestTree(new ArrayList<>(digests));
        var iterator = merkle.iterator();
        var i = -1;
        while (iterator.hasNext()) {
            var next = iterator.next();
            if (next.isLeaf()) {
                i++;
                assertEquals(digests.get(i), next.leaf());
            }
        }

        assertEquals(digests, merkle.stream().filter(e -> e.isLeaf()).map(e -> e.leaf()).toList());
    }

    @Test
    public void smokin() throws Exception {
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });

        var digests = new ArrayList<Digest>();
        for (var i = 0; i < 1023; i++) {
            digests.add(new Digest(DigestAlgorithm.DEFAULT, new long[] { entropy.nextLong(), entropy.nextLong(),
                                                                         entropy.nextLong(), entropy.nextLong() }));
        }

        digests.sort(Comparator.naturalOrder());

        var merkle = new DigestTree(digests);

        assertEquals(10, merkle.getHeight());
    }
}
