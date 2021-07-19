/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import static com.salesforce.apollo.crypto.Digest.combine;

import java.util.stream.IntStream;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;

/**
 * Crown represents nProc units created by different processes in a condensed
 * form. It contains heights of the units and a combined hash of the units - the
 * controlHash. Any missing unit is represented by height -1, and
 * DigestAlgorithm.origin()
 * 
 * @author hal.hildebrand
 *
 */
public record Crown(int[] heights, Digest controlHash) {

    public static Crown crownFromParents(Unit[] parents, DigestAlgorithm algo) {
        var nProc = parents.length;
        var heights = new int[nProc];
        var hashes = new Digest[nProc];
        int i = 0;
        for (Unit u : parents) {
            if (u == null) {
                heights[i] = -1;
                hashes[i] = algo.getOrigin();
            } else {
                heights[i] = u.height();
                hashes[i] = u.hash();
            }
            i++;
        }
        return new Crown(heights, combine(algo, hashes));
    }

    public static Crown newCrown(int[] heights, Digest hash) {
        return new Crown(heights, hash);
    }

    public static Crown emptyCrown(short nProc, DigestAlgorithm digestAlgorithm) {
        return new Crown(IntStream.range(0, nProc).map(e -> -1).toArray(), combine(digestAlgorithm, new Digest[nProc]));
    }
}
