/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import static com.salesforce.apollo.cryptography.Digest.combine;

import com.salesforce.apollo.ethereal.proto.Crown_s;
import com.salesforce.apollo.ethereal.proto.Crown_s.Builder;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;

/**
 * Crown represents nProc units created by different processes in a condensed form. It contains heights of the units and
 * a combined hash of the units - the controlHash. Any missing unit is represented by height -1, and
 * DigestAlgorithm.origin()
 *
 * @author hal.hildebrand
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

    public static Crown from(Crown_s crown) {
        var heights = new int[crown.getHeightsCount()];
        for (int i = 0; i < heights.length; i++) {
            heights[i] = crown.getHeights(i);
        }
        return new Crown(heights, new Digest(crown.getControlHash()));
    }

    public Crown_s toCrown_s() {
        Builder builder = Crown_s.newBuilder().setControlHash(controlHash.toDigeste());
        for (int i : heights) {
            builder.addHeights(i);
        }
        return builder.build();
    }
}
