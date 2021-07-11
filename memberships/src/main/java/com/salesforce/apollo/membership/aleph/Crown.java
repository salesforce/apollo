/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.aleph;

import static com.salesforce.apollo.crypto.Digest.combine;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
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
public record Crown(List<Integer> heights, Digest controlHash) {

    public static Crown crownFromParents(List<Unit> parents, DigestAlgorithm algo) {
        var nProc = parents.size();
        var heights = new ArrayList<Integer>(nProc);
        var hashes = new ArrayList<Digest>(nProc);
        for (Unit u : parents) {
            if (u == null) {
                heights.add(-1);
                hashes.add(algo.getOrigin());
            } else {
                heights.add(u.height());
                hashes.add(u.hash());
            }
        }
        return new Crown(heights, combine(hashes));
    }

    public static Crown newCrown(List<Integer> heights, Digest hash) {
        return new Crown(heights, hash);
    }

    public static Crown emptyCrown(short nProc) {
        return new Crown(IntStream.range(0, nProc).mapToObj(e -> -1).collect(Collectors.toList()),
                         combine(IntStream.range(0, nProc).mapToObj(e -> (Digest) null).collect(Collectors.toList())));
    }
}
