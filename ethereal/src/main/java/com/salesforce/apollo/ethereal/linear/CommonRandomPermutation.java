/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.linear;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.ethereal.Dag;
import com.salesforce.apollo.ethereal.RandomSource;
import com.salesforce.apollo.ethereal.SlottedUnits;
import com.salesforce.apollo.ethereal.Unit;

/**
 * 
 * @author hal.hildebrand
 *
 */

public record CommonRandomPermutation(Dag dag, RandomSource randomSource, short crpFixedPrefix,
                                      DigestAlgorithm digestAlgorithm) {

    private static final Logger log = LoggerFactory.getLogger(CommonRandomPermutation.class);

    /**
     * Iterates over all the prime units on a given level in random order. It calls
     * the given work function on each of the units until the function returns false
     * or the contents run out.
     * <p>
     * The underlying random permutation of units is generated in two steps
     * <p>
     * (1) the prefix is based only on the previous timing unit and hashes of units
     * <p>
     * (2) the suffix is computed using the random source
     * <p>
     * The second part of the permutation is being calculated only when needed, i.e.
     * the given work function returns true on all the units in the prefix.
     * <p>
     * 
     * The function itself returns
     * <p>
     * - false when generating the suffix of the permutation failed (because the dag
     * hasn't reached a level high enough to reveal the randomBytes needed)
     * <p>
     * - true otherwise
     */
    public boolean iterate(int level, SlottedUnits unitsOnLevel, Unit previousTU, Function<Unit, Boolean> work) {
        var split = splitProcesses(dag.nProc(), crpFixedPrefix, level, previousTU);

        List<Unit> defaultPermutation = defaultPermutation(dag, level, split.prefix, unitsOnLevel);
        if (defaultPermutation.size() != crpFixedPrefix) {
            return false;
        }
        for (var u : defaultPermutation) {
            if (!work.apply(u)) {
                return true;
            }
        }
        return true;

    }

    private record split(List<Short> prefix, List<Short> suffix) {}

    private split splitProcesses(short nProc, short prefixLen, int level, Unit tu) {
        if (prefixLen > nProc) {
            prefixLen = nProc;
        }
        var pids = new ArrayList<Short>();
        for (int pid = 0; pid < nProc; pid++) {
            pids.add((short) ((pid + level) % nProc));
        }
        if (tu == null) {
            return new split(pids.subList(0, prefixLen), pids.subList(prefixLen, pids.size()));

        }
        for (short pid : new ArrayList<>(pids)) {
            pids.set(pid, (short) ((pids.get(pid) + tu.creator()) % nProc));
        }
        return new split(pids.subList(0, prefixLen), pids.subList(prefixLen, pids.size()));

    }

    private List<Unit> defaultPermutation(Dag dag, int level, List<Short> pids, SlottedUnits unitsOnLevel) {
        var permutation = new ArrayList<Unit>();
        for (short pid : pids) {
            permutation.addAll(unitsOnLevel.get(pid));
        }

        Collections.sort(permutation, (a, b) -> a.hash().compareTo(b.hash()));
        log.trace("permutation for: {} : {}", level, permutation);
        return permutation;
    }

}
