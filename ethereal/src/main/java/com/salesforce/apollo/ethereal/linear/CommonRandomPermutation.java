/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.linear;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.ethereal.RandomSource;
import com.salesforce.apollo.ethereal.SlottedUnits;
import com.salesforce.apollo.ethereal.Unit;

/**
 * 
 * @author hal.hildebrand
 *
 */

public record CommonRandomPermutation(short nProc, RandomSource randomSource, short prefix,
                                      DigestAlgorithm digestAlgorithm, String logLabel) {

    private static final Logger log = LoggerFactory.getLogger(CommonRandomPermutation.class);

    /**
     * Iterates over all the prime units on a given level in random order. It calls
     * the given work function on each of the units until the function returns false
     * or the contents run out.
     */
    public void iterate(int level, SlottedUnits unitsOnLevel, Unit previousTU, Function<Unit, Boolean> work) {
        List<Unit> permutation = randomPermutation(level, pidOrder(prefix, level, previousTU), unitsOnLevel,
                                                   previousTU);
        permutation.stream().map(work).filter(r -> !r).findFirst().orElse(true);
    }

    private List<Short> pidOrder(short prefixLen, int level, Unit tu) {
        assert prefixLen >= 0;
        if (prefixLen > nProc) {
            prefixLen = nProc;
        }
        var pids = new ArrayList<Short>();
        for (int pid = 0; pid < nProc; pid++) {
            pids.add((short) ((pid + level) % nProc));
        }
        if (tu == null) {
            return pids.subList(0, prefixLen);

        }
        for (short pid : new ArrayList<>(pids)) {
            pids.set(pid, (short) ((pids.get(pid) + tu.creator()) % nProc));
        }
        return pids.subList(0, prefixLen);

    }

    @SuppressWarnings("unused")
    private List<Unit> defaultPermutation(int level, List<Short> pids, SlottedUnits unitsOnLevel) {
        var permutation = new ArrayList<Unit>();
        for (short pid : pids) {
            permutation.addAll(unitsOnLevel.get(pid));
        }

        Collections.sort(permutation, (a, b) -> a.hash().compareTo(b.hash()));
        log.trace("permutation for: {} : {} on: {}", level, permutation, logLabel);
        return permutation;
    }

    private List<Unit> randomPermutation(int level, List<Short> pids, SlottedUnits unitsOnLevel, Unit unit) {
        var permutation = new ArrayList<Unit>();
        var priority = new HashMap<Digest, Digest>();

        for (short pid : pids) {
            var units = unitsOnLevel.get(pid);
            if (units.isEmpty()) {
                continue;
            }
            var randomBytes = randomSource.randomBytes(pid, level + 5);
            var cumulative = randomBytes == null ? digestAlgorithm.getOrigin() : digestAlgorithm.digest(randomBytes);
            cumulative = unit == null ? cumulative : cumulative.xor(unit.hash());
            for (var u : units) {
                cumulative = cumulative.xor(u.hash());
                priority.put(u.hash(), cumulative);
            }
            permutation.addAll(units);
        }
        Collections.sort(permutation, (a, b) -> priority.get(a.hash()).compareTo(priority.get(b.hash())));

        return permutation;

    }

}
