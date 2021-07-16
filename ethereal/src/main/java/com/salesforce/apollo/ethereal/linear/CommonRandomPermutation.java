/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.linear;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.function.Function;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.ethereal.Dag;
import com.salesforce.apollo.ethereal.RandomSource;
import com.salesforce.apollo.ethereal.Unit;

/**
 * 
 * @author hal.hildebrand
 *
 */

public record CommonRandomPermutation(Dag dag, RandomSource randomSource, short crpFixedPrefix,
                                      DigestAlgorithm digestAlgorithm) {

    // Iterates over all the prime units on a given level in random order.
    // It calls the given work function on each of the units until the function
    // returns false or the contents run out.
    //
    // The underlying random permutation of units is generated in two steps
    // (1) the prefix is based only on the previous timing unit and hashes of units
    // (2) the suffix is computed using the random source
    // The second part of the permutation is being calculated only when needed,
    // i.e. the given work function returns true on all the units in the prefix.
    //
    // The function itself returns
    // - false when generating the suffix of the permutation failed (because the dag
    // hasn't reached a level high enough to reveal the randomBytes needed)
    // - true otherwise
    public boolean iterate(int level, Unit previousTU, Function<Unit, Boolean> work) {
        var split = splitProcesses(dag.nProc(), crpFixedPrefix, level, previousTU);

        List<Unit> defaultPermutation = defaultPermutation(dag, level, split.prefix);
        for (var u : defaultPermutation) {
            if (!work.apply(u)) {
                return true;
            }
        }

        var permutation = randomPermutation(randomSource, dag, level, split.suffix, digestAlgorithm);
        if (!permutation.ok) {
            return false;
        }
        for (var u : permutation.units) {
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

    private List<Unit> defaultPermutation(Dag dag, int level, List<Short> pids) {
        var permutation = new ArrayList<Unit>();

        for (short pid : pids) {
            permutation.addAll(dag.unitsOnLevel(level).get(pid));
        }

        Collections.sort(permutation, (a, b) -> a.hash().compareTo(b.hash()));
        return permutation;
    }

    private record rp(List<Unit> units, boolean ok) {}

    private rp randomPermutation(RandomSource rs, Dag dag, int level, List<Short> pids, DigestAlgorithm algo) {
        var permutation = new ArrayList<Unit>();
        var priority = new HashMap<Digest, Digest>();

        var allUnitsOnLevel = dag.unitsOnLevel(level);
        for (short pid : pids) {
            var units = allUnitsOnLevel.get(pid);
            if (units.isEmpty()) {
                continue;
            }
            var randomBytes = rs.randomBytes(pid, level + 5);
            if (randomBytes == null) {
                return new rp(Collections.emptyList(), false);
            }

            try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                baos.write(randomBytes);
                for (var u : units) {
                    baos.write(u.hash().getBytes());
                    priority.put(u.hash(), algo.digest(baos.toByteArray()));
                }
            } catch (IOException e) {
                throw new IllegalStateException("Fatal issue when creating random permutation", e);
            }
            permutation.addAll(units);
        }
        Collections.sort(permutation, (a, b) -> priority.get(a.hash()).compareTo(priority.get(b.hash())));

        return new rp(permutation, true);

    }

}
