/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.aleph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.aleph.DagFactory.DagAdder;
import com.salesforce.apollo.membership.aleph.linear.CommonRandomPermutation;

/**
 * @author hal.hildebrand
 *
 */
public class CRPTest {
    private static class RandomSourceMock implements RandomSource {

        boolean called = false;

        @Override
        public byte[] dataToInclude(List<Unit> parents, int level) {
            called = true;
            return null;
        }

        // RandomBytes returns a sequence of "random" bits for a given unit.
        // It bases the sequence only on the pid and level, ignoring the unit itself.
        @Override
        public byte[] randomBytes(short process, int level) {
            called = true;
            return (Integer.toString(process) + level).getBytes();
        }
    }

    @Test
    public void returnDifferentPermutations() throws Exception {
        short nProc = 4;
        DagAdder d = null;
        try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/4/regular.txt"))) {
            d = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        var rs = new RandomSourceMock();
        short crpFixedPrefix = nProc;
        var crpIt = new CommonRandomPermutation(d.dag(), rs, crpFixedPrefix, DigestAlgorithm.DEFAULT);
        assertNotNull(crpIt);

        checkIfSameWithProvidedTimingUnit(d.dag(), crpIt, rs);
        assertFalse(rs.called);
    }

    private void checkIfSameWithProvidedTimingUnit(Dag dag, CommonRandomPermutation crpIt, RandomSourceMock rs) {
        checkWithProvidedTimingUnit(dag, crpIt, rs, true);
    }

    private void checkWithProvidedTimingUnit(Dag dag, CommonRandomPermutation crpIt, RandomSourceMock rs,
                                             boolean shouldBeEqual) {
        var permutation = new ArrayList<Unit>();

        crpIt.iterate(2, null, u -> {
            permutation.add(u);
            return true;
        });

        var tu = dag.unitsOnLevel(1).get((short) 1).get(0);
        var permutation2 = new ArrayList<Unit>();
        crpIt.iterate(2, tu, u -> {
            permutation2.add(u);
            return true;
        });

        tu = dag.unitsOnLevel(1).get((short) 2).get(0);
        var permutation3 = new ArrayList<Unit>();
        crpIt.iterate(2, tu, u -> {
            permutation3.add(u);
            return true;
        });

        if (shouldBeEqual) {
            assertEquals(permutation2, permutation);
            assertEquals(permutation3, permutation2);
        } else {
            assertNotEquals(permutation2, permutation);
            assertNotEquals(permutation3, permutation);
            assertNotEquals(permutation3, permutation2);
        }
    }
}
