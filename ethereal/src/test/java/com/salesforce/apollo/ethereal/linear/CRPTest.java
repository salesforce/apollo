/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.linear;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.ethereal.Dag;
import com.salesforce.apollo.ethereal.DagFactory;
import com.salesforce.apollo.ethereal.DagFactory.DagAdder;
import com.salesforce.apollo.ethereal.DagReader;
import com.salesforce.apollo.ethereal.RandomSource;
import com.salesforce.apollo.ethereal.Unit;

/**
 * @author hal.hildebrand
 *
 */
public class CRPTest {
    public static class DeterministicRandomSource extends RandomSourceMock {
        Map<Integer, byte[]> randomBytes;

        public DeterministicRandomSource(HashMap<Integer, byte[]> randomBytes) {
            this.randomBytes = randomBytes;
        }

        @Override
        public byte[] dataToInclude(Unit[] parents, int level) {
            return super.dataToInclude(parents, level);
        }

        @Override
        public byte[] randomBytes(short process, int level) {
            super.randomBytes(process, level);
            return randomBytes.get(level);
        }

    }

    public static class RandomSourceMock implements RandomSource {

        boolean called = false;

        @Override
        public byte[] dataToInclude(Unit[] parents, int level) {
            called = true;
            return null;
        }

        // RandomBytes returns a sequence of "random" bits for a given unit.
        // It bases the sequence only on the pid and level, ignoring the unit itself.
        @Override
        public byte[] randomBytes(short process, int level) {
            called = true;
            byte[] answer = new byte[33];
            answer[32] = (byte) (process + level);
            return answer;
        }
    }

    @Test
    public void emptyDagProvidesNoUnits() throws Exception {
        short nProc = 4;
        Dag dag = new DagFactory.TestDagFactory().createDag(nProc).dag();
        var rs = new RandomSourceMock();
        short crpFixedPrefix = nProc;
        var crpIt = new CommonRandomPermutation(dag, rs, crpFixedPrefix, DigestAlgorithm.DEFAULT);
        assertNotNull(crpIt);

        AtomicBoolean called = new AtomicBoolean(false);
        var result = crpIt.iterate(0, null, u -> {
            called.set(true);
            return true;
        });
        assertFalse(called.get());
        assertTrue(result);
    }

    @Test
    public void enoughUnitsReturnAllUnits() throws Exception {
        short nProc = 4;
        DagAdder d = null;
        try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/4/regular.txt"))) {
            d = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        var rs = new RandomSourceMock();
        short crpFixedPrefix = nProc;
        var crpIt = new CommonRandomPermutation(d.dag(), rs, crpFixedPrefix, DigestAlgorithm.DEFAULT);
        assertNotNull(crpIt);

        var perm = new HashMap<Digest, Boolean>();
        var called = new AtomicInteger();
        var result = crpIt.iterate(0, null, u -> {
            perm.put(u.hash(), true);
            called.incrementAndGet();
            return true;
        });
        assertTrue(result);
        assertEquals(nProc, perm.size());
        assertEquals(nProc, called.get());
    }

    @Test
    public void missingRandomBytesButDeterministicProvided() throws Exception {
        DagAdder d = null;
        try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/10/only_dealing.txt"))) {
            d = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        var rs = new DeterministicRandomSource(new HashMap<>());
        short crpFixedPrefix = 4;
        var crpIt = new CommonRandomPermutation(d.dag(), rs, crpFixedPrefix, DigestAlgorithm.DEFAULT);
        assertNotNull(crpIt);

        var permutation = new ArrayList<Unit>();
        var ok = crpIt.iterate(0, null, u -> {
            permutation.add(u);
            return true;
        });
        assertFalse(ok);
        assertEquals(4, permutation.size());
    }

    @Test
    public void returnDifferentPermutations() throws Exception {
        short nProc = 4;
        DagAdder d = null;
        try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/4/regular.txt"))) {
            d = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        var rs = new RandomSourceMock();
        short crpFixedPrefix = (short) (nProc - 2);
        var crpIt = new CommonRandomPermutation(d.dag(), rs, crpFixedPrefix, DigestAlgorithm.DEFAULT);
        assertNotNull(crpIt);

        checkIfDifferentWithProvidedTimingUnit(d.dag(), crpIt, rs);
        assertTrue(rs.called);
    }

    @Test
    public void returnSamePermutations() throws Exception {
        DagAdder d = null;
        try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/4/regular.txt"))) {
            d = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        var rs = new RandomSourceMock();
        short crpFixedPrefix = 0;
        var crpIt = new CommonRandomPermutation(d.dag(), rs, crpFixedPrefix, DigestAlgorithm.DEFAULT);
        assertNotNull(crpIt);

        checkIfSameWithProvidedTimingUnit(d.dag(), crpIt, rs);
        assertTrue(rs.called);
    }

//    @Test
    public void useDifferentRSproducesDifferentPermutations() throws Exception {
        DagAdder d = null;
        try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/4/regular.txt"))) {
            d = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        Random rand = new Random(0x1638);
        var rsData = new HashMap<Integer, byte[]>();
        for (int level = 0; level < 10; level++) {
            var randData = new byte[64];
            rand.nextBytes(randData);
            rsData.put(level, randData);
        }
        var rs = new DeterministicRandomSource(rsData);

        var crpFixedPrefix = (short) 0;
        var crpIt = new CommonRandomPermutation(d.dag(), rs, crpFixedPrefix, DigestAlgorithm.DEFAULT);
        assertNotNull(crpIt);

        var permutation = new ArrayList<Unit>();
        var perm = new HashMap<Digest, Boolean>();
        crpIt.iterate(0, null, u -> {
            permutation.add(u);
            perm.put(u.hash(), true);
            return true;
        });

        assertTrue(rs.called);

        for (int level = 0; level < 10; level++) {
            for (int ix = 0; ix < 64; ix++) {
                var randData = new byte[64];
                rand.nextBytes(randData);
                rsData.put(level, randData);
            }
        }
        rs = new DeterministicRandomSource(rsData);

        crpIt = new CommonRandomPermutation(d.dag(), rs, crpFixedPrefix, DigestAlgorithm.DEFAULT);
        assertNotNull(crpIt);

        var permutation2 = new ArrayList<Unit>();
        var perm2 = new HashMap<Digest, Boolean>();
        crpIt.iterate(0, null, u -> {
            permutation2.add(u);
            perm2.put(u.hash(), true);
            return true;
        });

        assertTrue(rs.called);
        assertEquals(perm2, perm);
        assertNotEquals(permutation, permutation2);
    }

    @Test
    public void useRandomSourceToDeterminePermutation() throws Exception {
        DagAdder d = null;
        try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/4/regular.txt"))) {
            d = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        var rs = new RandomSourceMock();
        short crpFixedPrefix = 1;
        var crpIt = new CommonRandomPermutation(d.dag(), rs, crpFixedPrefix, DigestAlgorithm.DEFAULT);
        assertNotNull(crpIt);

        AtomicBoolean called = new AtomicBoolean(false);
        var result = crpIt.iterate(0, null, u -> {
            called.set(true);
            return true;
        });

        assertTrue(result);
        assertTrue(rs.called);
    }

    @Test
    public void viewSubsetReturnsDeterministicButNotUnits() throws Exception {
        DagAdder d1 = null;
        try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/10/only_dealing.txt"))) {
            d1 = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        DagAdder d2 = null;
        try (
        FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/10/only_dealing_but_not_all.txt"))) {
            d2 = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        var rs = new RandomSourceMock();
        short crpFixedPrefix = 4;
        var crpIt = new CommonRandomPermutation(d1.dag(), rs, crpFixedPrefix, DigestAlgorithm.DEFAULT);
        assertNotNull(crpIt);

        var permutation = new ArrayList<Unit>();
        var perm = new HashMap<Digest, Boolean>();
        var ok = crpIt.iterate(0, null, u -> {
            permutation.add(u);
            perm.put(u.hash(), true);
            return true;
        });
        assertTrue(ok);

        rs = new RandomSourceMock();
        crpIt = new CommonRandomPermutation(d2.dag(), rs, crpFixedPrefix, DigestAlgorithm.DEFAULT);
        assertNotNull(crpIt);

        var permutation2 = new ArrayList<Unit>();
        var perm2 = new HashMap<Digest, Boolean>();
        ok = crpIt.iterate(0, null, u -> {
            permutation2.add(u);
            perm2.put(u.hash(), true);
            return true;
        });

        var suffix = permutation.subList(crpFixedPrefix, permutation.size());
        var suffix2 = permutation2.subList(crpFixedPrefix - 1, permutation2.size());

        assertEquals(suffix2, suffix);
    }

    private void checkIfDifferentWithProvidedTimingUnit(Dag dag, CommonRandomPermutation crpIt, RandomSourceMock rs) {
        checkWithProvidedTimingUnit(dag, crpIt, rs, false);
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
