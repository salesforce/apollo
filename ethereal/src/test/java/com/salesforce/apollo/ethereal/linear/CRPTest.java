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

import java.io.File;
import java.io.FileInputStream;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.ethereal.Dag;
import com.salesforce.apollo.ethereal.DagFactory;
import com.salesforce.apollo.ethereal.DagReader;
import com.salesforce.apollo.ethereal.RandomSource;
import com.salesforce.apollo.ethereal.Unit;

/**
 * @author hal.hildebrand
 *
 */
public class CRPTest {

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
        Dag dag = new DagFactory.TestDagFactory().createDag(nProc);
        var crpIt = new CommonRandomPermutation(dag.nProc(), DigestAlgorithm.DEFAULT, "foo");
        assertNotNull(crpIt);

        AtomicBoolean called = new AtomicBoolean(false);

        crpIt.iterate(0, dag.unitsOnLevel(0), null, u -> {
            called.set(true);
            return true;
        });
        assertFalse(called.get());
    }

    @Test
    public void enoughUnitsReturnAllUnits() throws Exception {
        short nProc = 4;
        Dag d = null;
        try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/4/regular.txt"))) {
            d = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        var crpIt = new CommonRandomPermutation(d.nProc(), DigestAlgorithm.DEFAULT, "foo");
        assertNotNull(crpIt);

        var perm = new HashMap<Digest, Boolean>();
        var called = new AtomicInteger();
        crpIt.iterate(0, d.unitsOnLevel(0), null, u -> {
            perm.put(u.hash(), true);
            called.incrementAndGet();
            return true;
        });
        assertEquals(nProc, perm.size());
        assertEquals(nProc, called.get());
    }

//    @Test
    public void useDifferentRSproducesDifferentPermutations() throws Exception {
        Dag d = null;
        try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/4/regular.txt"))) {
            d = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        Random rand = new SecureRandom();
        var rsData = new HashMap<Integer, byte[]>();
        for (int level = 0; level < 10; level++) {
            var randData = new byte[64];
            rand.nextBytes(randData);
            rsData.put(level, randData);
        }

        var crpIt = new CommonRandomPermutation(d.nProc(), DigestAlgorithm.DEFAULT, "foo");
        assertNotNull(crpIt);

        var permutation = new ArrayList<Unit>();
        var perm = new HashMap<Digest, Boolean>();
        crpIt.iterate(0, d.unitsOnLevel(0), null, u -> {
            permutation.add(u);
            perm.put(u.hash(), true);
            return true;
        });

        for (int level = 0; level < 10; level++) {
            var randData = new byte[64];
            rand.nextBytes(randData);
            rsData.put(level, randData);
        }

        crpIt = new CommonRandomPermutation(d.nProc(), DigestAlgorithm.DEFAULT, "foo");
        assertNotNull(crpIt);

        var permutation2 = new ArrayList<Unit>();
        var perm2 = new HashMap<Digest, Boolean>();
        crpIt.iterate(0, d.unitsOnLevel(0), null, u -> {
            permutation2.add(u);
            perm2.put(u.hash(), true);
            return true;
        });

        assertEquals(perm2, perm);
        assertNotEquals(permutation, permutation2);
    }

    @Test
    public void useRandomSourceToDeterminePermutation() throws Exception {
        Dag d = null;
        try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/4/regular.txt"))) {
            d = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        var crpIt = new CommonRandomPermutation(d.nProc(), DigestAlgorithm.DEFAULT, "foo");
        assertNotNull(crpIt);

        AtomicBoolean called = new AtomicBoolean(false);
        crpIt.iterate(0, d.unitsOnLevel(0), null, u -> {
            called.set(true);
            return true;
        });
    }
}
