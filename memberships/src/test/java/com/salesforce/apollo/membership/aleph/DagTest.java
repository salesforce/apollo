/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.aleph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.aleph.DagFactory.DagAdder;

/**
 * @author hal.hildebrand
 *
 */
public class DagTest {
    //@Test // TODO, resolution of forking (with alerts or RBAC)
    public void aboveWorkingFoTwoForksFromOneUnit() throws Exception {
        DagAdder d = null;
        try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/10/fork_4u.txt"))) {
            d = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        var units = collectUnits(d.dag());
        var uBase = units.get((short) 0).get(0).get(0);
        var u1 = units.get((short) 0).get(1).get(0);
        var u2 = units.get((short) 0).get(1).get(1);

        assertTrue(u1.above(uBase));
        assertTrue(u2.above(uBase));
        assertTrue(uBase.above(u1));
        assertTrue(uBase.above(u2));
        assertTrue(u1.above(u2));
        assertTrue(u2.above(u1));
    }

    // @Test TODO, resolution of forking (with alerts or RBAC)
    public void correctForkedDealingUnits() throws Exception {
        DagAdder d = null;
        try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/10/forked_dealing.txt"))) {
            d = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        var units = collectUnits(d.dag());
        var u0 = units.get((short) 0).get(0).get(0);
        var u1 = units.get((short) 0).get(0).get(1);

        assertNotNull(u0);
        assertNotNull(u1);

        assertTrue(u0.above(u1));
        assertTrue(u1.above(u0));
    }

    @Test
    public void floorsOnDealing() throws Exception {
        DagAdder d = null;
        try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/4/only_dealing.txt"))) {
            d = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        var units = collectUnits(d.dag());
        for (short pid = 0; pid < d.dag().nProc(); pid++) {
            for (short pid2 = 0; pid2 < d.dag().nProc(); pid2++) {
                var myFloor = units.get(pid).get(0).get(0).floor(pid2);
                assertEquals(0, myFloor.size());
            }
        }
    }

    @Test
    public void floorsOnSeeingFork() throws Exception {
        try {
            try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/10/fork_accepted.txt"))) {
                DagReader.readDag(fis, new DagFactory.TestDagFactory());
            }
            fail("Should not have succeeded due to fork");
        } catch (IllegalStateException e) {
            assertEquals("Trying to set parent to non-existing unit", e.getMessage());
        }
    }

    @Test
    public void floorsOnSingleUwithTwoParents() throws Exception {
        DagAdder d = null;
        try (
        FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/10/single_unit_with_two_parents.txt"))) {
            d = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        var units = collectUnits(d.dag());

        var floor0 = units.get((short) 0).get(1).get(0).floor((short) 0);
        var floor1 = units.get((short) 0).get(1).get(0).floor((short) 1);
        assertEquals(1, floor0.size());
        assertEquals(units.get((short) 0).get(0).get(0), floor0.get(0));
        assertEquals(1, floor1.size());
        assertEquals(units.get((short) 1).get(0).get(0), floor1.get(0));
    }

    @Test
    public void lackOfSymmetryOfAbove() throws Exception {
        DagAdder d = null;
        try (
        FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/10/single_unit_with_two_parents.txt"))) {
            d = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        var units = collectUnits(d.dag());
        var u0 = units.get((short) 0).get(0).get(0);
        var u1 = units.get((short) 1).get(0).get(0);
        var u01 = units.get((short) 0).get(1).get(0);
        assertNotNull(u0);
        assertNotNull(u1);
        assertNotNull(u01);

        assertTrue(u01.above(u0));
        assertTrue(u01.above(u1));
        assertFalse(u0.above(u01));
        assertFalse(u1.above(u01));
    }

    @Test
    public void reflexivityOfAbove() throws Exception {
        DagAdder d = null;
        try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/10/only_dealing.txt"))) {
            d = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        var units = collectUnits(d.dag());
        var u = units.get((short) 0).get(0).get(0);
        assertNotNull(u);
        assertTrue(u.above(u));
    }

    @Test
    public void transitivityOfAbove() throws Exception {
        DagAdder d = null;
        try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/10/six_units.txt"))) {
            d = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        var units = collectUnits(d.dag());
        var u0 = units.get((short) 0).get(0).get(0);
        var u01 = units.get((short) 0).get(1).get(0);
        var u02 = units.get((short) 0).get(2).get(0);
        var u21 = units.get((short) 2).get(1).get(0);
        assertNotNull(u0);
        assertNotNull(u01);
        assertNotNull(u02);
        assertNotNull(u21);

        assertTrue(u01.above(u0));
        assertTrue(u02.above(u01));
        assertTrue(u02.above(u0));
        assertTrue(u21.above(u01));
        assertTrue(u21.above(u0));
    }

    // collectUnits runs dfs from maximal units in the given dag and returns a map
    // creator => (height => slice of units by this creator on this height)
    private HashMap<Short, Map<Integer, List<Unit>>> collectUnits(Dag dag) {
        var traversed = new HashSet<Digest>();
        var result = new HashMap<Short, Map<Integer, List<Unit>>>();
        for (short pid = 0; pid < dag.nProc(); pid++) {
            result.put(pid, new HashMap<>());
        }
        dag.maximalUnitsPerProcess().iterate(units -> {
            for (Unit u : units) {
                if (!traversed.contains(u.hash())) {
                    traverse(u, traversed, result);
                }
            }
            return true;
        });
        return result;
    }

    private void traverse(Unit u, HashSet<Digest> traversed, HashMap<Short, Map<Integer, List<Unit>>> result) {
        traversed.add(u.hash());
        result.get(u.creator()).computeIfAbsent(u.height(), k -> new ArrayList<>()).add(u);
        for (Unit uParent : u.parents()) {
            if (uParent == null) {
                continue;
            }
            if (!traversed.contains(uParent.hash())) {
                traverse(uParent, traversed, result);
            }
        }
    }
}
