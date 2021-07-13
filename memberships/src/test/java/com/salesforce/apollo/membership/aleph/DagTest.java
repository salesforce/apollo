/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.aleph;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    public void checkReflexivityOfAbove() throws Exception {
        DagAdder d = null;
        try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/4/one_unit.txt"))) {
            d = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        var units = collectUnits(d.dag());
        var u = units.get((short) 0).get(0).get(0);
        assertNotNull(u);
        assertTrue(u.above(u));
    }

    // collectUnits runs dfs from maximal units in the given dag and returns a map
    // creator => (height => slice of units by this creator on this height)
    private HashMap<Short, Map<Integer, List<Unit>>> collectUnits(Dag dag) {
        var seenUnits = new HashSet<Digest>();
        var result = new HashMap<Short, Map<Integer, List<Unit>>>();
        for (short pid = 0; pid < dag.nProc(); pid++) {
            result.put(pid, new HashMap<>());
        }
        dag.maximalUnitsPerProcess().iterate(units -> {
            for (Unit u : units) {
                if (!seenUnits.contains(u.hash())) {
                    traverse(u, seenUnits, result);
                }
            }
            return true;
        });
        return result;
    }

    private void traverse(Unit u, HashSet<Digest> seenUnits, HashMap<Short, Map<Integer, List<Unit>>> result) {
        seenUnits.add(u.hash());
        result.get(u.creator()).computeIfAbsent(u.height(), k -> new ArrayList<>()).add(u);
        for (Unit uParent : u.parents()) {
            if (uParent == null) {
                continue;
            }
            if (!seenUnits.add(uParent.hash())) {
                traverse(u, seenUnits, result);
            }
        }
    }
}
