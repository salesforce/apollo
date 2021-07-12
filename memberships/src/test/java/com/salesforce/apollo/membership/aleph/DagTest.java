/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.aleph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
public class DagTest {
    // collectUnits runs dfs from maximal units in the given dag and returns a map
    // creator => (height => slice of units by this creator on this height)
    HashMap<Short, Map<Integer, List<Unit>>> collectUnits(Dag dag) {
        var seenUnits = new HashSet<Digest>();
        var result = new HashMap<Short, Map<Integer, List<Unit>>>();
        for (short pid = 0; pid < dag.nProc(); pid++) {
            result.put(pid, new HashMap<>());
        }
        dag.maximalUnitsPerProcess().iterate(units -> {
            for (Unit u : units) {
                if (!seenUnits.add(u.hash())) {
                    traverse(u, seenUnits, result);
                }
            }
            return true;
        });
        return result;
    }

    private void traverse(Unit u, HashSet<Digest> seenUnits, HashMap<Short, Map<Integer, List<Unit>>> result) {
        seenUnits.add(u.hash());
        if (result.get(u.creator()).get(u.height()) != null) {
            result.get(u.creator()).put(u.height(), new ArrayList<>());
        }
        result.get(u.creator()).get(u.height()).add(u);
        for (Unit uParent : u.parents()) {
            if (uParent == null) {
                continue;
            }
            if (!seenUnits.add(uParent.hash())) {
                traverse(u, seenUnits, result);
            }
        }
    }
    
    @Test
    public checkReflexivityOfAbove() {
        
    }
}
