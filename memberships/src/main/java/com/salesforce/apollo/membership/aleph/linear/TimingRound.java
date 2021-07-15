/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.aleph.linear;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.aleph.Unit;

/**
 * @author hal.hildebrand
 *
 */
public record TimingRound(Unit currentTU, List<Unit> lastTUs, DigestAlgorithm digestAlgorithm) {
    // returns all units ordered in this timing round.
    public List<Unit> orderedUnits() {
        var layers = getAntichainLayers(currentTU, lastTUs);
        var sortedUnits = mergeLayers(layers);
        return sortedUnits;
    }

    // NOTE we can prove that comparing with last k timing units, where k is the
    // first round for which the deterministic common vote is zero, is enough to
    // verify if a unit was already ordered. Since the common vote for round k is 0,
    // every unit on level tu.Level()+k must be above a timing unit tu, otherwise
    // some unit would decide 0 for it.
    private boolean checkIfAlreadyOrdered(Unit u, List<Unit> prevTUs) {
        var prevTU = prevTUs.get(prevTUs.size() - 1);
        if (prevTU == null || u.level() > prevTU.level()) {
            return false;
        }
        for (var it = prevTUs.size() - 1; it >= 0; it--) {
            if (prevTUs.get(it).above(u)) {
                return true;
            }
        }
        return false;
    }

    // getAntichainLayers for a given timing unit tu, returns all the units in its
    // timing round divided into layers.
    // 0-th layer is formed by minimal units in this timing round.
    // 1-st layer is formed by minimal units when the 0th layer is removed.
    // etc.
    private List<List<Unit>> getAntichainLayers(Unit tu, List<Unit> prevTUs) {
        var unitToLayer = new HashMap<Digest, Integer>();
        var seenUnits = new HashMap<Digest, Boolean>();
        var result = new ArrayList<List<Unit>>();
        traverse(tu, prevTUs, unitToLayer, seenUnits, result);
        return result;
    }

    private void traverse(Unit u, List<Unit> prevTUs, HashMap<Digest, Integer> unitToLayer,
                          HashMap<Digest, Boolean> seenUnits, ArrayList<List<Unit>> result) {
        seenUnits.put(u.hash(), true);
        var minLayerBelow = -1;
        for (var uParent : u.parents()) {
            if (uParent == null) {
                continue;
            }
            if (checkIfAlreadyOrdered(uParent, prevTUs)) {
                continue;
            }
            if (!seenUnits.get(uParent.hash())) {
                traverse(uParent, prevTUs, unitToLayer, seenUnits, result);
            }
            if (unitToLayer.get(uParent.hash()) > minLayerBelow) {
                minLayerBelow = unitToLayer.get(uParent.hash());
            }
        }
        var uLayer = minLayerBelow + 1;
        unitToLayer.put(u.hash(), uLayer);
        if (result.size() <= uLayer) {
            var l = new ArrayList<Unit>();
            l.add(u);
            result.add(l);
        } else {
            result.get(uLayer).add(u);
        }
    }

    private List<Unit> mergeLayers(List<List<Unit>> layers) {
        Digest totalXOR = digestAlgorithm.getOrigin();
        for (int i = 0; i < layers.size(); i++) {
            for (var u : layers.get(i)) {
                totalXOR = totalXOR.xor(u.hash());
            }
        }
        // tiebreaker is a map from units to its tiebreaker value
        var tiebreaker = new HashMap<Digest, Digest>();
        for (int l = 0; l < layers.size(); l++) {
            for (var u : layers.get(l)) {
                tiebreaker.put(u.hash(), totalXOR.xor(u.hash()));
            }
        }

        List<Unit> sortedUnits = new ArrayList<Unit>();

        for (int l = 0; l < layers.size(); l++) {
            layers.get(l).sort((a, b) -> tiebreaker.get(a.hash()).compareTo(tiebreaker.get(b.hash())));
            sortedUnits.addAll(layers.get(l));
        }
        return sortedUnits;
    }

}
