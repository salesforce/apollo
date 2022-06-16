/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.linear;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.ethereal.Unit;

/**
 * @author hal.hildebrand
 *
 */
public record TimingRound(Unit currentTU, int level, Unit lastTU) {

    private static final Logger log = LoggerFactory.getLogger(TimingRound.class);

    @Override
    public String toString() {
        return String.format("TimingRound [current: %s, level: %s, lastTU: %s]", currentTU.shortString(), level,
                             lastTU == null ? null : lastTU.shortString());
    }

    /**
     * returns all units ordered in this timing round.
     * 
     * @param digestAlgorithm
     **/
    public List<Unit> orderedUnits(DigestAlgorithm digestAlgorithm, String logLabel) {
        var layers = getAntichainLayers(logLabel);
        var sortedUnits = mergeLayers(layers, digestAlgorithm);
        return sortedUnits;
    }

    /**
     * NOTE we can prove that comparing with last k timing units, where k is the
     * first round for which the deterministic common vote is zero, is enough to
     * verify if a unit was already ordered. Since the common vote for round k is 0,
     * every unit on level tu.Level()+k must be above a timing unit tu, otherwise
     * some unit would decide 0 for it.
     */
    private boolean checkIfAlreadyOrdered(Unit u) {
        if (lastTU == null || u.level() > lastTU.level()) {
            return false;
        }
        if (lastTU != null && lastTU.above(u)) {
            return true;
        }
        return false;
    }

    /**
     * getAntichainLayers for a given timing unit tu, returns all the units in its
     * timing round divided into layers. 0-th layer is formed by minimal units in
     * this timing round. 1-st layer is formed by minimal units when the 0th layer
     * is removed. etc.
     *
     * @param logLabel
     */
    private List<List<Unit>> getAntichainLayers(String logLabel) {
        var unitToLayer = new HashMap<Digest, Integer>();
        var seenUnits = new HashMap<Digest, Boolean>();
        var result = new ArrayList<List<Unit>>();
        traverse(currentTU, unitToLayer, seenUnits, result, logLabel);
        return result;
    }

    private void traverse(Unit u, HashMap<Digest, Integer> unitToLayer, HashMap<Digest, Boolean> seenUnits,
                          ArrayList<List<Unit>> result, String logLabel) {
        seenUnits.put(u.hash(), true);
        log.trace("Traversing: {} parents: {} on: {}", u.shortString(), u.parents(), logLabel);
        var minLayerBelow = -1;
        for (var uParent : u.parents()) {
            if ((uParent == null) || checkIfAlreadyOrdered(uParent)) {
                if (uParent != null) {
                    log.trace("Traversing: {}, already ordered parent: {} on: {}", u.shortString(),
                              uParent.shortString(), logLabel);
                }
                continue;
            }
            if (!seenUnits.getOrDefault(uParent.hash(), false)) {
                traverse(uParent, unitToLayer, seenUnits, result, logLabel);
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
            log.trace("Traversed and added: {}, added layer: {} on: {}", u.shortString(), uLayer, logLabel);
        } else {
            result.get(uLayer).add(u);
            log.trace("Traversed added: {} to layer: {} on: {}", u.shortString(), uLayer, logLabel);
        }
    }

    private List<Unit> mergeLayers(List<List<Unit>> layers, DigestAlgorithm digestAlgorithm) {
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
