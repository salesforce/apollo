/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.avalanche;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.salesforce.apollo.avalanche.WorkingSet.KnownNode;
import com.salesforce.apollo.avalanche.WorkingSet.Node;
import com.salesforce.apollo.protocols.HashKey;

/**
 * 
 * @author hhildebrand
 *
 */
public class ConflictSet {
    private Set<KnownNode>     conflicts = ConcurrentHashMap.newKeySet();
    private volatile int  counter   = 0;
    private final HashKey key;
    private volatile KnownNode last;
    private volatile KnownNode preferred;

    public ConflictSet(HashKey key, KnownNode frist) {
        this.key = key;
        last = preferred = frist;
    }

    public void add(KnownNode conflict) {
        if (!conflicts.isEmpty()) {
            boolean f = false;
            for (Node node : conflicts) {
                if (node.isFinalized()) {
                    f = true;
                }
            }
            System.out.println("Dup detected, previous finalized: " + f);
        }
        conflicts.add(conflict);
    }

    public void clearCounter() {
        counter = 0;
    }

    public int getCardinality() {
        return conflicts.size();
    }

    public int getCounter() {
        final int current = counter;
        return current;
    }

    public HashKey getKey() {
        return key;
    }

    public KnownNode getLast() {
        final KnownNode current = last;
        return current;
    }

    public Collection<KnownNode> getLosers() {
        conflicts.remove(getPreferred());
        return conflicts;
    }

    public Node getPreferred() {
        final Node current = preferred;
        return current;
    }

    public void prefer(KnownNode node) {
        final KnownNode currentLast = last;
        final KnownNode currentPreferred = preferred;

        last = node;
        if (currentPreferred.getConfidence() < node.getConfidence()) {
            preferred = node;
        }
        if (currentLast == node) {
            counter++;
        }
    }
}
