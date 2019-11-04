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

import com.salesforce.apollo.avalanche.WorkingSet.Node;
import com.salesforce.apollo.protocols.HashKey;

/**
 * 
 * @author hhildebrand
 *
 */
public class ConflictSet {
    private Set<Node>     conflicts = ConcurrentHashMap.newKeySet();
    private volatile int  counter   = 0;
    private final HashKey key;
    private volatile Node last;
    private volatile Node preferred;

    public ConflictSet(HashKey key, Node frist) {
        this.key = key;
        last = preferred = frist;
    }

    public void add(Node conflict) {
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

    public Node getLast() {
        final Node current = last;
        return current;
    }

    public Collection<Node> getLosers() {
        conflicts.remove(getPreferred());
        return conflicts;
    }

    public Node getPreferred() {
        final Node current = preferred;
        return current;
    }

    public void prefer(Node node) {
        final Node currentLast = last;
        final Node currentPreferred = preferred;

        last = node;
        if (currentPreferred.getConfidence() < node.getConfidence()) {
            preferred = node;
        }
        if (currentLast == node) {
            counter++;
        }
    }
}
