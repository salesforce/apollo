/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.avalanche;

import com.salesforce.apollo.avalanche.WorkingSet.Node;

/**
 * 
 * @author hhildebrand
 *
 */
public class ConflictSet {
    private volatile int  cardinality = 0;
    private volatile int  counter     = 0;
    private volatile Node last;
    private volatile Node preferred;

    public ConflictSet(Node frist) {
        last = preferred = frist;
        cardinality = 1;
    }

    public void clearCounter() {
        counter = 0;
    }

    public int getCardinality() {
        final int current = cardinality;
        return current;
    }

    public int getCounter() {
        final int current = counter;
        return current;
    }

    public Node getLast() {
        final Node current = last;
        return current;
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
        if (currentLast.equals(node)) {
            counter++;
        }
    }
}
