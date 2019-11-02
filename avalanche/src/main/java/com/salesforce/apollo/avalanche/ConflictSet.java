/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.avalanche;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.salesforce.apollo.avalanche.WorkingSet.Node;

/**
 * 
 * @author hhildebrand
 *
 */
public class ConflictSet {
    private final AtomicInteger         cardinality = new AtomicInteger(1);
    private final AtomicInteger         counter     = new AtomicInteger(0);
    private final AtomicReference<Node> last        = new AtomicReference<>();
    private final AtomicReference<Node> preferred   = new AtomicReference<>();

    public ConflictSet(Node frist) {
        last.set(frist);
        preferred.set(frist);
    }

    public void clearCounter() {
        counter.set(0);
    }

    public int getCardinality() {
        return cardinality.get();
    }

    public int getCounter() {
        return counter.get();
    }

    public Node getLast() {
        return last.get();
    }

    public Node getPreferred() {
        return preferred.get();
    }

    public void incrementCardinality() {
        cardinality.incrementAndGet();
    }

    public void incrementCounter() {
        counter.incrementAndGet();
    }

    public int prefer(Node node) {
        last.set(node);
        preferred.set(node);
        return counter.incrementAndGet();
    }

    public void setLast(Node n) {
        last.set(n);
    }

    public void setPreferred(Node n) {
        preferred.set(n);
    }
}
