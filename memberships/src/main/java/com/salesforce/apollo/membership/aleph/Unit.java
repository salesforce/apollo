/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.aleph;

import static com.salesforce.apollo.membership.aleph.Dag.minimalQuorum;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.protobuf.Any;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;

/**
 * @author hal.hildebrand
 *
 */
public interface Unit extends PreUnit {

    record unitInDag(Unit unit, int forkingHeight) implements Unit {

        @Override
        public int hashCode() {
            return unit.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof unitInDag uid) {
                if (unit.equals(uid)) {
                    return true;
                }
                return forkingHeight == uid.forkingHeight;
            }
            return unit.equals(obj);
        }

        @Override
        public short creator() {
            return unit.creator();
        }

        @Override
        public Any data() {
            return unit.data();
        }

        @Override
        public int epoch() {
            return unit.epoch();
        }

        @Override
        public Digest hash() {
            return unit.hash();
        }

        @Override
        public int height() {
            return unit.height();
        }

        @Override
        public byte[] randomSourceData() {
            return unit.randomSourceData();
        }

        @Override
        public JohnHancock signature() {
            return unit.signature();
        }

        @Override
        public Crown view() {
            return unit.view();
        }

        @Override
        public boolean aboveWithinProc(Unit v) {
            if (unit.height() < v.height() || unit.creator() != v.creator()) {
                return false;
            }
            if (v instanceof unitInDag uid) {
                if (v.height() < commonForkHeight(uid)) {
                    return true;
                }
            }
            // Either we have a fork or a different type of unit, either way no optimization
            // is possible.
            return unit.aboveWithinProc(v);

        }

        int commonForkHeight(unitInDag v) {
            if (forkingHeight < v.forkingHeight) {
                return forkingHeight;
            }
            return v.forkingHeight;
        }

        @Override
        public List<Unit> floor(short slice) {
            return unit.floor(slice);
        }

        @Override
        public int level() {
            return unit.level();
        }

        @Override
        public List<Unit> parents() {
            return unit.parents();
        }
    }

    static int levelFromParents(List<Unit> parents) {
        var nProc = (short) parents.size();
        var level = 0;
        var onLevel = (short) 0;
        for (Unit p : parents) {
            if (p == null) {
                continue;
            }
            if (p.level() == level) {
                onLevel++;
            } else if (p.level() > level) {
                onLevel = 1;
                level = p.level();
            }
        }
        if (onLevel >= minimalQuorum(nProc)) {
            level++;
        }

        return level;
    }

    // Computes all maximal units produced by a pid present in parents and their
    // floors
    static List<Unit> maximalByPid(List<Unit> parents, short pid) {
        if (parents.get(pid) == null) {
            return Collections.emptyList();
        }
        var maximal = new ArrayList<Unit>();
        maximal.add(parents.get(pid));
        for (Unit parent : parents) {
            if (parent == null) {
                continue;
            }
            for (Unit w : parent.floor(pid)) {
                var found = false;
                var ri = -1;
                for (int ix = 0; ix < maximal.size(); ix++) {
                    var v = maximal.get(ix);
                    if (w.above(v)) {
                        found = true;
                        ri = ix;
                        // Break because if we find any other index for storing w, it would proof
                        // of self forking
                        break;
                    }
                    if (v.above(w)) {
                        found = true;
                        // Break because if w would be > some other index, it would contradict the
                        // ssumption that elements of floors() are not comparable
                        break;
                    }
                }
                if (!found) {
                    maximal.add(w);
                } else if (ri >= 0) {
                    maximal.set(ri, w);
                }
            }
        }

        return maximal;
    }

    // Is the receiver above the specified unit?
    default boolean above(Unit v) {
        if (v == null) {
            return false;
        }
        if (equals(v)) {
            return true;
        }

        for (Unit w : floor(v.creator())) {
            if (w.aboveWithinProc(v)) {
                return true;
            }
        }
        return false;
    }

    boolean aboveWithinProc(Unit unit);

    // checks whether the receiver is below any of the specified units
    default boolean belowAny(List<Unit> units) {
        for (Unit v : units) {
            if (v != null && v.above(this)) {
                return true;
            }
        }
        return false;
    }

    // this implementation works as long as there is no race for writing/reading to
    // dag.maxUnits, i.e. as long as units created by one process are added
    // atomically
    default int computeForkingHeight(Dag dag) {
        if (dealing()) {
            if (dag.maximalUnitsPerProcess().get(creator()).size() > 0) {
                return -1;
            } else {
                return Integer.MAX_VALUE;
            }
        }
        unitInDag predecessor = (unitInDag) predecessor();
        if (predecessor != null) {
            var found = false;
            for (Unit v : dag.maximalUnitsPerProcess().get(creator())) {
                if (v.equals(predecessor)) {
                    found = true;
                    break;
                }
            }
            if (found) {
                return predecessor.forkingHeight;
            } else {
                // there is already a unit that has 'predecessor' as a predecessor, hence u is a
                // fork
                if (predecessor.forkingHeight < predecessor.height()) {
                    return predecessor.forkingHeight;
                } else {
                    return predecessor.height();
                }
            }
        }
        return 0;
    }

    default Unit embed(Dag dag) {
        return new unitInDag(this, computeForkingHeight(dag));
    }

    List<Unit> floor(short slice);

    int level();

    List<Unit> parents();

    // Return the parent that was created by the same process as the receiver
    default Unit predecessor() {
        return parents().get(creator());
    }
}
