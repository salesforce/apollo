/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.context.Context;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.JohnHancock;
import com.salesforce.apollo.cryptography.Verifier;
import com.salesforce.apollo.ethereal.proto.PreUnit_s;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author hal.hildebrand
 */
public interface Unit extends PreUnit {

    static int levelFromParents(Unit[] parents, double bias) {
        var nProc = (short) parents.length;
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
        if (onLevel >= Context.minimalQuorum(nProc, bias)) {
            level++;
        }

        return level;
    }

    /**
     * Computes all maximal units produced by a pid present in parents and their floors
     */
    static Unit[] maximalByPid(Unit[] parents, short pid) {
        if (parents[pid] == null) {
            return new Unit[0];
        }
        var maximal = new ArrayList<Unit>();
        maximal.add(parents[pid]);
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

        return maximal.toArray(new Unit[maximal.size()]);
    }

    static List<Unit> topologicalSort(List<Unit> units) {
        List<Unit> result = new ArrayList<>();
        return buildReverseDfsOrder(units, result);
    }

    private static List<Unit> buildReverseDfsOrder(List<Unit> units, List<Unit> result) {
        var notVisited = new HashMap<Digest, Boolean>();
        for (var unit : units) {
            notVisited.put(unit.hash(), true);
        }
        for (var unit : units) {
            result = reverseDfsOrder(unit, notVisited, result);
        }
        return result;
    }

    private static List<Unit> reverseDfsOrder(Unit unit, HashMap<Digest, Boolean> notVisited, List<Unit> result) {
        if (notVisited.put(unit.hash(), false)) {
            for (var parent : unit.parents()) {
                result = reverseDfsOrder(parent, notVisited, result);
            }
            result.add(unit);
        }
        return result;
    }

    /** Is the receiver above the specified unit? */
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

    /** checks whether the receiver is below any of the specified units */
    default boolean belowAny(List<Unit> units) {
        for (Unit v : units) {
            if (v != null && v.above(this)) {
                return true;
            }
        }
        return false;
    }

    /**
     * this implementation works as long as there is no race for writing/reading to dag.maxUnits, i.e. as long as units
     * created by one process are added atomically
     */
    default int computeForkingHeight(Dag dag) {
        if (dealing()) {
            if (dag.maximalUnitsPerProcess().get(creator()) != null) {
                return -1;
            } else {
                return Integer.MAX_VALUE;
            }
        }
        var u = predecessor();
        if (u instanceof unitInDag predecessor) {
            var found = false;
            Unit v = dag.maximalUnitsPerProcess().get(creator());
            if (predecessor.equals(v)) {
                found = true;
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
        assert this.parents().length == dag.nProc();
        return new unitInDag(this, computeForkingHeight(dag));
    }

    Unit[] floor(short slice);

    int level();

    Unit[] parents();

    /** Return the parent that was created by the same process as the receiver */
    default Unit predecessor() {
        return parents()[creator()];
    }

    record unitInDag(Unit unit, int forkingHeight) implements Unit {

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

        @Override
        public short creator() {
            return unit.creator();
        }

        @Override
        public ByteString data() {
            return unit.data();
        }

        @Override
        public int epoch() {
            return unit.epoch();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof Unit uid) {
                return hash().equals(uid.hash());
            }
            return false;
        }

        @Override
        public Unit[] floor(short slice) {
            return unit.floor(slice);
        }

        @Override
        public Digest hash() {
            return unit.hash();
        }

        @Override
        public int hashCode() {
            return unit.hashCode();
        }

        @Override
        public int height() {
            return unit.height();
        }

        @Override
        public int level() {
            return unit.level();
        }

        @Override
        public Unit[] parents() {
            return unit.parents();
        }

        @Override
        public String shortString() {
            return creator() + ":" + level() + ":" + epoch();
        }

        @Override
        public JohnHancock signature() {
            return unit.signature();
        }

        @Override
        public PreUnit toPreUnit() {
            return unit.toPreUnit();
        }

        @Override
        public PreUnit_s toPreUnit_s() {
            return unit.toPreUnit_s();
        }

        @Override
        public String toString() {
            return "uid[" + shortString() + "]";
        }

        @Override
        public boolean verify(Verifier[] verifiers) {
            return unit.verify(verifiers);
        }

        @Override
        public Crown view() {
            return unit.view();
        }

        int commonForkHeight(unitInDag v) {
            if (forkingHeight < v.forkingHeight) {
                return forkingHeight;
            }
            return v.forkingHeight;
        }
    }
}
