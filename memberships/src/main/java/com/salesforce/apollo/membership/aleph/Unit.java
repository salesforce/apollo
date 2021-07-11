/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.aleph;

import static com.salesforce.apollo.membership.aleph.Dag.minimalQuorum;

import java.util.List;

/**
 * @author hal.hildebrand
 *
 */
public interface Unit extends PreUnit {
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

    List<Unit> floor(short slice);

    long level();

    default long levelFromParents(List<Unit> parents) {
        var nProc = (short) parents.size();
        var level = 0L;
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
    default List<Unit> maximalByPid(List<Unit> parents, short pid) {
        if (parents.get(pid) == null) {
            return null;
        }
        var maximal = parents.subList(pid, parents.size());
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

    List<Unit> parents();

    // Return the parent that was created by the same process as the receiver
    default Unit predecessor() {
        return parents().get(creator());
    }
}
