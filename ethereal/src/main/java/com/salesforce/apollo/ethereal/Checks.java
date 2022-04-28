/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;

/**
 * @author hal.hildebrand
 *
 */
public interface Checks {

    static final List<BiFunction<Unit, Dag, Correctness>> ConsensusChecks                 = Arrays.asList(basicCorrectness(),
                                                                                                          parentConsistency(),
                                                                                                          noSelfForkingEvidence(),
                                                                                                          forkerMuting());
    static final int                                      MaxDtaBytesPerUnit              = (int) 2e6;
    static final int                                      MaxRandomSourceDataBytesPerUnit = (int) 1e6;
    static final int                                      MaxUnitsInChunk                 = (int) 1e6;
    static final List<BiFunction<Unit, Dag, Correctness>> SetupChecks                     = Arrays.asList(basicCorrectness(),
                                                                                                          parentConsistency(),
                                                                                                          noLevelSkipping(),
                                                                                                          noForks());

    static BiFunction<Unit, Dag, Correctness> basicCorrectness() {
        return (u, dag) -> {
            var parents = u.parents();
            var nproc = dag.nProc();
            if (parents.length != nproc) {
                return Correctness.COMPLIANCE_ERROR;
            }
            short nonNillParents = 0;
            for (short i = 0; i < nproc; i++) {
                if (parents[i] == null) {
                    continue;
                }
                nonNillParents++;
                if (parents[i].creator() != i) {
                    return Correctness.COMPLIANCE_ERROR;
                }
            }
            if (u.predecessor() == null && nonNillParents > 0) {
                return Correctness.COMPLIANCE_ERROR;
            }
            if (u.predecessor() != null && u.predecessor().level() >= u.level()) {
                return Correctness.COMPLIANCE_ERROR;
            }
            return null;
        };
    }

    static BiFunction<Unit, Dag, Correctness> forkerMuting() {
        return (u, dag) -> null;
    }

    static BiFunction<Unit, Dag, Correctness> noForks() {
        return (u, dag) -> null;
    }

    static BiFunction<Unit, Dag, Correctness> noLevelSkipping() {
        return (u, dag) -> null;
    }

    static BiFunction<Unit, Dag, Correctness> noSelfForkingEvidence() {
        return (u, dag) -> null;
    }

    static BiFunction<Unit, Dag, Correctness> parentConsistency() {
        return (unit, dag) -> {
            var parents = unit.parents();
            var nproc = dag.nProc();
            for (short i = 0; i < nproc; i++) {
                for (short j = 0; j < nproc; j++) {
                    if (parents[j] == null) {
                        continue;
                    }
                    var u = parents[j].parents()[i];
                    if (u != null && (parents[i] == null || parents[i].level() < u.level())) {
                        return Correctness.COMPLIANCE_ERROR;
                    }
                }
            }
            return null;
        };
    }
}
