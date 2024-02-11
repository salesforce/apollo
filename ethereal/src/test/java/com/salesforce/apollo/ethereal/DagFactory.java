/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import com.salesforce.apollo.ethereal.Dag.DagImpl;

import java.util.function.BiFunction;

/**
 * @author hal.hildebrand
 */
public interface DagFactory {
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

    Dag createDag(short nProc);

    class DefaultChecksFactory implements DagFactory {

        @Override
        public Dag createDag(short nProc) {
            var cnf = Config.newBuilder().setnProc(nProc).build();
            var dag = new DagImpl(cnf, 0);
            dag.addCheck(basicCorrectness());
            dag.addCheck(parentConsistency());
            return dag;
        }
    }

    class TestDagFactory implements DagFactory {

        private final int initialEpoch;

        public TestDagFactory() {
            this(0);
        }

        public TestDagFactory(int initialEpoch) {
            this.initialEpoch = initialEpoch;
        }

        @Override
        public Dag createDag(short nProc) {
            var cnf = Config.newBuilder().setnProc(nProc).build();
            return new DagImpl(cnf, initialEpoch);
        }
    }
}
