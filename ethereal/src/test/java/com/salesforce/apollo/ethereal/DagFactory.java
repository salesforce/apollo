/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import com.salesforce.apollo.ethereal.Dag.DagImpl;

/**
 * @author hal.hildebrand
 *
 */
public interface DagFactory {
    public class DefaultChecksFactory implements DagFactory {

        @Override
        public Dag createDag(short nProc) {
            var cnf = Config.newBuilder().setnProc(nProc).build();
            var dag = new DagImpl(cnf, 0);
            dag.addCheck(Checks.basicCorrectness());
            dag.addCheck(Checks.parentConsistency());
            dag.addCheck(Checks.noSelfForkingEvidence());
            dag.addCheck(Checks.forkerMuting());

            return dag;
        }
    }

    public class TestDagFactory implements DagFactory {

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

    Dag createDag(short nProc);
}
