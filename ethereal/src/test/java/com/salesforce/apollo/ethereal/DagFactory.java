/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import java.time.Clock;

import com.salesforce.apollo.ethereal.Adder.AdderImpl;
import com.salesforce.apollo.ethereal.Utils.TestAdder;

/**
 * @author hal.hildebrand
 *
 */
public interface DagFactory {
    public class DefaultChecksFactory implements DagFactory {

        @Override
        public DagAdder createDag(short nProc) {
            var cnf = Config.Builder.empty().setnProc(nProc).build();
            var dag = Dag.newDag(cnf, 0);
            dag.addCheck(Checks.basicCorrectness());
            dag.addCheck(Checks.parentConsistency());
            dag.addCheck(Checks.noSelfForkingEvidence());
            dag.addCheck(Checks.forkerMuting());

            return new DagAdder(dag, new AdderImpl(dag, Clock.systemUTC(), cnf.digestAlgorithm()));
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
        public DagAdder createDag(short nProc) {
            var cnf = Config.Builder.empty().setnProc(nProc).build();
            var dag = Dag.newDag(cnf, initialEpoch);
            return new DagAdder(dag, new TestAdder(dag));
        }
    }

    record DagAdder(Dag dag, Adder adder) {}

    DagAdder createDag(short nProc);
}
