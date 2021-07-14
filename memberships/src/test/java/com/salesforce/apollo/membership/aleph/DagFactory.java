/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.aleph;

import java.time.Clock;

import com.salesforce.apollo.membership.aleph.Adder.AdderImpl;
import com.salesforce.apollo.membership.aleph.Utils.TestAdder;

/**
 * @author hal.hildebrand
 *
 */
public interface DagFactory {
    public class TestDagFactory implements DagFactory {

        @Override
        public DagAdder createDag(short nProc) {
            var cnf = Config.Builder.empty().setnProc(nProc).build();
            var dag = Dag.newDag(cnf, 0);
            return new DagAdder(dag, new TestAdder(dag));
        }
    }

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

    record DagAdder(Dag dag, Adder adder) {}

    DagAdder createDag(short nProc);
}
