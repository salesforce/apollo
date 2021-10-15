/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.ethereal.Dag.Decoded;

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

            return new DagAdder(dag, new DagFactory.TestAdder(dag));
        }
    }

    class TestAdder implements Adder {
        private final Dag dag;

        public TestAdder(Dag dag) {
            this.dag = dag;
        }

        @Override
        public Map<Digest, Correctness> addPreunits(short source, List<PreUnit> preunits) {
            var errors = new HashMap<Digest, Correctness>();
            var hashes = new ArrayList<Digest>();
            var failed = new ArrayList<Boolean>();
            for (var pu : preunits) {
                failed.add(false);
                hashes.add(pu.hash());
            }

            for (int i = 0; i < preunits.size(); i++) {
                var pu = preunits.get(i);
                if (pu.epoch() != dag.epoch()) {
                    errors.put(pu.hash(), Correctness.DATA_ERROR);
                    failed.set(i, true);
                    continue;
                }
                var alreadyInDag = dag.get(pu.hash());
                if (alreadyInDag != null) {
                    errors.put(pu.hash(), Correctness.DUPLICATE_UNIT);
                    failed.set(i, true);
                    continue;
                }
                Decoded decodedParents = dag.decodeParents(pu);
                if (decodedParents.inError()) {
                    errors.put(pu.hash(), decodedParents.classification());
                    failed.set(i, true);
                    continue;
                }
                Unit[] parents = decodedParents.parents();
                var freeUnit = dag.build(pu, parents);
                dag.insert(freeUnit);
            }
            return errors.isEmpty() ? null : errors;
        }

        @Override
        public void close() {
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
            return new DagAdder(dag, new DagFactory.TestAdder(dag));
        }
    }

    record DagAdder(Dag dag, Adder adder) {}

    DagAdder createDag(short nProc);
}
