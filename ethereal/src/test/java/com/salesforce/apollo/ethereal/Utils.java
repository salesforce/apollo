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
public interface Utils {
    public static class TestAdder implements Adder {
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
}
