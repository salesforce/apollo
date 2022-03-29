/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.random.beacon;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.ethereal.Dag;
import com.salesforce.apollo.ethereal.RandomSource;
import com.salesforce.apollo.ethereal.Unit;

/**
 * @author hal.hildebrand
 *
 */
public class DeterministicRandomSource implements RandomSource {
    final DigestAlgorithm algo;

    public DeterministicRandomSource(DigestAlgorithm algo) {
        this.algo = algo;
    }

    public static class DsrFactory implements RandomSourceFactory {
        public DsrFactory(DigestAlgorithm algo) {
            this.algo = algo;
        }

        final DigestAlgorithm algo;

        @Override
        public byte[] dealingData(int epoch) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public RandomSource newRandomSource(Dag dag) {
            return new DeterministicRandomSource(algo);
        }

    }

    @Override
    public byte[] dataToInclude(Unit[] parents, int level) {
        return null;
    }

    @Override
    public byte[] randomBytes(short process, int level) {
        return algo.getOrigin().prefix(process).prefix(level).getBytes();
    }
}
