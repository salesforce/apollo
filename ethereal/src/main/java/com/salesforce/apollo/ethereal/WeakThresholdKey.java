/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.salesfoce.apollo.ethereal.proto.EpochProof;
import com.salesfoce.apollo.ethereal.proto.Proof;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.ethereal.creator.EpochProofBuilder.DecodedShare;
import com.salesforce.apollo.ethereal.creator.EpochProofBuilder.Share;

/**
 * @author hal.hildebrand
 *
 */
public interface WeakThresholdKey {

    class NoOpWeakThresholdKey implements WeakThresholdKey {
        private final Map<Short, Boolean> shareProviders = new HashMap<>();
        private final int                 threshold;

        public NoOpWeakThresholdKey(int threshold) {
            this.threshold = threshold;
        }

        @Override
        public JohnHancock combineShares(Collection<Share> shareSlice) {
            return shareSlice.size() >= threshold ? new JohnHancock(SignatureAlgorithm.DEFAULT, new byte[0]) : null;
        }

        @Override
        public Share createShare(Proof proof, short pid) {
            return new Share(pid, new JohnHancock(SignatureAlgorithm.DEFAULT, new byte[0]));
        }

        @Override
        public Map<Short, Boolean> shareProviders() {
            return shareProviders;
        }

        @Override
        public int threshold() {
            return threshold;
        }

        @Override
        public boolean verifyShare(DecodedShare share) {
            return true;
        }

        @Override
        public boolean verifySignature(EpochProof decoded) {
            return true;
        }
    }

    static WeakThresholdKey seededWTK(short nProc, short pid, int i, Object object) {
        return null;
    }

    JohnHancock combineShares(Collection<Share> shareSlice);

    Share createShare(Proof proof, short pid);

    Map<Short, Boolean> shareProviders();

    int threshold();

    boolean verifyShare(DecodedShare share);

    boolean verifySignature(EpochProof decoded);

}
