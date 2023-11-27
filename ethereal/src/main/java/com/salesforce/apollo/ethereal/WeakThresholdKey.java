/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import com.salesfoce.apollo.ethereal.proto.EpochProof;
import com.salesfoce.apollo.ethereal.proto.Proof;
import com.salesforce.apollo.cryptography.JohnHancock;
import com.salesforce.apollo.cryptography.SignatureAlgorithm;
import com.salesforce.apollo.ethereal.EpochProofBuilder.DecodedShare;
import com.salesforce.apollo.ethereal.EpochProofBuilder.Share;
import org.joou.ULong;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author hal.hildebrand
 */
public interface WeakThresholdKey {

    static WeakThresholdKey seededWTK(short nProc, short pid, int i, Object object) {
        return null;
    }

    JohnHancock combineShares(Collection<Share> shareSlice);

    Share createShare(Proof proof, short pid);

    Map<Short, Boolean> shareProviders();

    int threshold();

    boolean verifyShare(DecodedShare share);

    boolean verifySignature(EpochProof decoded);

    class NoOpWeakThresholdKey implements WeakThresholdKey {
        private final Map<Short, Boolean> shareProviders = new HashMap<>();
        private final int                 threshold;

        public NoOpWeakThresholdKey(int threshold) {
            this.threshold = threshold;
        }

        @Override
        public JohnHancock combineShares(Collection<Share> shareSlice) {
            return shareSlice.size() >= threshold ? new JohnHancock(SignatureAlgorithm.DEFAULT, new byte[0], ULong.MIN)
                                                  : null;
        }

        @Override
        public Share createShare(Proof proof, short pid) {
            return new Share(pid, new JohnHancock(SignatureAlgorithm.DEFAULT, new byte[0], ULong.MIN));
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

}
