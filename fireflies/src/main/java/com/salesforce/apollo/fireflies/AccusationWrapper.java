/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import static com.salesforce.apollo.crypto.QualifiedBase64.signature;

import com.salesfoce.apollo.fireflies.proto.SignedAccusation;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;

/**
 * @author hal.hildebrand
 *
 */
public class AccusationWrapper {

    private final SignedAccusation signedAccusation;
    private final Digest           hash;

    public AccusationWrapper(SignedAccusation signedAccusation, DigestAlgorithm algo) {
        this.signedAccusation = signedAccusation;
        this.hash = JohnHancock.from(signedAccusation.getSignature()).toDigest(algo);
    }

    public Digest getAccused() {
        return new Digest(signedAccusation.getAccusation().getAccused());
    }

    public Digest getAccuser() {
        return new Digest(signedAccusation.getAccusation().getAccuser());
    }

    public long getEpoch() {
        return signedAccusation.getAccusation().getEpoch();
    }

    public Digest getHash() {
        return hash;
    }

    public int getRingNumber() {
        return signedAccusation.getAccusation().getRingNumber();
    }

    public JohnHancock getSignature() {
        return signature(signedAccusation.getSignature());
    }

    public SignedAccusation getWrapped() {
        return signedAccusation;
    }
}
