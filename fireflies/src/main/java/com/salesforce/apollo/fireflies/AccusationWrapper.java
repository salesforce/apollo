/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import static com.salesforce.apollo.crypto.QualifiedBase64.signature;

import java.nio.ByteBuffer;

import com.salesfoce.apollo.fireflies.proto.Accusation;
import com.salesfoce.apollo.fireflies.proto.AccusationOrBuilder;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;

/**
 * @author hal.hildebrand
 *
 */
public class AccusationWrapper {

    public static ByteBuffer forSigning(AccusationOrBuilder accuse) {
        byte[] accuser = accuse.getAccuser().toByteArray();
        byte[] accused = accuse.getAccused().toByteArray();
        ByteBuffer accusation = ByteBuffer.allocate(8 + 4 + accuser.length + accused.length);
        accusation.putLong(accuse.getEpoch()).putInt(accuse.getRingNumber()).put(accuser).put(accused);
        accusation.flip();
        return accusation;
    }

    private final Accusation accusation;
    private final Digest     hash;

    public AccusationWrapper(Digest hash, Accusation accusation) {
        this.accusation = accusation;
        this.hash = hash;
    }

    public Digest getAccused() {
        return new Digest(accusation.getAccused());
    }

    public Digest getAccuser() {
        return new Digest(accusation.getAccuser());
    }

    public long getEpoch() {
        return accusation.getEpoch();
    }

    public Digest getHash() {
        return hash;
    }

    public int getRingNumber() {
        return accusation.getRingNumber();
    }

    public JohnHancock getSignature() {
        return signature(accusation.getSignature());
    }

    public Accusation getWrapped() {
        return accusation;
    }
}
