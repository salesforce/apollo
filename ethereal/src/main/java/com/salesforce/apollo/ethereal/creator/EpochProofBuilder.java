/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.creator;

import java.util.function.Function;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.ethereal.proto.EpochProof;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.PreUnit;
import com.salesforce.apollo.ethereal.Unit;
import com.salesforce.apollo.ethereal.WeakThresholdKey;

/**
 * proof is a message required to verify if the epoch has finished. It consists
 * of id and hash of the last timing unit of the epoch. This message is signed
 * with a threshold signature.
 * 
 * @author hal.hildebrand
 *
 */
public interface EpochProofBuilder {

    public static Function<Integer, EpochProofBuilder> newProofBuilder(Config conf) {
        return epoch -> new epochProofImpl(conf, epoch);
    }

    record epochProofImpl(Config conf, int epoch) implements EpochProofBuilder {

        @Override
        public Any buildShare(Unit timingUnit) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Any tryBuilding(Unit unit) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public boolean verify(Unit unit) {
            // TODO Auto-generated method stub
            return false;
        }

    }

    // EpochProof checks if the given preunit is a proof that a new epoch started.
    static boolean epochProof(PreUnit pu, WeakThresholdKey wtk) {
        if (!pu.dealing() || wtk == null) {
            return false;
        }
        if (pu.epoch() == 0) {
            return true;
        }
        EpochProof decoded;
        try {
            decoded = pu.data().unpack(EpochProof.class);
        } catch (InvalidProtocolBufferException e) {
            // TODO Log
            e.printStackTrace();
            return false;
        }
        int epoch = decoded.getMsg().getEpoch();
        if (epoch + 1 != pu.epoch()) {
            return false;
        }
        return wtk.verifySignature(new JohnHancock(decoded.getSignature()), decoded.getMsg());
    }

    Any buildShare(Unit timingUnit);

    Any tryBuilding(Unit unit);

    boolean verify(Unit unit);

}
