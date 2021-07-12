/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.aleph.creator;

import com.google.protobuf.Any;
import com.salesforce.apollo.membership.aleph.Config;
import com.salesforce.apollo.membership.aleph.PreUnit;
import com.salesforce.apollo.membership.aleph.Unit;

/**
 * @author hal.hildebrand
 *
 */
public interface EpochProofBuilder {

    public interface WeakThresholdKey {
    }

    // EpochProof checks if the given preunit is a proof that a new epoch started.
    static boolean epochProof(PreUnit pu, WeakThresholdKey wtk) {
//     if (!pu.dealing() || wtk == null) {
//         return false;
//     }
//     if (pu.epoch() == 0) {
//         return true;
//     }
//     sig, msg, err := decodeSignature(pu.Data())
//     if err != nil {
//         return false
//     }
//     _, _, epoch, _ := decodeProof(msg)
//     if epoch+1 != pu.EpochID() {
//         return false
//     }
//     return wtk.VerifySignature(sig, msg)
        return false; // TODO
    }

    record epochProofImpl(Config conf, int epoch) {}

    Any buildShare(Unit timingUnit);

    Any tryBuilding(Unit unit);

    boolean verify(Unit unit);

}
