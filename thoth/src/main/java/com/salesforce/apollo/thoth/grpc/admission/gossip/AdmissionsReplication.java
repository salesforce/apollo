/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth.grpc.admission.gossip;

import com.salesfoce.apollo.thoth.proto.AdmissionsGossip;
import com.salesfoce.apollo.thoth.proto.AdmissionsUpdate;
import com.salesfoce.apollo.thoth.proto.Expunge;
import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
public interface AdmissionsReplication {
    void expunge(Expunge expunge, Digest from);

    AdmissionsUpdate gossip(AdmissionsGossip gossip, Digest from);

    void update(AdmissionsUpdate update, Digest from);
}
