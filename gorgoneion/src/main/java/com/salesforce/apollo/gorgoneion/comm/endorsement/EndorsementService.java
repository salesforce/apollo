/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion.comm.endorsement;

import com.salesforce.apollo.gorgoneion.proto.Credentials;
import com.salesforce.apollo.gorgoneion.proto.MemberSignature;
import com.salesforce.apollo.gorgoneion.proto.Nonce;
import com.salesforce.apollo.gorgoneion.proto.Notarization;
import com.salesforce.apollo.stereotomy.event.proto.Validation_;
import com.salesforce.apollo.cryptography.Digest;

/**
 * @author hal.hildebrand
 */
public interface EndorsementService {

    MemberSignature endorse(Nonce request, Digest from);

    void enroll(Notarization request, Digest from);

    Validation_ validate(Credentials credentials, Digest id);
}
