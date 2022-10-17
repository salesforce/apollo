/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion.comm.endorsement;

import java.util.concurrent.CompletableFuture;

import com.google.protobuf.Empty;
import com.salesfoce.apollo.gorgoneion.proto.Credentials;
import com.salesfoce.apollo.gorgoneion.proto.Nonce;
import com.salesfoce.apollo.gorgoneion.proto.Notarization;
import com.salesfoce.apollo.stereotomy.event.proto.Validation_;
import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
public interface EndorsementService {

    CompletableFuture<Validation_> endorse(Nonce request, Digest from);

    CompletableFuture<Empty> enroll(Notarization request, Digest from);

    CompletableFuture<Validation_> validate(Credentials credentials, Digest id);
}
