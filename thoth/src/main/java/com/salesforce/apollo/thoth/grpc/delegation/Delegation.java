/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth.grpc.delegation;

import com.salesforce.apollo.cryptography.SigningThreshold;
import com.salesforce.apollo.stereotomy.event.DelegatedInceptionEvent;
import com.salesforce.apollo.stereotomy.event.DelegatedRotationEvent;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification;

import java.util.concurrent.CompletableFuture;

/**
 * @author hal.hildebrand
 */
public interface Delegation {
    DelegatedInceptionEvent inception(SelfAddressingIdentifier controller, SigningThreshold signingThreshold,
                                      SigningThreshold witnessThreshold);

    CompletableFuture<DelegatedRotationEvent> rotate(RotationSpecification.Builder specification);
}
