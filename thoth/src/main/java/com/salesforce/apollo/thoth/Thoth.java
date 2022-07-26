/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth;

import java.util.Arrays;

import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.Stereotomy;
import com.salesforce.apollo.stereotomy.event.Seal.EventSeal;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification.Builder;
import com.salesforce.apollo.stereotomy.identifier.spec.InteractionSpecification;

/**
 * 
 * The control interface for a node
 *
 * @author hal.hildebrand
 *
 */
public class Thoth {

    @SuppressWarnings("unused")
    private final SelfAddressingIdentifier                       controller;
    private final ControlledIdentifier<SelfAddressingIdentifier> identifier;
    private final InteractionSpecification                       inception;

    public Thoth(Stereotomy stereotomy, SelfAddressingIdentifier controller,
                 Builder<SelfAddressingIdentifier> specification) {
        final var id = stereotomy.newIdentifier(controller, specification);
        if (id.isEmpty()) {
            throw new IllegalStateException("Cannot create identifier");
        }
        identifier = id.get();
        this.controller = controller;
        inception = InteractionSpecification.newBuilder()
                                            .addAllSeals(Arrays.asList(EventSeal.construct(identifier.getIdentifier(),
                                                                                           identifier.getDigest(),
                                                                                           identifier.getSequenceNumber()
                                                                                                     .longValue())))
                                            .build();
    }

    public SelfAddressingIdentifier identifier() {
        return identifier.getIdentifier();
    }

    public InteractionSpecification inception() {
        return inception;
    }

    public ControlledIdentifierMember member() {
        return new ControlledIdentifierMember(identifier);
    }

    public InteractionSpecification rotate() {
        identifier.rotate();
        // Seal we need to verify the inception, based on the delegated inception
        // location
        return InteractionSpecification.newBuilder()
                                       .addAllSeals(Arrays.asList(EventSeal.construct(identifier.getIdentifier(),
                                                                                      identifier.getDigest(),
                                                                                      identifier.getSequenceNumber()
                                                                                                .longValue())))
                                       .build();
    }

}
