/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event.protobuf;

import static com.salesforce.apollo.stereotomy.QualifiedBase64.identifier;

import java.util.List;
import java.util.stream.Collectors;

import com.salesforce.apollo.stereotomy.event.RotationEvent;
import com.salesforce.apollo.stereotomy.event.Seal;
import com.salesforce.apollo.stereotomy.identifier.BasicIdentifier;

/**
 * @author hal.hildebrand
 *
 */
public class RotationEventImpl extends EstablishmentEventImpl implements RotationEvent {

    final com.salesfoce.apollo.stereotomy.event.proto.RotationEvent event;

    public RotationEventImpl(com.salesfoce.apollo.stereotomy.event.proto.RotationEvent event) {
        super(event.getHeader(), event.getEstablishment());
        this.event = event;
    }

    @Override
    public List<BasicIdentifier> getAddedWitnesses() {
        return event.getAddedWitnessesList()
                    .stream()
                    .map(s -> identifier(s))
                    .map(i -> i instanceof BasicIdentifier ? (BasicIdentifier) i : null)
                    .collect(Collectors.toList());
    }

    @Override
    public List<BasicIdentifier> getRemovedWitnesses() {
        return event.getRemovedWitnessesList()
                    .stream()
                    .map(s -> identifier(s))
                    .map(i -> i instanceof BasicIdentifier ? (BasicIdentifier) i : null)
                    .collect(Collectors.toList());
    }

    @Override
    public List<Seal> seals() {
        return event.getSealsList().stream().map(s -> KeyEventImpl.sealOf(s)).collect(Collectors.toList());
    }

    @Override
    public byte[] getBytes() {
        return event.toByteArray();
    }
}
