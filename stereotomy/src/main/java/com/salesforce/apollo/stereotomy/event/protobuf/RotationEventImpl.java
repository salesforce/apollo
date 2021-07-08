/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event.protobuf;

import static com.salesforce.apollo.stereotomy.identifier.QualifiedBase64Identifier.identifier;

import java.util.List;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
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
        super(event.getSpecification().getHeader(), event.getCommon(), event.getSpecification().getEstablishment());
        this.event = event;
    }

    @Override
    public List<Seal> getSeals() {
        return event.getSpecification().getSealsList().stream().map(s -> Seal.from(s)).collect(Collectors.toList());
    }

    @Override
    public List<BasicIdentifier> getWitnessesAddedList() {
        return event.getSpecification()
                    .getWitnessesAddedList()
                    .stream()
                    .map(s -> identifier(s))
                    .map(i -> i instanceof BasicIdentifier ? (BasicIdentifier) i : null)
                    .collect(Collectors.toList());
    }

    @Override
    public List<BasicIdentifier> getWitnessesRemovedList() {
        return event.getSpecification()
                    .getWitnessesRemovedList()
                    .stream()
                    .map(s -> identifier(s))
                    .map(i -> i instanceof BasicIdentifier ? (BasicIdentifier) i : null)
                    .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "\n" + event + "\n";
    }

    @Override
    protected ByteString toByteString() {
        return event.toByteString();
    }
}
