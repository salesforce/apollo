/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event.protobuf;

import static com.salesforce.apollo.stereotomy.identifier.QualifiedBase64Identifier.identifier;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.stereotomy.event.InceptionEvent;
import com.salesforce.apollo.stereotomy.identifier.BasicIdentifier;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * @author hal.hildebrand
 *
 */
public class InceptionEventImpl extends EstablishmentEventImpl implements InceptionEvent {

    final com.salesfoce.apollo.stereotomy.event.proto.InceptionEvent event;

    public InceptionEventImpl(com.salesfoce.apollo.stereotomy.event.proto.InceptionEvent inceptionEvent) {
        super(inceptionEvent.getSpecification().getHeader(), inceptionEvent.getCommon(),
                inceptionEvent.getSpecification().getEstablishment());
        event = inceptionEvent;

    }

    @Override
    public Set<ConfigurationTrait> getConfigurationTraits() {
        return event.getSpecification()
                    .getConfigurationList()
                    .stream()
                    .map(s -> ConfigurationTrait.valueOf(s))
                    .collect(Collectors.toSet());
    }

    @Override
    public Identifier getIdentifier() {
        return identifier(event.getIdentifier());
    }

    @Override
    public byte[] getInceptionStatement() {
        return event.getSpecification().toByteArray();
    }

    @Override
    public List<BasicIdentifier> getWitnesses() {
        return event.getSpecification()
                    .getWitnessesList()
                    .stream()
                    .map(s -> identifier(s))
                    .map(i -> i instanceof BasicIdentifier ? (BasicIdentifier) i : null)
                    .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "InceptionEventImpl\n" + event + "\n";
    }

    @Override
    protected ByteString toByteString() {
        return event.toByteString();
    }
}
