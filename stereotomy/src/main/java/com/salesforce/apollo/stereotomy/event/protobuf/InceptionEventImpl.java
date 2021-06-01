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

import com.salesforce.apollo.stereotomy.event.InceptionEvent;
import com.salesforce.apollo.stereotomy.identifier.BasicIdentifier;

/**
 * @author hal.hildebrand
 *
 */
public class InceptionEventImpl extends EstablishmentEventImpl implements InceptionEvent {

    final com.salesfoce.apollo.stereotomy.event.proto.InceptionEvent event;

    public InceptionEventImpl(com.salesfoce.apollo.stereotomy.event.proto.InceptionEvent inceptionEvent) {
        super(inceptionEvent.getHeader(), inceptionEvent.getEstablishment());
        event = inceptionEvent;

    }

    @Override
    public byte[] getBytes() {
        return event.toByteArray();
    }

    @Override
    public Set<ConfigurationTrait> getConfigurationTraits() {
        return event.getConfigurationTraitsList()
                    .stream()
                    .map(s -> ConfigurationTrait.valueOf(s))
                    .collect(Collectors.toSet());
    }

    @Override
    public byte[] getInceptionStatement() {
        return event.getInceptionStatement().toByteArray();
    }

    @Override
    public List<BasicIdentifier> getWitnesses() {
        return event.getWitnessesList()
                    .stream()
                    .map(s -> identifier(s))
                    .map(i -> i instanceof BasicIdentifier ? (BasicIdentifier) i : null)
                    .collect(Collectors.toList());
    }
}
