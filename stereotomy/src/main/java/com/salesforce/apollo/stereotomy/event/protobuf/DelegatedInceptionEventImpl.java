/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event.protobuf;

import static com.salesforce.apollo.stereotomy.QualifiedBase64.digest;
import static com.salesforce.apollo.stereotomy.QualifiedBase64.identifier;

import com.salesforce.apollo.stereotomy.controller.Coordinates;
import com.salesforce.apollo.stereotomy.event.DelegatedInceptionEvent;
import com.salesforce.apollo.stereotomy.event.EventCoordinates;

/**
 * @author hal.hildebrand
 *
 */
public class DelegatedInceptionEventImpl extends InceptionEventImpl implements DelegatedInceptionEvent {

    public DelegatedInceptionEventImpl(com.salesfoce.apollo.stereotomy.event.proto.InceptionEvent inceptionEvent) {
        super(inceptionEvent);
    }

    @Override
    public EventCoordinates getDelegatingEvent() {
        com.salesfoce.apollo.stereotomy.event.proto.EventCoordinates coordinates = event.getDelegatingEvent();
        return new Coordinates(identifier(coordinates.getIdentifier()), coordinates.getSequenceNumber(),
                digest(coordinates.getDigest()));

    }
}
