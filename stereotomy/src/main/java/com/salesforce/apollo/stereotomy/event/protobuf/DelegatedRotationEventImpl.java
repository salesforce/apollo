/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event.protobuf;

import static com.salesforce.apollo.crypto.QualifiedBase64.digest;
import static com.salesforce.apollo.stereotomy.identifier.QualifiedBase64Identifier.identifier;

import com.salesfoce.apollo.stereotomy.event.proto.RotationEvent;
import com.salesforce.apollo.stereotomy.event.DelegatedRotationEvent;
import com.salesforce.apollo.stereotomy.event.EventCoordinates;

/**
 * @author hal.hildebrand
 *
 */
public class DelegatedRotationEventImpl extends RotationEventImpl implements DelegatedRotationEvent {

    public DelegatedRotationEventImpl(RotationEvent event) {
        super(event);
    }

    @Override
    public EventCoordinates getDelegatingEvent() {
        com.salesfoce.apollo.stereotomy.event.proto.EventCoordinates coordinates = event.getDelegatingEvent();
        return new EventCoordinates(identifier(coordinates.getIdentifier()), coordinates.getSequenceNumber(),
                digest(coordinates.getDigest()));
    }

}
