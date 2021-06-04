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
import com.salesforce.apollo.stereotomy.event.DelegatingEventCoordinates;
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
    public DelegatingEventCoordinates getDelegatingEvent() {
        EventCoordinates coordinates = getCoordinates();
        com.salesfoce.apollo.stereotomy.event.proto.EventCoordinates delegated = event.getDelegatingEvent();
        return new DelegatingEventCoordinates(coordinates.getIdentifier(), coordinates.getSequenceNumber(),
                new EventCoordinates(identifier(delegated.getIdentifier()), delegated.getSequenceNumber(),
                                     digest(delegated.getDigest())));

    }

}
