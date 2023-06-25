/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model.demesnes;

import java.util.List;

import com.salesfoce.apollo.stereotomy.event.proto.EventCoords;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.DelegatedInceptionEvent;
import com.salesforce.apollo.stereotomy.event.DelegatedRotationEvent;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification.Builder;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification;

/**
 * Domain Isolate interface
 *
 * @author hal.hildebrand
 *
 */
public interface Demesne {

    boolean active();

    void commit(EventCoords coordinates);

    SelfAddressingIdentifier getId();

    DelegatedInceptionEvent inception(Ident identifier, Builder<SelfAddressingIdentifier> specification);

    DelegatedRotationEvent rotate(RotationSpecification.Builder specification);

    void start();

    void stop();

    void viewChange(Digest viewId, List<EventCoordinates> joining, List<Digest> leaving);

}
