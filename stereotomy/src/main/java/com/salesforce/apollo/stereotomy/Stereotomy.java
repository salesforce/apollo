/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import java.util.Optional;

import com.salesforce.apollo.stereotomy.event.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.Version;
import com.salesforce.apollo.stereotomy.identifier.BasicIdentifier;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;

/**
 * The Controller interface
 * 
 * @author hal.hildebrand
 *
 */
public interface Stereotomy {

    static Version currentVersion() {
        return new Version() {

            @Override
            public int getMajor() {
                return 0;
            }

            @Override
            public int getMinor() {
                return 1;
            }
        };
    }

    Optional<ControllableIdentifier> controlOf(Identifier identifier);

    Optional<KeyState> getKeyState(EventCoordinates eventCoordinates);

    default Optional<KeyState> getKeyState(KeyCoordinates keyCoordinates) {
        return getKeyState(keyCoordinates.getEstablishmentEvent());
    }

    Optional<ControllableIdentifier> newDelegatedIdentifier(Identifier delegator);

    Optional<ControllableIdentifier> newIdentifier(Identifier identifier, BasicIdentifier... witnesses);

    Optional<ControllableIdentifier> newIdentifier(Identifier identifier, IdentifierSpecification.Builder spec);

}
