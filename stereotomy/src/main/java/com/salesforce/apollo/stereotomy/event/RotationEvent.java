/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event;

import java.util.List;

import com.salesforce.apollo.stereotomy.identifier.BasicIdentifier;

/**
 * @author hal.hildebrand
 *
 */
public interface RotationEvent extends EstablishmentEvent, SealingEvent {

    @Override
    default String getIlk() {
        return ROTATION_TYPE;
    }

    List<BasicIdentifier> getWitnessesAddedList();

    List<BasicIdentifier> getWitnessesRemovedList();

}
