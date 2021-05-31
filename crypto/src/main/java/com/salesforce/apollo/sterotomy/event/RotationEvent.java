/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.sterotomy.event;

import java.util.List;

import com.salesforce.apollo.sterotomy.identifier.BasicIdentifier;
 

/**
 * @author hal.hildebrand
 *
 */
public interface RotationEvent extends EstablishmentEvent, SealingEvent {

    List<BasicIdentifier> removedWitnesses();

    List<BasicIdentifier> addedWitnesses();

    List<Seal> seals();

  }