/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.sterotomy.event;

import java.util.List;
import java.util.Set;

import com.salesforce.apollo.sterotomy.identifier.BasicIdentifier;

/**
 * @author hal.hildebrand
 *
 */
public interface InceptionEvent extends EstablishmentEvent {
    public enum ConfigurationTrait {
        ESTABLISHMENT_EVENTS_ONLY, DO_NOT_DELEGATE,
    }

    byte[] inceptionStatement();

    List<BasicIdentifier> witnesses();

    Set<ConfigurationTrait> configurationTraits();

}
