/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KERL.EventWithAttachments;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * @author hal.hildebrand
 *
 */
public interface KERLProvider {
    /**
     * Answer the KERL of the identifier prefix
     */
    Optional<List<EventWithAttachments>> kerl(Identifier prefix) throws TimeoutException;

    /**
     * Resolve the key state for the supplied event coordinates
     */
    Optional<KeyState> resolve(EventCoordinates coordinates) throws TimeoutException;

    /**
     * Resolve the current key state of the identifier prefix
     */
    Optional<KeyState> resolve(Identifier prefix) throws TimeoutException;
}
