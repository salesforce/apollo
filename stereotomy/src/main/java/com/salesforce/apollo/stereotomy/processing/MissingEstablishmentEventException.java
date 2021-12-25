/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.processing;

import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.KeyEvent;

/**
 * @author hal.hildebrand
 *
 */
public class MissingEstablishmentEventException extends KeyEventProcessingException {

    private static final long      serialVersionUID = 1L;
    private final EventCoordinates missing;

    public EventCoordinates getMissing() {
        return missing;
    }

    public MissingEstablishmentEventException(KeyEvent keyEvent, EventCoordinates missing) {
        super(keyEvent);
        this.missing = missing;
    }

}
