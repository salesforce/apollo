/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services;

import com.google.common.base.Predicate;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;

/**
 * The EventValidation provides validation predicates for EstablishmentEvents
 * 
 * @author hal.hildebrand
 *
 */
public interface EventValidation extends Predicate<EstablishmentEvent> {

    /**
     * Answer true if the identifier prefix is validated. This means that thresholds
     * have been met from indicated witnesses and trusted validators.
     */
    @Override
    boolean apply(EstablishmentEvent event);
}
