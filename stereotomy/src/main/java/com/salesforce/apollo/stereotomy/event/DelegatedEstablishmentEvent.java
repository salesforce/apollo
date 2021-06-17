/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event;

import com.salesforce.apollo.stereotomy.event.Seal.DelegatingLocationSeal;

/**
 * @author hal.hildebrand
 *
 */
public interface DelegatedEstablishmentEvent extends EstablishmentEvent {

    DelegatingLocationSeal getDelegatingSeal();

}
