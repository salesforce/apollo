/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event;

import java.security.PublicKey;
import java.util.List;
import java.util.Optional;

import com.salesforce.apollo.stereotomy.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
public interface EstablishmentEvent extends KeyEvent {

    SigningThreshold signingThreshold();

    List<PublicKey> keys();

    Optional<Digest> nextKeyConfiguration();

    int witnessThreshold();

}
