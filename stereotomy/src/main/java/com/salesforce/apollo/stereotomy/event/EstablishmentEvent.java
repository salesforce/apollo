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

import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.SigningThreshold;

/**
 * @author hal.hildebrand
 *
 */
public interface EstablishmentEvent extends KeyEvent {

    SigningThreshold getSigningThreshold();

    List<PublicKey> getKeys();

    Optional<Digest> getNextKeysDigest();

    int getWitnessThreshold();

}
