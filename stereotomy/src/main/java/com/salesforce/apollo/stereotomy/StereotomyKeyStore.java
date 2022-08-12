/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import java.security.KeyPair;
import java.util.Optional;

/**
 * The interface for a keystore that tracks KeyPairs by keyCoordinates, and
 * knows about current and next keypairs associated with those coordinates
 * 
 * @author hal.hildebrand
 *
 */
public interface StereotomyKeyStore {

    Optional<KeyPair> getKey(KeyCoordinates keyCoordinates);

    Optional<KeyPair> getNextKey(KeyCoordinates keyCoordinates);

    void removeKey(KeyCoordinates keyCoordinates);

    void removeNextKey(KeyCoordinates keyCoordinates);

    void storeKey(KeyCoordinates keyCoordinates, KeyPair keyPair);

    void storeNextKey(KeyCoordinates keyCoordinates, KeyPair keyPair);

}
