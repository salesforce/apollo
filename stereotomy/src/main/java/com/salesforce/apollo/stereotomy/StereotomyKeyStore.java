/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import java.security.KeyPair;
import java.security.PublicKey;
import java.util.Optional;

public interface StereotomyKeyStore {

    Optional<KeyPair> getKey(KeyCoordinates keyCoordinates);

    Optional<KeyPair> getNextKey(KeyCoordinates keyCoordinates);

    Optional<PublicKey> getPublicKey(KeyCoordinates keyCoordinates);

    Optional<KeyPair> removeKey(KeyCoordinates keyCoordinates);

    Optional<KeyPair> removeNextKey(KeyCoordinates keyCoordinates);

    void storeKey(KeyCoordinates keyCoordinates, KeyPair keyPair);

    void storeNextKey(KeyCoordinates keyCoordinates, KeyPair keyPair);

}