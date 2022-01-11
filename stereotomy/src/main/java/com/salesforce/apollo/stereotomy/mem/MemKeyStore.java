/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.mem;

import java.security.KeyPair;
import java.security.PublicKey;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import com.salesforce.apollo.stereotomy.KeyCoordinates;
import com.salesforce.apollo.stereotomy.StereotomyKeyStore;

/**
 * @author hal.hildebrand
 *
 */
public class MemKeyStore implements StereotomyKeyStore {

    private final Map<KeyCoordinates, KeyPair> keys     = new ConcurrentHashMap<>();
    private final Map<KeyCoordinates, KeyPair> nextKeys = new ConcurrentHashMap<>();

    @Override
    public Optional<KeyPair> getKey(KeyCoordinates keyCoordinates) {
        return Optional.ofNullable(this.keys.get(keyCoordinates));
    }

    @Override
    public Optional<KeyPair> getNextKey(KeyCoordinates keyCoordinates) {
        return Optional.ofNullable(this.nextKeys.get(keyCoordinates));
    }

    @Override
    public Optional<PublicKey> getPublicKey(KeyCoordinates keyCoordinates) {
        return getKey(keyCoordinates).stream().map(kp -> kp.getPublic()).findFirst();
    }

    @Override
    public Optional<KeyPair> removeKey(KeyCoordinates keyCoordinates) {
        return Optional.ofNullable(this.keys.remove(keyCoordinates));
    }

    @Override
    public Optional<KeyPair> removeNextKey(KeyCoordinates keyCoordinates) {
        return Optional.ofNullable(this.nextKeys.remove(keyCoordinates));
    }

    @Override
    public void storeKey(KeyCoordinates coordinates, KeyPair keyPair) {
        this.keys.put(coordinates, keyPair);
    }

    @Override
    public void storeNextKey(KeyCoordinates coordinates, KeyPair keyPair) {
        this.nextKeys.put(coordinates, keyPair);
    }

}
