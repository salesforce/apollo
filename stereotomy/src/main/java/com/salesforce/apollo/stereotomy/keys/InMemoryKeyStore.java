/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.keys;

import static java.util.Comparator.comparing;
import static java.util.Comparator.comparingInt;
import static java.util.Map.Entry.comparingByKey;

import java.security.KeyPair;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.salesforce.apollo.crypto.QualifiedBase64;
import com.salesforce.apollo.stereotomy.KeyCoordinates;
import com.salesforce.apollo.stereotomy.Stereotomy.StereotomyKeyStore;

/**
 * @author hal.hildebrand
 *
 */
public class InMemoryKeyStore implements StereotomyKeyStore {

    private final Map<KeyCoordinates, KeyPair> keys     = new HashMap<>();
    private final Map<KeyCoordinates, KeyPair> nextKeys = new HashMap<>();

    @Override
    public void storeKey(KeyCoordinates coordinates, KeyPair keyPair) {
        this.keys.put(coordinates, keyPair);
    }

    @Override
    public Optional<KeyPair> getKey(KeyCoordinates keyCoordinates) {
        // TODO digest algorithm agility--need to re-hash if not found
        return Optional.ofNullable(this.keys.get(keyCoordinates));
    }

    @Override
    public Optional<KeyPair> removeKey(KeyCoordinates keyCoordinates) {
        // TODO digest algorithm agility--need to re-hash if not found
        return Optional.ofNullable(this.keys.remove(keyCoordinates));
    }

    @Override
    public void storeNextKey(KeyCoordinates coordinates, KeyPair keyPair) {
        this.nextKeys.put(coordinates, keyPair);
    }

    @Override
    public Optional<KeyPair> getNextKey(KeyCoordinates keyCoordinates) {
        // TODO digest algorithm agility--need to re-hash if not found
        return Optional.ofNullable(this.nextKeys.get(keyCoordinates));
    }

    @Override
    public Optional<KeyPair> removeNextKey(KeyCoordinates keyCoordinates) {
        // TODO digest algorithm agility--need to re-hash if not found
        return Optional.ofNullable(this.nextKeys.remove(keyCoordinates));
    }

    public void printContents() {
        System.out.println();
        System.out.println("====== IDENTIFIER KEY STORE ======");
        System.out.println("KEYS:");

        Comparator<KeyCoordinates> keyIdentifierComparator = comparing(k -> ((KeyCoordinates) k).getEstablishmentEvent()
                                                                                                .getIdentifier()
                                                                                                .toString());
        var keySequenceNumberComparator = comparing(k -> ((KeyCoordinates) k).getEstablishmentEvent()
                                                                             .getSequenceNumber());
        var keyEventDigestComparator = comparing(k -> ((KeyCoordinates) k).getEstablishmentEvent()
                                                                          .getDigest()
                                                                          .toString());
        var keyIndexComparator = comparingInt(KeyCoordinates::getKeyIndex);
        var keyCoordinatesComparator = keyIdentifierComparator.thenComparing(keySequenceNumberComparator)
                                                              .thenComparing(keyEventDigestComparator)
                                                              .thenComparing(keyIndexComparator);

        this.keys.entrySet()
                 .stream()
                 .sorted(comparingByKey(keyCoordinatesComparator))
                 .forEachOrdered(kv -> System.out.println(kv.getKey() + " -> "
                         + QualifiedBase64.qb64(kv.getValue().getPublic())));

        System.out.println("NEXT KEYS:");
        this.nextKeys.entrySet()
                     .stream()
                     .sorted(comparingByKey(keyCoordinatesComparator))
                     .forEachOrdered(kv -> System.out.println(kv.getKey() + " -> "
                             + QualifiedBase64.qb64(kv.getValue().getPublic())));

        System.out.println("=========================");
        System.out.println();
    }

}
