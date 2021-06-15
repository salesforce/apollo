/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.specification;

import static com.salesforce.apollo.crypto.QualifiedBase64.digest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.List;

import org.h2.mvstore.MVStore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.stereotomy.KeyCoordinates;
import com.salesforce.apollo.stereotomy.Stereotomy;
import com.salesforce.apollo.stereotomy.Stereotomy.ControllableIdentifier;
import com.salesforce.apollo.stereotomy.Stereotomy.StereotomyKeyStore;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.Seal.CoordinatesSeal;
import com.salesforce.apollo.stereotomy.event.Seal.DigestSeal;
import com.salesforce.apollo.stereotomy.event.SigningThreshold;
import com.salesforce.apollo.stereotomy.event.SigningThreshold.Unweighted;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.keys.InMemoryKeyStore;
import com.salesforce.apollo.stereotomy.store.StateStore;
import com.salesforce.apollo.utils.Hex;

/**
 * @author hal.hildebrand
 *
 */
public class StereotomyTests {

    SecureRandom             secureRandom   = new SecureRandom(new byte[] { 6, 6, 6 });
    final StateStore         testEventStore = new StateStore(DigestAlgorithm.DEFAULT, MVStore.open(null));
    final StereotomyKeyStore testKeyStore   = new InMemoryKeyStore();

    @BeforeAll
    public static void beforeClass() {
        // secp256k1 is considered "unsecure" so you have enable it like this:
        System.setProperty("jdk.sunec.disableNative", "false");
    }

    @BeforeEach
    public void beforeEachTest() throws NoSuchAlgorithmException {
        // this makes the values of secureRandom deterministic
        secureRandom = SecureRandom.getInstance("SHA1PRNG");
        secureRandom.setSeed(new byte[] { 0 });
    }

    @Test
    public void test_newPrivateIdentifier() {
        var controller = new Stereotomy(testKeyStore, testEventStore, secureRandom);

        ControllableIdentifier identifier = controller.newPrivateIdentifier();

        // identifier
        assertTrue(identifier.getIdentifier() instanceof SelfAddressingIdentifier);
        var sap = (SelfAddressingIdentifier) identifier.getIdentifier();
        assertEquals(DigestAlgorithm.BLAKE3_256, sap.getDigest().getAlgorithm());
        assertEquals("08b35ddf78c25271e9b18ac25814572b8359503a003f5ad71d95f5fd8e1e21e8",
                     Hex.hex(sap.getDigest().getBytes()));

        assertEquals(1, ((Unweighted) identifier.getSigningThreshold()).getThreshold());

        // keys
        assertEquals(1, identifier.getKeys().size());
        assertNotNull(identifier.getKeys().get(0));

        EstablishmentEvent lastEstablishmentEvent = (EstablishmentEvent) testEventStore.getKeyEvent(identifier.getLastEstablishmentEvent())
                                                                                       .get();
        assertEquals(identifier.getKeys().get(0), lastEstablishmentEvent.getKeys().get(0));

        var keyCoordinates = KeyCoordinates.of(lastEstablishmentEvent, 0);
        var keyStoreKeyPair = testKeyStore.getKey(keyCoordinates);
        assertTrue(keyStoreKeyPair.isPresent());
        assertEquals(keyStoreKeyPair.get().getPublic(), identifier.getKeys().get(0));

        // nextKeys
        assertTrue(identifier.getNextKeyConfigurationDigest().isPresent());
        var keyStoreNextKeyPair = testKeyStore.getNextKey(keyCoordinates);
        assertTrue(keyStoreNextKeyPair.isPresent());
        var expectedNextKeys = KeyConfigurationDigester.digest(SigningThreshold.unweighted(1),
                                                               List.of(keyStoreNextKeyPair.get().getPublic()),
                                                               identifier.getNextKeyConfigurationDigest()
                                                                         .get()
                                                                         .getAlgorithm());
        assertEquals(expectedNextKeys, identifier.getNextKeyConfigurationDigest().get());

        // witnesses
        assertEquals(0, identifier.getWitnessThreshold());
        assertEquals(0, identifier.getWitnesses().size());

        // config
        assertEquals(0, identifier.configurationTraits().size());

        // lastEstablishmentEvent
        assertEquals(identifier.getIdentifier(), lastEstablishmentEvent.getIdentifier());
        assertEquals(0, lastEstablishmentEvent.getSequenceNumber());
        assertEquals(lastEstablishmentEvent.hash(DigestAlgorithm.DEFAULT),
                     digest(identifier.getDigest().toByteString()));

        // lastEvent
        KeyEvent lastEvent = testEventStore.getKeyEvent(identifier.getLastEvent()).get();
        assertEquals(identifier.getIdentifier(), lastEvent.getIdentifier());
        assertEquals(0, lastEvent.getSequenceNumber());
        // TODO digest

        assertEquals(lastEvent, lastEstablishmentEvent);

        // delegation
        assertFalse(identifier.getDelegatingIdentifier().isPresent());
        assertFalse(identifier.isDelegated());
    }

    @Test
    public void test_privateIdentifier_rotate() {
        var controller = new Stereotomy(testKeyStore, testEventStore, secureRandom);

        var i = controller.newPrivateIdentifier();

        var digest = DigestAlgorithm.BLAKE3_256.digest("digest seal".getBytes());
        var event = EventCoordinates.of(testEventStore.getKeyEvent(i.getLastEstablishmentEvent()).get());

        i.rotate(List.of(DigestSeal.construct(digest), DigestSeal.construct(digest), CoordinatesSeal.construct(event)));

        i.rotate();
    }

//    @Test
    public void test_privateIdentifier_interaction() {
        var controller = new Stereotomy(testKeyStore, testEventStore, secureRandom);

        var i = controller.newPrivateIdentifier();

        var digest = DigestAlgorithm.BLAKE3_256.digest("digest seal".getBytes());
        var event = EventCoordinates.of(testEventStore.getKeyEvent(i.getLastEstablishmentEvent()).get());
        var seals = List.of(DigestSeal.construct(digest), DigestSeal.construct(digest),
                            CoordinatesSeal.construct(event));

        i.rotate();
        i.seal(List.of());
        i.rotate(seals);
        i.seal(seals);
    }

}
