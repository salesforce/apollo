/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.specification;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.List;

import org.h2.mvstore.MVStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
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
import com.salesforce.apollo.stereotomy.identifier.AutonomicIdentifier;
import com.salesforce.apollo.stereotomy.identifier.BasicIdentifier;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.KeyConfigurationDigester;
import com.salesforce.apollo.stereotomy.keys.InMemoryKeyStore;
import com.salesforce.apollo.stereotomy.mvlog.MvLog;
import com.salesforce.apollo.utils.Hex;

/**
 * @author hal.hildebrand
 *
 */
public class StereotomyTests {

    final MvLog              kel          = new MvLog(DigestAlgorithm.DEFAULT, MVStore.open(null));
    final StereotomyKeyStore ks           = new InMemoryKeyStore();
    SecureRandom             secureRandom = new SecureRandom(new byte[] { 6, 6, 6 });

    @BeforeEach
    public void beforeEachTest() throws NoSuchAlgorithmException {
        // this makes the values of secureRandom deterministic
        secureRandom = SecureRandom.getInstance("SHA1PRNG");
        secureRandom.setSeed(new byte[] { 0 });
    }

    @Test
    public void identifierInteraction() {
        var controller = new Stereotomy(ks, kel, secureRandom);

        var i = controller.newIdentifier(Identifier.NONE);

        var digest = DigestAlgorithm.BLAKE3_256.digest("digest seal".getBytes());
        var event = EventCoordinates.of(kel.getKeyEvent(i.getLastEstablishmentEvent()).get());
        var seals = List.of(DigestSeal.construct(digest), DigestSeal.construct(digest),
                            CoordinatesSeal.construct(event));

        i.rotate();
        i.seal(List.of());
        i.rotate(seals);
        i.seal(seals);
    }

    @Test
    public void identifierRotate() {
        var controller = new Stereotomy(ks, kel, secureRandom);

        var i = controller.newIdentifier(Identifier.NONE);

        var digest = DigestAlgorithm.BLAKE3_256.digest("digest seal".getBytes());
        var event = EventCoordinates.of(kel.getKeyEvent(i.getLastEstablishmentEvent()).get());

        i.rotate(List.of(DigestSeal.construct(digest), DigestSeal.construct(digest), CoordinatesSeal.construct(event)));

        i.rotate();
    }

    @Test
    public void newIdentifier() {
        var controller = new Stereotomy(ks, kel, secureRandom);

        ControllableIdentifier identifier = controller.newIdentifier(Identifier.NONE);

        // identifier
        assertTrue(identifier.getIdentifier() instanceof SelfAddressingIdentifier);
        var sap = (SelfAddressingIdentifier) identifier.getIdentifier();
        assertEquals(DigestAlgorithm.BLAKE2B_256, sap.getDigest().getAlgorithm());
        assertEquals("c9612a2c0e775f6c3365d516234ef15870eed7236fe37bef798fc218df78a9ee",
                     Hex.hex(sap.getDigest().getBytes()));

        assertEquals(1, ((Unweighted) identifier.getSigningThreshold()).getThreshold());

        // keys
        assertEquals(1, identifier.getKeys().size());
        assertNotNull(identifier.getKeys().get(0));

        EstablishmentEvent lastEstablishmentEvent = (EstablishmentEvent) kel.getKeyEvent(identifier.getLastEstablishmentEvent())
                                                                            .get();
        assertEquals(identifier.getKeys().get(0), lastEstablishmentEvent.getKeys().get(0));

        var keyCoordinates = KeyCoordinates.of(lastEstablishmentEvent, 0);
        var keyStoreKeyPair = ks.getKey(keyCoordinates);
        assertTrue(keyStoreKeyPair.isPresent());
        assertEquals(keyStoreKeyPair.get().getPublic(), identifier.getKeys().get(0));

        // nextKeys
        assertTrue(identifier.getNextKeyConfigurationDigest().isPresent());
        var keyStoreNextKeyPair = ks.getNextKey(keyCoordinates);
        assertTrue(keyStoreNextKeyPair.isPresent());
        var expectedNextKeys = KeyConfigurationDigester.digest(SigningThreshold.unweighted(1),
                                                               List.of(keyStoreNextKeyPair.get().getPublic()),
                                                               identifier.getNextKeyConfigurationDigest().get()
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
        assertEquals(lastEstablishmentEvent.hash(DigestAlgorithm.DEFAULT), identifier.getDigest());

        // lastEvent
        KeyEvent lastEvent = kel.getKeyEvent(identifier.getLastEvent()).get();
        assertEquals(identifier.getIdentifier(), lastEvent.getIdentifier());
        assertEquals(0, lastEvent.getSequenceNumber());
        // TODO digest

        assertEquals(lastEvent, lastEstablishmentEvent);

        // delegation
        assertFalse(identifier.getDelegatingIdentifier().isPresent());
        assertFalse(identifier.isDelegated());
    }

    @Test
    public void newIdentifierFromIdentifier() throws Exception {
        var controller = new Stereotomy(ks, kel, secureRandom);
        KeyPair keyPair = SignatureAlgorithm.DEFAULT.generateKeyPair(secureRandom);
        AutonomicIdentifier aid = new AutonomicIdentifier(new BasicIdentifier(keyPair.getPublic()),
                                                          new URL("http://foo.com/bar/baz/bozo").toURI());
        ControllableIdentifier identifier = controller.newIdentifier(aid);

        // identifier
        assertTrue(identifier.getIdentifier() instanceof SelfAddressingIdentifier);
        var sap = (SelfAddressingIdentifier) identifier.getIdentifier();
        assertEquals(DigestAlgorithm.BLAKE2B_256, sap.getDigest().getAlgorithm());
        assertEquals("4b442022c4ac298ec6c427c71833b0d1740082500ccd84a499ee569c988d0c6a",
                     Hex.hex(sap.getDigest().getBytes()));

        assertEquals(1, ((Unweighted) identifier.getSigningThreshold()).getThreshold());

        // keys
        assertEquals(1, identifier.getKeys().size());
        assertNotNull(identifier.getKeys().get(0));

        EstablishmentEvent lastEstablishmentEvent = (EstablishmentEvent) kel.getKeyEvent(identifier.getLastEstablishmentEvent())
                                                                            .get();
        assertEquals(identifier.getKeys().get(0), lastEstablishmentEvent.getKeys().get(0));

        var keyCoordinates = KeyCoordinates.of(lastEstablishmentEvent, 0);
        var keyStoreKeyPair = ks.getKey(keyCoordinates);
        assertTrue(keyStoreKeyPair.isPresent());
        assertEquals(keyStoreKeyPair.get().getPublic(), identifier.getKeys().get(0));

        // nextKeys
        assertTrue(identifier.getNextKeyConfigurationDigest().isPresent());
        var keyStoreNextKeyPair = ks.getNextKey(keyCoordinates);
        assertTrue(keyStoreNextKeyPair.isPresent());
        var expectedNextKeys = KeyConfigurationDigester.digest(SigningThreshold.unweighted(1),
                                                               List.of(keyStoreNextKeyPair.get().getPublic()),
                                                               identifier.getNextKeyConfigurationDigest().get()
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
        assertEquals(lastEstablishmentEvent.hash(DigestAlgorithm.DEFAULT), identifier.getDigest());

        // lastEvent
        KeyEvent lastEvent = kel.getKeyEvent(identifier.getLastEvent()).get();
        assertEquals(identifier.getIdentifier(), lastEvent.getIdentifier());
        assertEquals(0, lastEvent.getSequenceNumber());
        // TODO digest

        assertEquals(lastEvent, lastEstablishmentEvent);

        // delegation
        assertFalse(identifier.getDelegatingIdentifier().isPresent());
        assertFalse(identifier.isDelegated());
    }

}
