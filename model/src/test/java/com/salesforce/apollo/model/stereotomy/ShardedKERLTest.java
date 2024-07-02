/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model.stereotomy;

import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.cryptography.SigningThreshold;
import com.salesforce.apollo.cryptography.SigningThreshold.Unweighted;
import com.salesforce.apollo.model.Domain;
import com.salesforce.apollo.state.Emulator;
import com.salesforce.apollo.stereotomy.*;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.Seal;
import com.salesforce.apollo.stereotomy.event.Seal.DigestSeal;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.InteractionSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.KeyConfigurationDigester;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.utils.Hex;
import org.joou.ULong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author hal.hildebrand
 */
public class ShardedKERLTest {
    private SecureRandom secureRandom;

    @BeforeEach
    public void before() throws Exception {
        secureRandom = SecureRandom.getInstance("SHA1PRNG");
        secureRandom.setSeed(new byte[] { 0 });
    }

    @Test
    public void delegated() throws Exception {
        Duration timeout = Duration.ofSeconds(1000);
        Emulator emmy = new Emulator();
        emmy.start(Domain.boostrapMigration());

        ShardedKERL kerl = new ShardedKERL(emmy.newConnector(), emmy.getMutator(), timeout, DigestAlgorithm.DEFAULT);

        var ks = new MemKeyStore();
        Stereotomy controller = new StereotomyImpl(ks, kerl, secureRandom);

        ControlledIdentifier<? extends Identifier> base = controller.newIdentifier();

        var opti2 = base.newIdentifier(IdentifierSpecification.newBuilder());
        ControlledIdentifier<? extends Identifier> identifier = opti2;

        // identifier
        assertInstanceOf(SelfAddressingIdentifier.class, identifier.getIdentifier());
        var sap = (SelfAddressingIdentifier) identifier.getIdentifier();
        assertEquals(DigestAlgorithm.DEFAULT, sap.getDigest().getAlgorithm());
        assertEquals("6000b1b611a2a6cb27b6c569c056cf56e04da4905168020fc054d133181d379b",
                     Hex.hex(sap.getDigest().getBytes()));

        assertEquals(1, ((Unweighted) identifier.getSigningThreshold()).getThreshold());

        // keys
        assertEquals(1, identifier.getKeys().size());
        assertNotNull(identifier.getKeys().get(0));

        EstablishmentEvent lastEstablishmentEvent = (EstablishmentEvent) kerl.getKeyEvent(
        identifier.getLastEstablishmentEvent());
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
        assertEquals(ULong.valueOf(0), lastEstablishmentEvent.getSequenceNumber());
        assertEquals(lastEstablishmentEvent.hash(DigestAlgorithm.DEFAULT), identifier.getDigest());

        // lastEvent
        assertNull(kerl.getKeyEvent(identifier.getLastEvent()));

        // delegation
        assertTrue(identifier.getDelegatingIdentifier().isPresent());
        assertTrue(identifier.isDelegated());

        var digest = DigestAlgorithm.BLAKE3_256.digest("digest seal".getBytes());
        var event = EventCoordinates.of(kerl.getKeyEvent(identifier.getLastEstablishmentEvent()));
        var seals = List.of(DigestSeal.construct(digest), DigestSeal.construct(digest), Seal.construct(event));

        identifier.rotate();
        identifier.seal(InteractionSpecification.newBuilder());
        identifier.rotate(RotationSpecification.newBuilder().addAllSeals(seals));
        identifier.seal(InteractionSpecification.newBuilder().addAllSeals(seals));
    }

    @Test
    public void direct() throws Exception {
        Duration timeout = Duration.ofSeconds(1);
        Emulator emmy = new Emulator();
        emmy.start(Domain.boostrapMigration());

        ShardedKERL kerl = new ShardedKERL(emmy.newConnector(), emmy.getMutator(), timeout, DigestAlgorithm.DEFAULT);

        Stereotomy controller = new StereotomyImpl(new MemKeyStore(), kerl, secureRandom);

        var i = controller.newIdentifier();

        var digest = DigestAlgorithm.BLAKE3_256.digest("digest seal".getBytes());
        var event = EventCoordinates.of(kerl.getKeyEvent(i.getLastEstablishmentEvent()));
        var seals = List.of(DigestSeal.construct(digest), DigestSeal.construct(digest), Seal.construct(event));

        i.rotate();
        i.seal(InteractionSpecification.newBuilder());
        i.rotate(RotationSpecification.newBuilder().addAllSeals(seals));
        i.seal(InteractionSpecification.newBuilder().addAllSeals(seals));
        i.rotate();
        i.rotate();
        var opti = kerl.kerl(i.getIdentifier());
        assertNotNull(opti);
        assertNotNull(opti);
        var iKerl = opti;
        assertEquals(7, iKerl.size());
        assertEquals(KeyEvent.INCEPTION_TYPE, iKerl.get(0).event().getIlk());
        assertEquals(KeyEvent.ROTATION_TYPE, iKerl.get(1).event().getIlk());
        assertEquals(KeyEvent.INTERACTION_TYPE, iKerl.get(2).event().getIlk());
        assertEquals(KeyEvent.ROTATION_TYPE, iKerl.get(3).event().getIlk());
        assertEquals(KeyEvent.INTERACTION_TYPE, iKerl.get(4).event().getIlk());
        assertEquals(KeyEvent.ROTATION_TYPE, iKerl.get(5).event().getIlk());
        assertEquals(KeyEvent.ROTATION_TYPE, iKerl.get(6).event().getIlk());

    }
}
