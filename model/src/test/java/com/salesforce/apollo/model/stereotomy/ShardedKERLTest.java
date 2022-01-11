/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model.stereotomy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.joou.ULong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.SigningThreshold;
import com.salesforce.apollo.crypto.SigningThreshold.Unweighted;
import com.salesforce.apollo.model.Node;
import com.salesforce.apollo.state.Emulator;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KeyCoordinates;
import com.salesforce.apollo.stereotomy.Stereotomy;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.Seal.CoordinatesSeal;
import com.salesforce.apollo.stereotomy.event.Seal.DigestSeal;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.InteractionSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.KeyConfigurationDigester;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.utils.Hex;

/**
 * @author hal.hildebrand
 *
 */
public class ShardedKERLTest {
    private SecureRandom secureRandom;

    @BeforeEach
    public void before() throws Exception {
        secureRandom = SecureRandom.getInstance("SHA1PRNG");
        secureRandom.setSeed(new byte[] { 0 });
    }

    @Test
    public void direct() throws Exception {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        Duration timeout = Duration.ofSeconds(1);
        Executor exec = Executors.newSingleThreadExecutor();
        Emulator emmy = new Emulator();
        emmy.start(Node.boostrapMigration());

        ShardedKERL kerl = new ShardedKERL(emmy.newConnector(), emmy.getMutator(), scheduler, timeout,
                                           DigestAlgorithm.DEFAULT, exec);

        Stereotomy controller = new StereotomyImpl(new MemKeyStore(), kerl, secureRandom);

        var opt = controller.newIdentifier();

        var i = opt.get();

        var digest = DigestAlgorithm.BLAKE3_256.digest("digest seal".getBytes());
        var event = EventCoordinates.of(kerl.getKeyEvent(i.getLastEstablishmentEvent()).get());
        var seals = List.of(DigestSeal.construct(digest), DigestSeal.construct(digest),
                            CoordinatesSeal.construct(event));

        i.rotate();
        i.seal(InteractionSpecification.newBuilder());
        i.rotate(RotationSpecification.newBuilder().addAllSeals(seals));
        i.seal(InteractionSpecification.newBuilder().addAllSeals(seals));

    }

    @Test
    public void delegated() throws Exception {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        Duration timeout = Duration.ofSeconds(1000);
        Executor exec = Executors.newSingleThreadExecutor();
        Emulator emmy = new Emulator();
        emmy.start(Node.boostrapMigration());

        ShardedKERL kerl = new ShardedKERL(emmy.newConnector(), emmy.getMutator(), scheduler, timeout,
                                           DigestAlgorithm.DEFAULT, exec);

        var ks = new MemKeyStore();
        Stereotomy controller = new StereotomyImpl(ks, kerl, secureRandom);

        var opti = controller.newIdentifier();
        ControlledIdentifier base = opti.get();

          opti = base.newIdentifier(IdentifierSpecification.newBuilder());
        ControlledIdentifier identifier = opti.get();

        // identifier
        assertTrue(identifier.getIdentifier() instanceof SelfAddressingIdentifier);
        var sap = (SelfAddressingIdentifier) identifier.getIdentifier();
        assertEquals(DigestAlgorithm.BLAKE2B_256, sap.getDigest().getAlgorithm());
        assertEquals("2287a5841816c8c02d4e188376b1f1a50dfcdc9eaac17610deba4ae33bb617f4",
                     Hex.hex(sap.getDigest().getBytes()));

        assertEquals(1, ((Unweighted) identifier.getSigningThreshold()).getThreshold());

        // keys
        assertEquals(1, identifier.getKeys().size());
        assertNotNull(identifier.getKeys().get(0));

        EstablishmentEvent lastEstablishmentEvent = (EstablishmentEvent) kerl.getKeyEvent(identifier.getLastEstablishmentEvent())
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
        assertTrue(kerl.getKeyEvent(identifier.getLastEvent()).isEmpty());

        // delegation
        assertTrue(identifier.getDelegatingIdentifier().isPresent());
        assertTrue(identifier.isDelegated());

        var digest = DigestAlgorithm.BLAKE3_256.digest("digest seal".getBytes());
        var event = EventCoordinates.of(kerl.getKeyEvent(identifier.getLastEstablishmentEvent()).get());
        var seals = List.of(DigestSeal.construct(digest), DigestSeal.construct(digest),
                            CoordinatesSeal.construct(event));

        identifier.rotate();
        identifier.seal(InteractionSpecification.newBuilder());
        identifier.rotate(RotationSpecification.newBuilder().addAllSeals(seals));
        identifier.seal(InteractionSpecification.newBuilder().addAllSeals(seals));
    }
}
