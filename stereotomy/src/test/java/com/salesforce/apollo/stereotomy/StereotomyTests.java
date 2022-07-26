/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import static com.salesforce.apollo.stereotomy.identifier.QualifiedBase64Identifier.qb64;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.joou.ULong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.SigningThreshold;
import com.salesforce.apollo.crypto.SigningThreshold.Unweighted;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.Seal.CoordinatesSeal;
import com.salesforce.apollo.stereotomy.event.Seal.DigestSeal;
import com.salesforce.apollo.stereotomy.identifier.BasicIdentifier;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.InteractionSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.KeyConfigurationDigester;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.utils.Hex;

/**
 * @author hal.hildebrand
 *
 */
public class StereotomyTests {
    KERL                     kel;
    final StereotomyKeyStore ks = new MemKeyStore();
    SecureRandom             secureRandom;

    @BeforeEach
    public void before() throws Exception {
        secureRandom = SecureRandom.getInstance("SHA1PRNG");
        secureRandom.setSeed(new byte[] { 0 });
        initializeKel();
        // this makes the values of secureRandom deterministic
    }

    @Test
    public void identifierInteraction() throws Exception {
        Stereotomy controller = new StereotomyImpl(ks, kel, secureRandom);

        var i = controller.newIdentifier().get();

        var digest = DigestAlgorithm.BLAKE3_256.digest("digest seal".getBytes());
        var event = EventCoordinates.of(kel.getKeyEvent(i.getLastEstablishmentEvent()).get());
        var seals = List.of(DigestSeal.construct(digest), DigestSeal.construct(digest),
                            CoordinatesSeal.construct(event));

        i.rotate();
        i.seal(InteractionSpecification.newBuilder());
        i.rotate(RotationSpecification.newBuilder().addAllSeals(seals));
        i.seal(InteractionSpecification.newBuilder().addAllSeals(seals));
        i.rotate();
        i.rotate();
        var iKerl = kel.kerl(i.getIdentifier()).get();
        assertNotNull(iKerl);
        assertEquals(7, iKerl.size());
        assertEquals(KeyEvent.INCEPTION_TYPE, iKerl.get(0).event().getIlk());
        assertEquals(KeyEvent.ROTATION_TYPE, iKerl.get(1).event().getIlk());
        assertEquals(KeyEvent.INTERACTION_TYPE, iKerl.get(2).event().getIlk());
        assertEquals(KeyEvent.ROTATION_TYPE, iKerl.get(3).event().getIlk());
        assertEquals(KeyEvent.INTERACTION_TYPE, iKerl.get(4).event().getIlk());
        assertEquals(KeyEvent.ROTATION_TYPE, iKerl.get(5).event().getIlk());
        assertEquals(KeyEvent.ROTATION_TYPE, iKerl.get(6).event().getIlk());
    }

    @Test
    public void identifierRotate() throws Exception {
        Stereotomy controller = new StereotomyImpl(ks, kel, secureRandom);

        var i = controller.newIdentifier().get();

        var digest = DigestAlgorithm.BLAKE3_256.digest("digest seal".getBytes());
        var event = EventCoordinates.of(kel.getKeyEvent(i.getLastEstablishmentEvent()).get());

        i.rotate(RotationSpecification.newBuilder()
                                      .addAllSeals(List.of(DigestSeal.construct(digest), DigestSeal.construct(digest),
                                                           CoordinatesSeal.construct(event))));

        i.rotate();
    }

    @Test
    public void newIdentifier() throws Exception {
        Stereotomy controller = new StereotomyImpl(ks, kel, secureRandom);

        ControlledIdentifier<? extends Identifier> identifier = controller.newIdentifier().get();

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
        assertNull(kel.getKeyEvent(identifier.getLastEvent()).get());

        // delegation
        assertFalse(identifier.getDelegatingIdentifier().isPresent());
        assertFalse(identifier.isDelegated());
    }

    @Test
    public void newIdentifierFromIdentifier() throws Exception {
        Stereotomy controller = new StereotomyImpl(ks, kel, secureRandom);
        ControlledIdentifier<? extends Identifier> base = controller.newIdentifier().get();

        ControlledIdentifier<? extends Identifier> identifier = base.newIdentifier(IdentifierSpecification.newBuilder())
                                                                    .get();

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
        assertNull(kel.getKeyEvent(identifier.getLastEvent()).get());

        // delegation
        assertTrue(identifier.getDelegatingIdentifier().isPresent());
        assertTrue(identifier.isDelegated());

        var digest = DigestAlgorithm.BLAKE3_256.digest("digest seal".getBytes());
        var event = EventCoordinates.of(kel.getKeyEvent(identifier.getLastEstablishmentEvent()).get());
        var seals = List.of(DigestSeal.construct(digest), DigestSeal.construct(digest),
                            CoordinatesSeal.construct(event));

        identifier.rotate();
        identifier.seal(InteractionSpecification.newBuilder());
        identifier.rotate(RotationSpecification.newBuilder().addAllSeals(seals));
        identifier.seal(InteractionSpecification.newBuilder().addAllSeals(seals));
    }

    @Test
    public void provision() throws Exception {
        Stereotomy controller = new StereotomyImpl(ks, kel, secureRandom);
        var i = controller.newIdentifier().get();
        provision(i, controller);
        i.rotate();
        provision(i, controller);
    }

    void initializeKel() throws Exception {
        kel = new MemKERL(DigestAlgorithm.DEFAULT);
    }

    private void provision(ControlledIdentifier<?> i, Stereotomy controller) throws Exception {
        var now = Instant.now();
        var cwpk = i.provision(now, Duration.ofSeconds(100), SignatureAlgorithm.DEFAULT).get();
        assertNotNull(cwpk);
        var cert = cwpk.getX509Certificate();
        assertNotNull(cert);
        cert.checkValidity();
        var publicKey = cert.getPublicKey();
        assertNotNull(publicKey);
        var basicId = new BasicIdentifier(publicKey);

        var decoded = Stereotomy.decode(cert);
        assertFalse(decoded.isEmpty());

        assertEquals(i.getIdentifier(), decoded.get().identifier());
        final var qb64Id = qb64(basicId);

        assertTrue(i.getVerifier().get().verify(decoded.get().signature(), qb64Id));

        var verifiers = new Verifiers() {
            @Override
            public Optional<Verifier> verifierFor(Identifier identifier) {
                return (i.getIdentifier().equals(identifier)) ? i.getVerifier() : Optional.empty();
            }
        };
        new StereotomyValidator(verifiers).validate(cert);

        var privateKey = cwpk.getPrivateKey();
        assertNotNull(privateKey);
    }

}
