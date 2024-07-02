/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.cryptography.SignatureAlgorithm;
import com.salesforce.apollo.cryptography.SigningThreshold;
import com.salesforce.apollo.cryptography.SigningThreshold.Unweighted;
import com.salesforce.apollo.cryptography.Verifier;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.Seal;
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
import org.joou.ULong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static com.salesforce.apollo.stereotomy.identifier.QualifiedBase64Identifier.qb64;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author hal.hildebrand
 */
public class StereotomyTests {
    KERL.AppendKERL    kel;
    StereotomyKeyStore ks;
    SecureRandom       secureRandom;

    @BeforeEach
    public void before() throws Exception {
        secureRandom = SecureRandom.getInstance("SHA1PRNG");
        secureRandom.setSeed(new byte[] { 0 });
        initializeKel();
        // this makes the values of secureRandom deterministic
        ks = initializeKeyStore();
    }

    @Test
    public void identifierInteraction() throws Exception {
        Stereotomy controller = new StereotomyImpl(ks, kel, secureRandom);

        var i = controller.newIdentifier();

        var digest = DigestAlgorithm.BLAKE3_256.digest("digest seal".getBytes());
        var event = EventCoordinates.of(kel.getKeyEvent(i.getLastEstablishmentEvent()));
        var seals = List.of(DigestSeal.construct(digest), DigestSeal.construct(digest), Seal.construct(event));

        i.rotate();
        i.seal(InteractionSpecification.newBuilder());
        i.rotate(RotationSpecification.newBuilder().addAllSeals(seals));
        i.seal(InteractionSpecification.newBuilder().addAllSeals(seals));
        i.rotate();
        i.rotate();
        var iKerl = kel.kerl(i.getIdentifier());
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

        var i = controller.newIdentifier();

        var digest = DigestAlgorithm.BLAKE3_256.digest("digest seal".getBytes());
        var event = EventCoordinates.of(kel.getKeyEvent(i.getLastEstablishmentEvent()));

        i.rotate(RotationSpecification.newBuilder()
                                      .addAllSeals(List.of(DigestSeal.construct(digest), DigestSeal.construct(digest),
                                                           Seal.construct(event))));

        i.rotate();
    }

    @Test
    public void newIdentifier() throws Exception {
        Stereotomy controller = new StereotomyImpl(ks, kel, secureRandom);

        ControlledIdentifier<? extends Identifier> identifier = controller.newIdentifier();

        // identifier
        assertInstanceOf(SelfAddressingIdentifier.class, identifier.getIdentifier());
        var sap = (SelfAddressingIdentifier) identifier.getIdentifier();
        assertEquals(DigestAlgorithm.DEFAULT, sap.getDigest().getAlgorithm());
        assertEquals("9f207937484c3e47833f7f78d22974b3b543f6363c138ebeda20793c4a5c082b",
                     Hex.hex(sap.getDigest().getBytes()));

        assertEquals(1, ((Unweighted) identifier.getSigningThreshold()).getThreshold());

        // keys
        assertEquals(1, identifier.getKeys().size());
        assertNotNull(identifier.getKeys().get(0));

        EstablishmentEvent lastEstablishmentEvent = (EstablishmentEvent) kel.getKeyEvent(
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
        assertNull(kel.getKeyEvent(identifier.getLastEvent()));

        assertTrue(identifier.getDelegatingIdentifier().isPresent());
        assertEquals(Identifier.NONE, identifier.getDelegatingIdentifier().get());
        assertFalse(identifier.isDelegated());
    }

    @Test
    public void newIdentifierFromIdentifier() throws Exception {
        Stereotomy controller = new StereotomyImpl(ks, kel, secureRandom);
        ControlledIdentifier<? extends Identifier> base = controller.newIdentifier();

        ControlledIdentifier<? extends Identifier> identifier = base.newIdentifier(
        IdentifierSpecification.newBuilder());

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

        EstablishmentEvent lastEstablishmentEvent = (EstablishmentEvent) kel.getKeyEvent(
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
        assertNull(kel.getKeyEvent(identifier.getLastEvent()));

        // delegation
        assertTrue(identifier.getDelegatingIdentifier().isPresent());
        assertNotEquals(Identifier.NONE, identifier.getDelegatingIdentifier());
        assertTrue(identifier.isDelegated());

        var digest = DigestAlgorithm.BLAKE3_256.digest("digest seal".getBytes());
        var event = EventCoordinates.of(kel.getKeyEvent(identifier.getLastEstablishmentEvent()));
        var seals = List.of(DigestSeal.construct(digest), DigestSeal.construct(digest), Seal.construct(event));

        identifier.rotate();
        identifier.seal(InteractionSpecification.newBuilder());
        identifier.rotate(RotationSpecification.newBuilder().addAllSeals(seals));
        identifier.seal(InteractionSpecification.newBuilder().addAllSeals(seals));
    }

    @Test
    public void provision() throws Exception {
        Stereotomy controller = new StereotomyImpl(ks, kel, secureRandom);
        var i = controller.newIdentifier();
        provision(i, controller);
        i.rotate();
        provision(i, controller);
    }

    void initializeKel() throws Exception {
        kel = new MemKERL(DigestAlgorithm.DEFAULT);
    }

    protected StereotomyKeyStore initializeKeyStore() {
        return new MemKeyStore();
    }

    private void provision(ControlledIdentifier<?> identifier, Stereotomy controller) throws Exception {
        var now = Instant.now();
        var cwpk = identifier.provision(now, Duration.ofSeconds(100), SignatureAlgorithm.DEFAULT);
        assertNotNull(cwpk);
        var cert = cwpk.getX509Certificate();
        assertNotNull(cert);
        cert.checkValidity();
        var publicKey = cert.getPublicKey();
        assertNotNull(publicKey);
        var basicId = new BasicIdentifier(publicKey);

        var decoded = Stereotomy.decode(cert);
        assertFalse(decoded.isEmpty());

        assertEquals(identifier.getIdentifier(), decoded.get().identifier());
        final var qb64Id = qb64(basicId);

        assertTrue(identifier.getVerifier().get().verify(decoded.get().signature(), qb64Id));

        var verifiers = new Verifiers() {
            @Override
            public Optional<Verifier> verifierFor(EventCoordinates coordinates) {
                return (identifier.getIdentifier().equals(coordinates.getIdentifier())) ? identifier.getVerifier()
                                                                                        : Optional.empty();
            }

            @Override
            public Optional<Verifier> verifierFor(Identifier id) {
                return (identifier.getIdentifier().equals(id)) ? identifier.getVerifier() : Optional.empty();
            }
        };
        new StereotomyValidator(verifiers).validate(cert); // exception means failure

        var privateKey = cwpk.getPrivateKey();
        assertNotNull(privateKey);
    }

}
