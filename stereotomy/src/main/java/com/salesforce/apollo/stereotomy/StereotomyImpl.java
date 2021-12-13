/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import static com.salesforce.apollo.crypto.QualifiedBase64.shortQb64;
import static com.salesforce.apollo.stereotomy.event.SigningThreshold.unweighted;

import java.io.InputStream;
import java.security.KeyPair;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.EventFactory;
import com.salesforce.apollo.stereotomy.event.Format;
import com.salesforce.apollo.stereotomy.event.InceptionEvent;
import com.salesforce.apollo.stereotomy.event.InceptionEvent.ConfigurationTrait;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.RotationEvent;
import com.salesforce.apollo.stereotomy.event.Seal;
import com.salesforce.apollo.stereotomy.event.SigningThreshold;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.BasicIdentifier;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.InteractionSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification.Builder;
import com.salesforce.apollo.stereotomy.processing.MissingEstablishmentEventException;

/**
 * @author hal.hildebrand
 *
 */
public class StereotomyImpl implements Stereotomy {
    private class BoundControllableIdentifier extends ControllableIdentifierImpl {
        private final KeyState state;

        public BoundControllableIdentifier(KeyState state) {
            super(state.getIdentifier());
            this.state = state;
        }

        @Override
        public ControllableIdentifier bind() {
            return this;
        }

        @Override
        KeyState getState() {
            return state;
        }

    }

    private class ControllableIdentifierImpl implements ControllableIdentifier {
        private final Identifier identifier;

        public ControllableIdentifierImpl(Identifier identifier) {
            this.identifier = identifier;
        }

        @Override
        public ControllableIdentifier bind() {
            return new BoundControllableIdentifier(getState());
        }

        @Override
        public Set<ConfigurationTrait> configurationTraits() {
            return getState().configurationTraits();
        }

        @Override
        public <T> T convertTo(Format format) {
            return getState().convertTo(format);
        }

        @Override
        public boolean equals(Object o) {
            return getState().equals(o);
        }

        @Override
        public byte[] getBytes() {
            return getState().getBytes();
        }

        @Override
        public EventCoordinates getCoordinates() {
            return getState().getCoordinates();
        }

        @Override
        public Optional<Identifier> getDelegatingIdentifier() {
            return getState().getDelegatingIdentifier();
        }

        @Override
        public Digest getDigest() {
            return getState().getDigest();
        }

        @Override
        public Identifier getIdentifier() {
            return identifier;
        }

        @Override
        public List<PublicKey> getKeys() {
            return getState().getKeys();
        }

        @Override
        public EventCoordinates getLastEstablishmentEvent() {
            return getState().getLastEstablishmentEvent();
        }

        @Override
        public EventCoordinates getLastEvent() {
            return getState().getLastEvent();
        }

        @Override
        public Optional<Digest> getNextKeyConfigurationDigest() {
            return getState().getNextKeyConfigurationDigest();
        }

        @Override
        public long getSequenceNumber() {
            return getState().getSequenceNumber();
        }

        @Override
        public Signer getSigner(int keyIndex) {
            return StereotomyImpl.this.getSigner(keyIndex, getIdentifier());
        }

        @Override
        public SigningThreshold getSigningThreshold() {
            return getState().getSigningThreshold();
        }

        @Override
        public Verifier getVerifier() {
            return StereotomyImpl.this.getVerifier(getIdentifier());
        }

        @Override
        public List<BasicIdentifier> getWitnesses() {
            return getState().getWitnesses();
        }

        @Override
        public int getWitnessThreshold() {
            return getState().getWitnessThreshold();
        }

        @Override
        public int hashCode() {
            return getState().hashCode();
        }

        @Override
        public boolean isDelegated() {
            return getState().isDelegated();
        }

        @Override
        public boolean isTransferable() {
            return getState().isTransferable();
        }

        @Override
        public void rotate() {
            StereotomyImpl.this.rotate(getIdentifier());
        }

        @Override
        public void rotate(Builder spec) {
            StereotomyImpl.this.rotate(getIdentifier(), spec);
        }

        @Override
        public void rotate(List<Seal> seals) {
            StereotomyImpl.this.rotate(getIdentifier(), seals);
        }

        @Override
        public void seal(InteractionSpecification.Builder spec) {
            StereotomyImpl.this.seal(getIdentifier(), spec);
        }

        @Override
        public void seal(List<Seal> seals) {
            StereotomyImpl.this.seal(getIdentifier(), seals);
        }

        @Override
        public JohnHancock sign(InputStream is) {
            return StereotomyImpl.this.sign(getIdentifier(), is);
        }

        @Override
        public EventSignature sign(KeyEvent event) {
            return StereotomyImpl.this.sign(getIdentifier(), event);
        }

        KeyState getState() {
            return kerl.getKeyState(identifier).get();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(StereotomyImpl.class);

    protected final SecureRandom entropy;
    protected final EventFactory eventFactory;
    protected final KERL         kerl;

    private final StereotomyKeyStore keyStore;

    public StereotomyImpl(StereotomyKeyStore keyStore, KERL kerl, SecureRandom entropy) {
        this(keyStore, kerl, entropy, new ProtobufEventFactory());
    }

    public StereotomyImpl(StereotomyKeyStore keyStore, KERL kerl, SecureRandom entropy, EventFactory eventFactory) {
        this.keyStore = keyStore;
        this.entropy = entropy;
        this.eventFactory = eventFactory;
        this.kerl = kerl;
    }

    @Override
    public ControllableIdentifier controlOf(Identifier identifier) {
        final var lookup = kerl.getKeyState(identifier);
        if (lookup.isEmpty()) {
            throw new IllegalArgumentException("Identifier has no key state: " + identifier);
        }
        return new ControllableIdentifierImpl(identifier);
    }

    @Override
    public ControllableIdentifier newDelegatedIdentifier(Identifier delegator) {
        return null;
    }

    @Override
    public ControllableIdentifier newIdentifier(Identifier identifier, BasicIdentifier... witnesses) {
        return newIdentifier(identifier, IdentifierSpecification.newBuilder().setWitnesses(Arrays.asList(witnesses)));
    }

    @Override
    public ControllableIdentifier newIdentifier(Identifier identifier, IdentifierSpecification.Builder spec) {
        IdentifierSpecification.Builder specification = spec.clone();

        var initialKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        var nextKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);

        specification.addKey(initialKeyPair.getPublic()).setSigningThreshold(unweighted(1))
                     .setNextKeys(List.of(nextKeyPair.getPublic())).setSigner(0, initialKeyPair.getPrivate());

        InceptionEvent event = this.eventFactory.inception(identifier, specification.build());
        KeyState state = kerl.append(event);
        if (state == null) {
            throw new IllegalStateException("Invalid event produced");
        }

        KeyCoordinates keyCoordinates = KeyCoordinates.of(event, 0);

        keyStore.storeKey(keyCoordinates, initialKeyPair);
        keyStore.storeNextKey(keyCoordinates, nextKeyPair);
        ControllableIdentifier cid = new ControllableIdentifierImpl(state.getIdentifier());

        log.info("New {} Identifier: {} prefix: {} coordinates: {} cur key: {} next key: {}",
                 specification.getWitnesses().isEmpty() ? "Private" : "Public", identifier, cid.getIdentifier(),
                 keyCoordinates, shortQb64(initialKeyPair.getPublic()), shortQb64(nextKeyPair.getPublic()));
        return cid;
    }

    protected Signer getSigner(int keyIndex, Identifier identifier) {
        KeyState state = kerl.getKeyState(identifier)
                             .orElseThrow(() -> new IllegalArgumentException("Identifier not found in KERL"));
        KeyPair keyPair = getKeyPair(identifier, state, kerl.getKeyEvent(state.getLastEstablishmentEvent()));

        return new Signer.SignerImpl(keyIndex, keyPair.getPrivate());
    }

    protected Verifier getVerifier(Identifier identifier) {
        KeyState state = kerl.getKeyState(identifier)
                             .orElseThrow(() -> new IllegalArgumentException("Identifier not found in KERL"));
        KeyPair keyPair = getKeyPair(identifier, state, kerl.getKeyEvent(state.getLastEstablishmentEvent()));

        var ops = SignatureAlgorithm.lookup(keyPair.getPrivate());
        return new Verifier.DefaultVerifier(ops, keyPair.getPublic());
    }

    protected void rotate(Identifier identifier) {
        rotate(identifier, RotationSpecification.newBuilder());
    }

    protected KeyState rotate(Identifier identifier, List<Seal> seals) {
        Builder builder = RotationSpecification.newBuilder().addAllSeals(seals);
        return rotate(identifier, builder);
    }

    protected KeyState rotate(Identifier identifier, RotationSpecification.Builder spec) {
        RotationSpecification.Builder specification = spec.clone();

        KeyState state = kerl.getKeyState(identifier)
                             .orElseThrow(() -> new IllegalArgumentException("identifier key state not found in key store"));

        if (state.getNextKeyConfigurationDigest().isEmpty()) {
            throw new IllegalArgumentException("identifier cannot be rotated");
        }

        var lastEstablishing = kerl.getKeyEvent(state.getLastEstablishmentEvent())
                                   .orElseThrow(() -> new IllegalStateException("establishment event is missing"));
        EstablishmentEvent establishing = (EstablishmentEvent) lastEstablishing;
        var currentKeyCoordinates = KeyCoordinates.of(establishing, 0);

        KeyPair nextKeyPair = keyStore.getNextKey(currentKeyCoordinates)
                                      .orElseThrow(() -> new IllegalArgumentException("next key pair for identifier not found in keystore: "
                                      + currentKeyCoordinates));

        KeyPair newNextKeyPair = spec.getSignatureAlgorithm().generateKeyPair(entropy);

        specification.setSigningThreshold(unweighted(1)).setIdentifier(identifier)
                     .setDigestAlgorithm(kerl.getDigestAlgorithm()).setCurrentCoords(state.getCoordinates())
                     .setCurrentDigest(state.getDigest()).setKey(nextKeyPair.getPublic())
                     .setNextKeys(List.of(newNextKeyPair.getPublic())).setSigner(0, nextKeyPair.getPrivate());

        RotationEvent event = eventFactory.rotation(specification.build());
        KeyState newState = kerl.append(event);

        KeyCoordinates nextKeyCoordinates = KeyCoordinates.of(event, 0);

        keyStore.storeKey(nextKeyCoordinates, nextKeyPair);
        keyStore.storeNextKey(nextKeyCoordinates, newNextKeyPair);

        keyStore.removeKey(currentKeyCoordinates);
        keyStore.removeNextKey(currentKeyCoordinates);

        log.info("Rotated Identifier: {} coordinates: {} cur key: {} next key: {} old coordinates: {}", identifier,
                 nextKeyCoordinates, shortQb64(nextKeyPair.getPublic()), shortQb64(newNextKeyPair.getPublic()),
                 currentKeyCoordinates);

        return newState;
    }

    protected KeyState seal(Identifier identifier, InteractionSpecification.Builder spec) {
        InteractionSpecification.Builder specification = spec.clone();
        KeyState state = kerl.getKeyState(identifier)
                             .orElseThrow(() -> new IllegalArgumentException("Identifier not found in KEL"));

        if (state == null) {
            throw new IllegalArgumentException("Identifier not found in KEL");
        }

        Optional<KeyEvent> lastEstablishmentEvent = kerl.getKeyEvent(state.getLastEstablishmentEvent());
        if (lastEstablishmentEvent.isEmpty()) {
            throw new MissingEstablishmentEventException(kerl.getKeyEvent(state.getCoordinates()).get(),
                                                         state.getLastEstablishmentEvent());
        }
        KeyCoordinates currentKeyCoordinates = KeyCoordinates.of((EstablishmentEvent) lastEstablishmentEvent.get(), 0);
        Optional<KeyPair> keyPair = keyStore.getKey(currentKeyCoordinates);

        if (keyPair.isEmpty()) {
            throw new IllegalArgumentException("Key pair for identifier not found in keystore");
        }

        specification.setPriorEventDigest(state.getDigest()).setLastEvent(state.getLastEvent())
                     .setIdentifier(identifier).setSigner(0, keyPair.get().getPrivate());

        KeyEvent event = eventFactory.interaction(specification.build());
        KeyState newKeyState = kerl.append(event);
        return newKeyState;
    }

    protected void seal(Identifier identifier, List<Seal> seals) {
        InteractionSpecification.Builder builder = InteractionSpecification.newBuilder();
        builder.setseals(seals);
        seal(identifier, builder);
    }

    protected JohnHancock sign(Identifier identifier, InputStream message) {
        KeyState state = kerl.getKeyState(identifier)
                             .orElseThrow(() -> new IllegalArgumentException("Identifier not found in KERL"));
        KeyPair keyPair = getKeyPair(identifier, state, kerl.getKeyEvent(state.getLastEstablishmentEvent()));

        var ops = SignatureAlgorithm.lookup(keyPair.getPrivate());
        return ops.sign(keyPair.getPrivate(), message);
    }

    protected EventSignature sign(Identifier identifier, KeyEvent event) {
        KeyState state = kerl.getKeyState(identifier)
                             .orElseThrow(() -> new IllegalArgumentException("Identifier not found in KERL"));

        KeyPair keyPair = getKeyPair(identifier, state, kerl.getKeyEvent(state.getLastEstablishmentEvent()));

        var ops = SignatureAlgorithm.lookup(keyPair.getPrivate());
        var signature = ops.sign(keyPair.getPrivate(), event.getBytes());

        return new EventSignature(event.getCoordinates(),
                                  kerl.getKeyEvent(state.getLastEstablishmentEvent()).get().getCoordinates(),
                                  Map.of(0, signature));
    }

    private KeyPair getKeyPair(Identifier identifier, KeyCoordinates keyCoords) {
        return keyStore.getKey(keyCoords)
                       .orElseThrow(() -> new IllegalArgumentException("Key pair not found for prefix: " + identifier));
    }

    private KeyPair getKeyPair(Identifier identifier, KeyState state, Optional<KeyEvent> lastEstablishmentEvent) {
        if (lastEstablishmentEvent.isEmpty()) {
            throw new MissingEstablishmentEventException(kerl.getKeyEvent(state.getCoordinates()).get(),
                                                         state.getLastEstablishmentEvent());
        }
        KeyCoordinates keyCoords = KeyCoordinates.of((EstablishmentEvent) lastEstablishmentEvent.get(), 0);
        KeyPair keyPair = getKeyPair(identifier, keyCoords);
        return keyPair;
    }
}
