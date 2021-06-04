/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import static com.salesforce.apollo.stereotomy.event.SigningThreshold.unweighted;

import java.security.KeyPair;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.InceptionEvent;
import com.salesforce.apollo.stereotomy.event.InceptionEvent.ConfigurationTrait;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.RotationEvent;
import com.salesforce.apollo.stereotomy.event.Seal;
import com.salesforce.apollo.stereotomy.event.SigningThreshold;
import com.salesforce.apollo.stereotomy.identifier.BasicIdentifier;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.processing.KeyStateProcessor;
import com.salesforce.apollo.stereotomy.specification.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.specification.InteractionSpecification;
import com.salesforce.apollo.stereotomy.specification.RotationSpecification;
import com.salesforce.apollo.stereotomy.specification.RotationSpecification.Builder;

/**
 * @author hal.hildebrand
 *
 */
public class Sterotomy {
    public interface ControllableIdentifier extends KeyState {

        void rotate(Builder spec, SignatureAlgorithm signatureAlgorithm);

        void rotate(List<Seal> seals, Builder spec, SignatureAlgorithm signatureAlgorithm);

        void seal(List<Seal> seals, InteractionSpecification.Builder spec);

        EventSignature sign(KeyEvent event);

    }

    public class ControllableIdentifierImpl implements ControllableIdentifier {
        private final KeyState state;

        public ControllableIdentifierImpl(KeyState state) {
            this.state = state;
        }

        @Override
        public Set<ConfigurationTrait> configurationTraits() {
            return state.configurationTraits();
        }

        @Override
        public boolean equals(Object o) {
            return state.equals(o);
        }

        @Override
        public EventCoordinates getCoordinates() {
            return state.getCoordinates();
        }

        @Override
        public boolean getDelegated() {
            return state.getDelegated();
        }

        @Override
        public Optional<Identifier> getDelegatingIdentifier() {
            return state.getDelegatingIdentifier();
        }

        @Override
        public Digest getDigest() {
            return state.getDigest();
        }

        @Override
        public Identifier getIdentifier() {
            return state.getIdentifier();
        }

        @Override
        public List<PublicKey> getKeys() {
            return state.getKeys();
        }

        @Override
        public EstablishmentEvent getLastEstablishmentEvent() {
            return state.getLastEstablishmentEvent();
        }

        @Override
        public KeyEvent getLastEvent() {
            return state.getLastEvent();
        }

        @Override
        public Optional<Digest> getNextKeyConfigurationDigest() {
            return state.getNextKeyConfigurationDigest();
        }

        @Override
        public long getSequenceNumber() {
            return state.getSequenceNumber();
        }

        @Override
        public SigningThreshold getSigningThreshold() {
            return state.getSigningThreshold();
        }

        @Override
        public List<BasicIdentifier> getWitnesses() {
            return state.getWitnesses();
        }

        @Override
        public int getWitnessThreshold() {
            return state.getWitnessThreshold();
        }

        @Override
        public int hashCode() {
            return state.hashCode();
        }

        @Override
        public boolean isTransferable() {
            return state.isTransferable();
        }

        @Override
        public void rotate(Builder spec, SignatureAlgorithm signatureAlgorithm) {
            Sterotomy.this.rotate(getIdentifier(), spec, signatureAlgorithm);
        }

        @Override
        public void rotate(List<Seal> seals, Builder spec, SignatureAlgorithm signatureAlgorithm) {
            Sterotomy.this.rotate(getIdentifier(), seals, spec, signatureAlgorithm);
        }

        @Override
        public void seal(List<Seal> seals, InteractionSpecification.Builder spec) {
            Sterotomy.this.seal(getIdentifier(), seals, spec);
        }

        @Override
        public EventSignature sign(KeyEvent event) {
            return Sterotomy.this.sign(getIdentifier(), event);
        }
    }

    public interface EventFactory {

        InceptionEvent inception(IdentifierSpecification specification);

        KeyEvent interaction(InteractionSpecification specification);

        RotationEvent rotation(RotationSpecification specification);

    }

    public interface StereotomyKeyStore {

        Optional<KeyPair> getKey(KeyCoordinates keyCoordinates);

        Optional<KeyState> getKeyState(Identifier identifier);

        Optional<KeyPair> getNextKey(KeyCoordinates keyCoordinates);

        Optional<KeyPair> removeKey(KeyCoordinates keyCoordinates);

        Optional<KeyPair> removeNextKey(KeyCoordinates keyCoordinates);

        void storeKey(KeyCoordinates keyCoordinates, KeyPair keyPair);

        void storeNextKey(KeyCoordinates keyCoordinates, KeyPair keyPair);

    }

    private final SecureRandom       entropy;
    private final EventFactory       eventFactory;
    private final StereotomyKeyStore keyStore;
    private final KeyStateProcessor  processor;

    public Sterotomy(StereotomyKeyStore keyStore, SecureRandom entropy, KeyStateProcessor processor,
            EventFactory eventFactory) {
        this.keyStore = keyStore;
        this.entropy = entropy;
        this.processor = processor;
        this.eventFactory = eventFactory;
    }

    public ControllableIdentifier newDelegatedIdentifier(Identifier delegator) {
        return null;
    }

    public ControllableIdentifier newPrivateIdentifier(IdentifierSpecification.Builder spec,
                                                       SignatureAlgorithm signatureAllgorithm) {
        IdentifierSpecification.Builder specification = spec.clone();

        KeyPair initialKeyPair = signatureAllgorithm.generateKeyPair(entropy);
        KeyPair nextKeyPair = signatureAllgorithm.generateKeyPair(entropy);
        Digest nextKeys = KeyConfigurationDigester.digest(unweighted(1), List.of(nextKeyPair.getPublic()),
                                                          specification.getSelfAddressingDigestAlgorithm());

        specification.setKey(initialKeyPair.getPublic())
                     .setNextKeys(nextKeys)
                     .setSigner(0, initialKeyPair.getPrivate());

        InceptionEvent event = eventFactory.inception(specification.build());
        KeyState state = processor.apply(null, event);
        if (state == null) {
            throw new IllegalStateException("Invalid event produced");
        }

        KeyCoordinates keyCoordinates = KeyCoordinates.of(event, 0);

        keyStore.storeKey(keyCoordinates, initialKeyPair);
        keyStore.storeNextKey(keyCoordinates, nextKeyPair);

        return new ControllableIdentifierImpl(state);
    }

    public ControllableIdentifier newPublicIdentifier(IdentifierSpecification.Builder spec,
                                                      SignatureAlgorithm signatureAlgorithm,
                                                      BasicIdentifier... witnesses) {
        IdentifierSpecification.Builder specification = spec.clone();
        KeyPair initialKeyPair = signatureAlgorithm.generateKeyPair(entropy);
        KeyPair nextKeyPair = signatureAlgorithm.generateKeyPair(entropy);
        Digest nextKeys = KeyConfigurationDigester.digest(unweighted(1), List.of(nextKeyPair.getPublic()),
                                                          specification.getSelfAddressingDigestAlgorithm());
        specification.setKey(initialKeyPair.getPublic())
                     .setNextKeys(nextKeys)
                     .setSigner(0, initialKeyPair.getPrivate());

        InceptionEvent event = eventFactory.inception(specification.build());
        KeyState newState = processor.apply(null, event);
        if (newState == null) {
            throw new IllegalStateException("Invalid event produced");
        }

        KeyCoordinates keyCoordinates = KeyCoordinates.of(event, 0);

        keyStore.storeKey(keyCoordinates, initialKeyPair);
        keyStore.storeNextKey(keyCoordinates, nextKeyPair);

        return new ControllableIdentifierImpl(newState);
    }

    public KeyState rotate(Identifier identifier, List<Seal> seals, RotationSpecification.Builder spec,
                           SignatureAlgorithm signatureAlgorithm) {
        RotationSpecification.Builder specification = spec.clone();

        KeyState state = keyStore.getKeyState(identifier)
                                 .orElseThrow(() -> new IllegalArgumentException(
                                         "identifier not found in event store"));

        if (state == null) {
            throw new IllegalArgumentException("identifier state not found in event store");
        }

        // require single keys, nextKeys
        if (state.getNextKeyConfigurationDigest().isEmpty()) {
            throw new IllegalArgumentException("identifier cannot be rotated");
        }

        var currentKeyCoordinates = KeyCoordinates.of(state.getLastEstablishmentEvent(), 0);
        Optional<KeyPair> nextKeyPair = keyStore.getNextKey(currentKeyCoordinates);

        if (nextKeyPair.isEmpty()) {
            throw new IllegalArgumentException("next key pair for identifier not found in keystore");
        }

        KeyPair newNextKeyPair = signatureAlgorithm.generateKeyPair(entropy);
        Digest nextKeys = KeyConfigurationDigester.digest(unweighted(1), List.of(newNextKeyPair.getPublic()),
                                                          specification.getNextKeysAlgorithm());
        specification.setState(state)
                     .setKey(nextKeyPair.get().getPublic())
                     .setNextKeys(nextKeys)
                     .setSigner(0, nextKeyPair.get().getPrivate())
                     .addAllSeals(seals);
        RotationEvent event = eventFactory.rotation(specification.build());
        KeyState newState = processor.apply(state, event);

        KeyCoordinates nextKeyCoordinates = KeyCoordinates.of(event, 0);
        keyStore.storeKey(nextKeyCoordinates, nextKeyPair.get());
        keyStore.storeNextKey(nextKeyCoordinates, newNextKeyPair);
        keyStore.removeKey(currentKeyCoordinates);
        keyStore.removeNextKey(currentKeyCoordinates);

        return newState;
    }

    public KeyState rotate(Identifier identifier, RotationSpecification.Builder spec,
                           SignatureAlgorithm signatureAlgorithm) {
        return rotate(identifier, List.of(), spec, signatureAlgorithm);
    }

    public KeyState seal(Identifier identifier, List<Seal> seals, InteractionSpecification.Builder spec) {
        InteractionSpecification.Builder specification = spec.clone();
        KeyState state = keyStore.getKeyState(identifier)
                                 .orElseThrow(() -> new IllegalArgumentException(
                                         "identifier not found in event store"));

        if (state == null) {
            throw new IllegalArgumentException("identifier not found in event store");
        }

        KeyCoordinates currentKeyCoordinates = KeyCoordinates.of(state.getLastEstablishmentEvent(), 0);
        Optional<KeyPair> keyPair = keyStore.getKey(currentKeyCoordinates);

        if (keyPair.isEmpty()) {
            throw new IllegalArgumentException("key pair for identifier not found in keystore");
        }

        specification.setState(state).setSigner(0, keyPair.get().getPrivate()).setseals(seals);

        KeyEvent event = eventFactory.interaction(specification.build());
        return processor.apply(state, event);
    }

    public EventSignature sign(Identifier identifier, KeyEvent event) {
        KeyState state = keyStore.getKeyState(identifier)
                                 .orElseThrow(() -> new IllegalArgumentException(
                                         "identifier not found in event store"));

        KeyCoordinates keyCoords = KeyCoordinates.of(state.getLastEstablishmentEvent(), 0);
        KeyPair keyPair = keyStore.getKey(keyCoords)
                                  .orElseThrow(() -> new IllegalArgumentException(
                                          "key pair not found for prefix: " + identifier));

        var ops = SignatureAlgorithm.lookup(keyPair.getPrivate());
        var signature = ops.sign(event.getBytes(), keyPair.getPrivate());

        return new EventSignature(event.getCoordinates(), state.getLastEstablishmentEvent().getCoordinates(),
                Map.of(0, signature));
    }
}
