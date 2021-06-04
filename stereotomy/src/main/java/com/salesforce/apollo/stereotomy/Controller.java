/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import static com.salesforce.apollo.stereotomy.event.SigningThreshold.unweighted;

import java.security.KeyPair;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.stereotomy.event.InceptionEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.RotationEvent;
import com.salesforce.apollo.stereotomy.event.Seal;
import com.salesforce.apollo.stereotomy.identifier.BasicIdentifier;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.specification.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.specification.InteractionSpecification;
import com.salesforce.apollo.stereotomy.specification.RotationSpecification;

/**
 * @author hal.hildebrand
 *
 */
public class Controller {
    public interface ControllerKeyStore {

        Optional<KeyPair> getKey(KeyCoordinates keyCoordinates);

        Optional<KeyState> getKeyState(Identifier identifier);

        Optional<KeyPair> getNextKey(KeyCoordinates keyCoordinates);

        Optional<KeyPair> removeKey(KeyCoordinates keyCoordinates);

        Optional<KeyPair> removeNextKey(KeyCoordinates keyCoordinates);

        void storeKey(KeyCoordinates keyCoordinates, KeyPair keyPair);

        void storeNextKey(KeyCoordinates keyCoordinates, KeyPair keyPair);

    }

    public interface EventFactory {

        InceptionEvent inception(IdentifierSpecification specification);

        KeyEvent interaction(InteractionSpecification specification);

        RotationEvent rotation(RotationSpecification specification);

    }

    private final SecureRandom                             entropy;
    private final EventFactory                             eventFactory;
    private final Function<KeyEvent, KeyState>             eventValidator;
    private final ControllerKeyStore                       keyStore;
    private final BiFunction<KeyState, KeyEvent, KeyState> processor;

    public Controller(ControllerKeyStore keyStore, SecureRandom entropy,
            BiFunction<KeyState, KeyEvent, KeyState> processor, Function<KeyEvent, KeyState> eventValidator,
            EventFactory eventFactory) {
        this.keyStore = keyStore;
        this.entropy = entropy;
        this.eventValidator = eventValidator;
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
        KeyState valid = eventValidator.apply(event);
        if (valid == null) {
            throw new IllegalStateException("Invalid event produced");
        }

        KeyCoordinates keyCoordinates = KeyCoordinates.of(event, 0);

        keyStore.storeKey(keyCoordinates, initialKeyPair);
        keyStore.storeNextKey(keyCoordinates, nextKeyPair);

        KeyState state = processor.apply(null, event);

        return new ControllableIdentifierImpl(this, state);
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
        KeyState newState = eventValidator.apply(event);

        KeyCoordinates keyCoordinates = KeyCoordinates.of(event, 0);

        keyStore.storeKey(keyCoordinates, initialKeyPair);
        keyStore.storeNextKey(keyCoordinates, nextKeyPair);

        return new ControllableIdentifierImpl(this, newState);
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
        KeyState newState = eventValidator.apply(event);

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
        return eventValidator.apply(event);
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
