/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import static com.salesforce.apollo.crypto.QualifiedBase64.shortQb64;
import static com.salesforce.apollo.stereotomy.event.SigningThreshold.unweighted;

import java.security.KeyPair;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.Format;
import com.salesforce.apollo.stereotomy.event.InceptionEvent;
import com.salesforce.apollo.stereotomy.event.InceptionEvent.ConfigurationTrait;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.RotationEvent;
import com.salesforce.apollo.stereotomy.event.Seal;
import com.salesforce.apollo.stereotomy.event.SigningThreshold;
import com.salesforce.apollo.stereotomy.event.Version;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.BasicIdentifier;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.processing.KeyEventProcessor;
import com.salesforce.apollo.stereotomy.processing.MissingEstablishmentEventException;
import com.salesforce.apollo.stereotomy.specification.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.specification.InteractionSpecification;
import com.salesforce.apollo.stereotomy.specification.KeyConfigurationDigester;
import com.salesforce.apollo.stereotomy.specification.RotationSpecification;
import com.salesforce.apollo.stereotomy.specification.RotationSpecification.Builder;

/**
 * @author hal.hildebrand
 *
 */
public class Stereotomy {
    public interface ControllableIdentifier extends KeyState {
        void rotate();

        void rotate(List<Seal> of);

        void rotate(List<Seal> seals, RotationSpecification.Builder spec);

        void rotate(RotationSpecification.Builder spec);

        void seal(List<Seal> seals);

        void seal(List<Seal> seals, InteractionSpecification.Builder spec);

        EventSignature sign(KeyEvent event);

    }

    public interface EventFactory {

        InceptionEvent inception(Identifier identifier, IdentifierSpecification specification);

        KeyEvent interaction(InteractionSpecification specification);

        RotationEvent rotation(RotationSpecification specification);

    }

    public static class EventSignature {

        private final EventCoordinates          event;
        private final EventCoordinates          keyEstablishmentEvent;
        private final Map<Integer, JohnHancock> signatures;

        public EventSignature(EventCoordinates event, EventCoordinates keyEstablishmentEvent,
                Map<Integer, JohnHancock> signatures) {
            this.event = event;
            this.keyEstablishmentEvent = keyEstablishmentEvent;
            this.signatures = Collections.unmodifiableMap(signatures);
        }

        public EventCoordinates getEvent() {
            return this.event;
        }

        public EventCoordinates getKeyEstablishmentEvent() {
            return this.keyEstablishmentEvent;
        }

        public Map<Integer, JohnHancock> getSignatures() {
            return this.signatures;
        }

    }

    public interface StereotomyKeyStore {

        Optional<KeyPair> getKey(KeyCoordinates keyCoordinates);

        Optional<KeyPair> getNextKey(KeyCoordinates keyCoordinates);

        Optional<KeyPair> removeKey(KeyCoordinates keyCoordinates);

        Optional<KeyPair> removeNextKey(KeyCoordinates keyCoordinates);

        void storeKey(KeyCoordinates keyCoordinates, KeyPair keyPair);

        void storeNextKey(KeyCoordinates keyCoordinates, KeyPair keyPair);

    }

    private class ControllableIdentifierImpl implements ControllableIdentifier {
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
        public EventCoordinates getLastEstablishmentEvent() {
            return state.getLastEstablishmentEvent();
        }

        @Override
        public EventCoordinates getLastEvent() {
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
        public boolean isDelegated() {
            return state.isDelegated();
        }

        @Override
        public boolean isTransferable() {
            return state.isTransferable();
        }

        @Override
        public void rotate() {
            Stereotomy.this.rotate(getIdentifier());
        }

        @Override
        public void rotate(Builder spec) {
            Stereotomy.this.rotate(getIdentifier(), spec);
        }

        @Override
        public void rotate(List<Seal> seals) {
            Stereotomy.this.rotate(getIdentifier(), seals);
        }

        @Override
        public void rotate(List<Seal> seals, Builder spec) {
            Stereotomy.this.rotate(getIdentifier(), seals, spec);
        }

        @Override
        public void seal(List<Seal> seals) {
            Stereotomy.this.seal(getIdentifier(), seals);
        }

        @Override
        public void seal(List<Seal> seals, InteractionSpecification.Builder spec) {
            Stereotomy.this.seal(getIdentifier(), seals, spec);
        }

        @Override
        public EventSignature sign(KeyEvent event) {
            return Stereotomy.this.sign(getIdentifier(), event);
        }

        @Override
        public <T> T convertTo(Format format) {
            return state.convertTo(format);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(Stereotomy.class);

    public static final Version currentVersion() {
        return new Version() {

            @Override
            public int getMajor() {
                return 0;
            }

            @Override
            public int getMinor() {
                return 1;
            }
        };
    }

    private final SecureRandom       entropy;
    private final EventFactory       eventFactory;
    private final KeyEventLog        events;
    private final StereotomyKeyStore keyStore;
    private final KeyEventProcessor  processor;

    public Stereotomy(StereotomyKeyStore keyStore, KeyEventLog kel, KeyEventReceiptLog kerl, SecureRandom entropy) {
        this(keyStore, kel, kerl, entropy, new ProtobufEventFactory());
    }

    public Stereotomy(StereotomyKeyStore keyStore, KeyEventLog kel, KeyEventReceiptLog kerl, SecureRandom entropy,
            EventFactory eventFactory) {
        this(keyStore, kel, entropy, eventFactory, new KeyEventProcessor(kel, kerl));
    }

    public Stereotomy(StereotomyKeyStore keyStore, KeyEventLog events, SecureRandom entropy, EventFactory eventFactory,
            KeyEventProcessor processor) {
        this.keyStore = keyStore;
        this.entropy = entropy;
        this.processor = processor;
        this.eventFactory = eventFactory;
        this.events = events;
    }

    public ControllableIdentifier newDelegatedIdentifier(Identifier delegator) {
        return null;
    }

    public ControllableIdentifier newPrivateIdentifier(Identifier identifier) {
        return newPrivateIdentifier(identifier, IdentifierSpecification.newBuilder());

    }

    public ControllableIdentifier newPrivateIdentifier(Identifier identifier, IdentifierSpecification.Builder spec) {
        IdentifierSpecification.Builder specification = spec.clone();
        KeyPair initialKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        KeyPair nextKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        Digest nextKeys = KeyConfigurationDigester.digest(unweighted(1), List.of(nextKeyPair.getPublic()),
                                                          specification.getIdentifierDigestAlgorithm());

        specification.setKey(initialKeyPair.getPublic())
                     .setNextKeys(nextKeys)
                     .setSigner(0, initialKeyPair.getPrivate());

        InceptionEvent event = eventFactory.inception(identifier, specification.build());
        KeyState state = processor.process(event);
        if (state == null) {
            throw new IllegalStateException("Invalid event produced");
        }

        KeyCoordinates keyCoordinates = KeyCoordinates.of(event, 0);

        keyStore.storeKey(keyCoordinates, initialKeyPair);
        keyStore.storeNextKey(keyCoordinates, nextKeyPair);
        ControllableIdentifierImpl cid = new ControllableIdentifierImpl(state);

        log.info("New Private Identifier: {} prefix: {} coordinates: {} cur key: {} next key: {}", identifier,
                 cid.getIdentifier(), keyCoordinates, shortQb64(initialKeyPair.getPublic()),
                 shortQb64(nextKeyPair.getPublic()));
        return cid;
    }

    public ControllableIdentifier newPublicIdentifier(Identifier identifier, BasicIdentifier... witnesses) {
        return newPublicIdentifier(identifier, IdentifierSpecification.newBuilder(), witnesses);
    }

    public ControllableIdentifier newPublicIdentifier(Identifier identifier, IdentifierSpecification.Builder spec,
                                                      BasicIdentifier... witnesses) {
        IdentifierSpecification.Builder specification = spec.clone();

        var initialKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        var nextKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        var nextKeys = KeyConfigurationDigester.digest(unweighted(1), List.of(nextKeyPair.getPublic()),
                                                       specification.getNextKeysAlgorithm());

        specification.setKey(initialKeyPair.getPublic())
                     .setNextKeys(nextKeys)
                     .setWitnesses(Arrays.asList(witnesses))
                     .setSigner(0, initialKeyPair.getPrivate())
                     .build();

        InceptionEvent event = this.eventFactory.inception(identifier, specification.build());
        KeyState state = processor.process(event);
        if (state == null) {
            throw new IllegalStateException("Invalid event produced");
        }

        KeyCoordinates keyCoordinates = KeyCoordinates.of(event, 0);

        keyStore.storeKey(keyCoordinates, initialKeyPair);
        keyStore.storeNextKey(keyCoordinates, nextKeyPair);
        ControllableIdentifier cid = new ControllableIdentifierImpl(state);

        log.info("New Public Identifier: {} prefix: {} coordinates: {} cur key: {} next key: {}", identifier,
                 cid.getIdentifier(), keyCoordinates, shortQb64(initialKeyPair.getPublic()),
                 shortQb64(nextKeyPair.getPublic()));
        return cid;
    }

    public void rotate(Identifier identifier) {
        rotate(identifier, RotationSpecification.newBuilder());
    }

    public void rotate(Identifier identifier, Builder spec) {
        rotate(identifier, Collections.emptyList(), spec);
    }

    public KeyState rotate(Identifier identifier, List<Seal> seals) {
        return rotate(identifier, seals, RotationSpecification.newBuilder());
    }

    public KeyState rotate(Identifier identifier, List<Seal> seals, RotationSpecification.Builder spec) {
        RotationSpecification.Builder specification = spec.clone();

        KeyState state = events.getKeyState(identifier)
                               .orElseThrow(() -> new IllegalArgumentException(
                                       "identifier key state not found in key store"));

        // require single keys, nextKeys
        if (state.getNextKeyConfigurationDigest().isEmpty()) {
            throw new IllegalArgumentException("identifier cannot be rotated");
        }

        var lastEstablishing = events.getKeyEvent(state.getLastEstablishmentEvent())
                                     .orElseThrow(() -> new IllegalStateException("establishment event is missing"));
        EstablishmentEvent establishing = (EstablishmentEvent) lastEstablishing;
        var currentKeyCoordinates = KeyCoordinates.of(establishing, 0);

        KeyPair nextKeyPair = keyStore.getNextKey(currentKeyCoordinates)
                                      .orElseThrow(() -> new IllegalArgumentException(
                                              "next key pair for identifier not found in keystore: "
                                                      + currentKeyCoordinates));

        KeyPair newNextKeyPair = spec.getSignatureAlgorithm().generateKeyPair(entropy);
        Digest nextKeys = KeyConfigurationDigester.digest(unweighted(1), List.of(newNextKeyPair.getPublic()),
                                                          specification.getNextKeysAlgorithm());
        specification.setState(state)
                     .setKey(nextKeyPair.getPublic())
                     .setNextKeys(nextKeys)
                     .setSigner(0, nextKeyPair.getPrivate())
                     .addAllSeals(seals);

        RotationEvent event = eventFactory.rotation(specification.build());
        KeyState newState = processor.process(state, event);

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

    public void seal(Identifier identifier, List<Seal> seals) {
        seal(identifier, seals, InteractionSpecification.newBuilder());
    }

    public KeyState seal(Identifier identifier, List<Seal> seals, InteractionSpecification.Builder spec) {
        InteractionSpecification.Builder specification = spec.clone();
        KeyState state = events.getKeyState(identifier)
                               .orElseThrow(() -> new IllegalArgumentException("identifier not found in event store"));

        if (state == null) {
            throw new IllegalArgumentException("identifier not found in event store");
        }

        Optional<KeyEvent> lastEstablishmentEvent = events.getKeyEvent(state.getLastEstablishmentEvent());
        if (lastEstablishmentEvent.isEmpty()) {
            throw new MissingEstablishmentEventException(events.getKeyEvent(state.getCoordinates()).get(),
                    state.getLastEstablishmentEvent());
        }
        KeyCoordinates currentKeyCoordinates = KeyCoordinates.of((EstablishmentEvent) lastEstablishmentEvent.get(), 0);
        Optional<KeyPair> keyPair = keyStore.getKey(currentKeyCoordinates);

        if (keyPair.isEmpty()) {
            throw new IllegalArgumentException("key pair for identifier not found in keystore");
        }

        specification.setState(state).setSigner(0, keyPair.get().getPrivate()).setseals(seals);

        KeyEvent event = eventFactory.interaction(specification.build());
        KeyState newKeyState = processor.process(state, event);
        return newKeyState;
    }

    public EventSignature sign(Identifier identifier, KeyEvent event) {
        KeyState state = events.getKeyState(identifier)
                               .orElseThrow(() -> new IllegalArgumentException("identifier not found in event store"));
        var lastEstablishmentEvent = events.getKeyEvent(state.getLastEstablishmentEvent());
        if (lastEstablishmentEvent.isEmpty()) {
            throw new MissingEstablishmentEventException(events.getKeyEvent(state.getCoordinates()).get(),
                    state.getLastEstablishmentEvent());
        }
        KeyCoordinates keyCoords = KeyCoordinates.of((EstablishmentEvent) lastEstablishmentEvent.get(), 0);
        KeyPair keyPair = keyStore.getKey(keyCoords)
                                  .orElseThrow(() -> new IllegalArgumentException(
                                          "key pair not found for prefix: " + identifier));

        var ops = SignatureAlgorithm.lookup(keyPair.getPrivate());
        var signature = ops.sign(keyPair.getPrivate(), event.getBytes());

        return new EventSignature(event.getCoordinates(), lastEstablishmentEvent.get().getCoordinates(),
                Map.of(0, signature));
    }
}
