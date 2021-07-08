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
import java.nio.ByteBuffer;
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

import com.google.protobuf.ByteString;
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
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.InteractionSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.KeyConfigurationDigester;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification.Builder;
import com.salesforce.apollo.stereotomy.processing.KeyEventProcessor;
import com.salesforce.apollo.stereotomy.processing.MissingEstablishmentEventException;
import com.salesforce.apollo.utils.BbBackedInputStream;

/**
 * @author hal.hildebrand
 *
 */
public class Stereotomy {
    public interface ControllableIdentifier extends KeyState {
        void rotate();

        void rotate(List<Seal> seals);

        void rotate(RotationSpecification.Builder spec);

        void seal(InteractionSpecification.Builder spec);

        void seal(List<Seal> seals);

        default JohnHancock sign(byte[]... buffs) {
            return sign(BbBackedInputStream.aggregate(buffs));
        }

        default JohnHancock sign(ByteBuffer... buffs) {
            return sign(BbBackedInputStream.aggregate(buffs));
        }

        default JohnHancock sign(ByteString... buffs) {
            return sign(BbBackedInputStream.aggregate(buffs));
        }

        JohnHancock sign(InputStream is);

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

        Optional<PublicKey> getPublicKey(KeyCoordinates keyCoordinates);

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
        public <T> T convertTo(Format format) {
            return state.convertTo(format);
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
        public void seal(InteractionSpecification.Builder spec) {
            Stereotomy.this.seal(getIdentifier(), spec);
        }

        @Override
        public void seal(List<Seal> seals) {
            Stereotomy.this.seal(getIdentifier(), seals);
        }

        @Override
        public JohnHancock sign(InputStream is) {
            return Stereotomy.this.sign(getIdentifier(), is);
        }

        @Override
        public EventSignature sign(KeyEvent event) {
            return Stereotomy.this.sign(getIdentifier(), event);
        }
    }

    private class LimitedStereotomy implements LimitedController {

        @Override
        public JohnHancock sign(SelfAddressingIdentifier identifier, InputStream message) {
            return Stereotomy.this.sign(identifier, message);
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

    private final SecureRandom entropy;

    private final EventFactory       eventFactory;
    private final KERL               kerl;
    private final StereotomyKeyStore keyStore;
    private final LimitedController  limitedController = new LimitedStereotomy();
    private final KeyEventProcessor  processor;

    public Stereotomy(StereotomyKeyStore keyStore, KERL kerl, SecureRandom entropy) {
        this(keyStore, kerl, entropy, new ProtobufEventFactory());
    }

    public Stereotomy(StereotomyKeyStore keyStore, KERL kerl, SecureRandom entropy, EventFactory eventFactory) {
        this(keyStore, kerl, entropy, eventFactory, new KeyEventProcessor(kerl));
    }

    public Stereotomy(StereotomyKeyStore keyStore, KERL kerl, SecureRandom entropy, EventFactory eventFactory,
            KeyEventProcessor processor) {
        this.keyStore = keyStore;
        this.entropy = entropy;
        this.processor = processor;
        this.eventFactory = eventFactory;
        this.kerl = kerl;
    }

    public LimitedController getLimitedController() {
        return limitedController;
    }

    public ControllableIdentifier newDelegatedIdentifier(Identifier delegator) {
        return null;
    }

    public ControllableIdentifier newIdentifier(Identifier identifier, BasicIdentifier... witnesses) {
        return newIdentifier(identifier, IdentifierSpecification.newBuilder(), witnesses);
    }

    public ControllableIdentifier newIdentifier(Identifier identifier, IdentifierSpecification.Builder spec,
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

        log.info("New {} Identifier: {} prefix: {} coordinates: {} cur key: {} next key: {}",
                 witnesses.length == 0 ? "Private" : "Public", identifier, cid.getIdentifier(), keyCoordinates,
                 shortQb64(initialKeyPair.getPublic()), shortQb64(nextKeyPair.getPublic()));
        return cid;
    }

    public void rotate(Identifier identifier) {
        rotate(identifier, RotationSpecification.newBuilder());
    }

    public KeyState rotate(Identifier identifier, List<Seal> seals) {
        Builder builder = RotationSpecification.newBuilder().addAllSeals(seals);
        return rotate(identifier, builder);
    }

    public KeyState rotate(Identifier identifier, RotationSpecification.Builder spec) {
        RotationSpecification.Builder specification = spec.clone();

        KeyState state = kerl.getKeyState(identifier)
                             .orElseThrow(() -> new IllegalArgumentException(
                                     "identifier key state not found in key store"));

        // require single keys, nextKeys
        if (state.getNextKeyConfigurationDigest().isEmpty()) {
            throw new IllegalArgumentException("identifier cannot be rotated");
        }

        var lastEstablishing = kerl.getKeyEvent(state.getLastEstablishmentEvent())
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
                     .setSigner(0, nextKeyPair.getPrivate());

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

    public KeyState seal(Identifier identifier, InteractionSpecification.Builder spec) {
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

        specification.setState(state).setSigner(0, keyPair.get().getPrivate());

        KeyEvent event = eventFactory.interaction(specification.build());
        KeyState newKeyState = processor.process(state, event);
        return newKeyState;
    }

    public void seal(Identifier identifier, List<Seal> seals) {
        InteractionSpecification.Builder builder = InteractionSpecification.newBuilder();
        builder.setseals(seals);
        seal(identifier, builder);
    }

    public JohnHancock sign(Identifier identifier, InputStream message) {
        KeyState state = kerl.getKeyState(identifier)
                             .orElseThrow(() -> new IllegalArgumentException("Identifier not found in KEL"));
        var lastEstablishmentEvent = kerl.getKeyEvent(state.getLastEstablishmentEvent());
        if (lastEstablishmentEvent.isEmpty()) {
            throw new MissingEstablishmentEventException(kerl.getKeyEvent(state.getCoordinates()).get(),
                    state.getLastEstablishmentEvent());
        }
        KeyCoordinates keyCoords = KeyCoordinates.of((EstablishmentEvent) lastEstablishmentEvent.get(), 0);
        KeyPair keyPair = keyStore.getKey(keyCoords)
                                  .orElseThrow(() -> new IllegalArgumentException(
                                          "Key pair not found for prefix: " + identifier));

        var ops = SignatureAlgorithm.lookup(keyPair.getPrivate());
        return ops.sign(keyPair.getPrivate(), message);
    }

    public EventSignature sign(Identifier identifier, KeyEvent event) {
        KeyState state = kerl.getKeyState(identifier)
                             .orElseThrow(() -> new IllegalArgumentException("Identifier not found in KEL"));
        var lastEstablishmentEvent = kerl.getKeyEvent(state.getLastEstablishmentEvent());
        if (lastEstablishmentEvent.isEmpty()) {
            throw new MissingEstablishmentEventException(kerl.getKeyEvent(state.getCoordinates()).get(),
                    state.getLastEstablishmentEvent());
        }
        KeyCoordinates keyCoords = KeyCoordinates.of((EstablishmentEvent) lastEstablishmentEvent.get(), 0);
        KeyPair keyPair = keyStore.getKey(keyCoords)
                                  .orElseThrow(() -> new IllegalArgumentException(
                                          "Key pair not found for prefix: " + identifier));

        var ops = SignatureAlgorithm.lookup(keyPair.getPrivate());
        var signature = ops.sign(keyPair.getPrivate(), event.getBytes());

        return new EventSignature(event.getCoordinates(), lastEstablishmentEvent.get().getCoordinates(),
                Map.of(0, signature));
    }
}
