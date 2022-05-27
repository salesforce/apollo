/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;
import static com.salesforce.apollo.crypto.SigningThreshold.unweighted;
import static com.salesforce.apollo.stereotomy.identifier.QualifiedBase64Identifier.qb64;

import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.joou.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.stereotomy.event.proto.KeyState_;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.Signer.SignerImpl;
import com.salesforce.apollo.crypto.SigningThreshold;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.crypto.cert.BcX500NameDnImpl;
import com.salesforce.apollo.crypto.cert.CertExtension;
import com.salesforce.apollo.crypto.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.crypto.cert.Certificates;
import com.salesforce.apollo.stereotomy.KERL.EventWithAttachments;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent.AttachmentImpl;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.EventFactory;
import com.salesforce.apollo.stereotomy.event.Format;
import com.salesforce.apollo.stereotomy.event.InceptionEvent;
import com.salesforce.apollo.stereotomy.event.InceptionEvent.ConfigurationTrait;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.RotationEvent;
import com.salesforce.apollo.stereotomy.event.Seal.EventSeal;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.BasicIdentifier;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.InteractionSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification.Builder;

/**
 * Direct mode implementation of a Stereotomy controller. This controller keeps
 * it's own KEL/KERL and does not cooperate with other controllers
 * 
 * @author hal.hildebrand
 *
 */
public class StereotomyImpl implements Stereotomy {
    private abstract static class AbstractCtrlId implements KeyState {

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
            return getState().getIdentifier();
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
        public ULong getSequenceNumber() {
            return getState().getSequenceNumber();
        }

        @Override
        public SigningThreshold getSigningThreshold() {
            return getState().getSigningThreshold();
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

        abstract KeyState getState();

    }

    private class BoundControllableIdentifier<D extends Identifier> extends AbstractCtrlId
                                             implements BoundIdentifier<D> {
        private volatile KeyState state;

        public BoundControllableIdentifier(KeyState state) {
            this.state = state;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!super.equals(obj) || !(obj instanceof BoundControllableIdentifier<?> other) ||
                !getEnclosingInstance().equals(other.getEnclosingInstance())) {
                return false;
            }
            return Objects.equals(getState(), other.getState());
        }

        @Override
        @SuppressWarnings("unchecked")
        public D getIdentifier() {
            return (D) super.getIdentifier();
        }

        @Override
        public Optional<EstablishmentEvent> getLastEstablishingEvent() {
            return StereotomyImpl.this.getLastEstablishingEvent(getState());
        }

        @Override
        public Optional<Verifier> getVerifier() {
            return Optional.of(new Verifier.DefaultVerifier(getState().getKeys()));
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result + getEnclosingInstance().hashCode();
            result = prime * result + Objects.hash(getState());
            return result;
        }

        @Override
        public KeyState_ toKeyState_() {
            return getState().toKeyState_();
        }

        @Override
        KeyState getState() {
            KeyState current = state;
            return current;
        }

        void setState(KeyState delegatingState) {
            this.state = delegatingState;
        }

        private StereotomyImpl getEnclosingInstance() {
            return StereotomyImpl.this;
        }

    }

    private class ControlledIdentifierImpl<D extends Identifier> extends BoundControllableIdentifier<D>
                                          implements ControlledIdentifier<D> {

        public ControlledIdentifierImpl(KeyState state) {
            super(state);
        }

        @Override
        public BoundIdentifier<D> bind() {
            return new BoundControllableIdentifier<D>(getState());
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!super.equals(obj) || !(obj instanceof @SuppressWarnings("rawtypes") ControlledIdentifierImpl other) ||
                !getEnclosingInstance().equals(other.getEnclosingInstance())) {
                return false;
            }
            return Objects.equals(getState(), other.getState());
        }

        @Override
        public Optional<List<EventWithAttachments>> getKerl() {
            return kerl.kerl(getIdentifier());
        }

        @Override
        public Optional<Signer> getSigner() {
            return StereotomyImpl.this.getSigner(getState());
        }

        @Override
        public Optional<Verifier> getVerifier() {
            return Optional.of(new Verifier.DefaultVerifier(getState().getKeys()));
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result + getEnclosingInstance().hashCode();
            result = prime * result + Objects.hash(getState());
            return result;
        }

        @Override
        public <I extends Identifier> Optional<ControlledIdentifier<I>> newIdentifier(IdentifierSpecification.Builder<I> spec) {
            return StereotomyImpl.this.newIdentifier(this, spec);
        }

        @Override
        public Optional<CertificateWithPrivateKey> provision(Instant validFrom, Duration valid,
                                                             List<CertExtension> extensions, SignatureAlgorithm algo) {

            var coords = getState().getLastEstablishmentEvent();
            var lastEstablishing = kerl.getKeyEvent(coords);
            if (lastEstablishing.isEmpty()) {
                log.warn("Cannot get last establishing event for: {}", getIdentifier());
                return Optional.empty();
            }
            var signer = getSigner();
            if (signer.isEmpty()) {
                log.warn("Cannot get signer for: {}", getIdentifier());
                return Optional.empty();
            }

            KeyPair keyPair = algo.generateKeyPair(entropy);

            var signature = signer.get().sign(qb64(new BasicIdentifier(keyPair.getPublic())));

            var dn = new BcX500NameDnImpl(String.format("UID=%s, DC=%s", qb64(getState().getIdentifier()),
                                                        qb64(signature)));

            return Optional.of(new CertificateWithPrivateKey(Certificates.selfSign(false, dn, keyPair, validFrom,
                                                                                   validFrom.plus(valid), extensions),
                                                             keyPair.getPrivate()));
        }

        @Override
        public void rotate() {
            final var rotated = StereotomyImpl.this.rotate(getState());
            if (rotated.isEmpty()) {
                throw new IllegalStateException("could not rotate the state for identifier: " + getIdentifier());
            }
            setState(rotated.get());
        }

        @Override
        public void rotate(Builder spec) {
            final var rotated = StereotomyImpl.this.rotate(getState(), spec);
            if (rotated.isEmpty()) {
                throw new IllegalStateException("could not rotate the state for identifier: " + getIdentifier());
            }
            setState(rotated.get());
        }

        @Override
        public void seal(InteractionSpecification.Builder spec) {
            final var sealed = StereotomyImpl.this.seal(getState(), spec);
            if (sealed.isEmpty()) {
                throw new IllegalStateException("could not generate seal for identifier: " + getIdentifier());
            }
            setState(sealed.get());
        }

        private StereotomyImpl getEnclosingInstance() {
            return StereotomyImpl.this;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(StereotomyImpl.class);

    private final SecureRandom       entropy;
    private final EventFactory       eventFactory;
    private final KERL               kerl;
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
    public <D extends Identifier> Optional<BoundIdentifier<D>> bindingOf(EventCoordinates coordinates) {
        final var lookup = kerl.getKeyState(coordinates);
        if (lookup.isEmpty()) {
            log.warn("Identifier has no key state: {}", coordinates);
            return Optional.empty();
        }
        return Optional.of(new ControlledIdentifierImpl<D>(lookup.get()));
    }

    @Override
    public <D extends Identifier> Optional<ControlledIdentifier<D>> controlOf(D identifier) {
        final var lookup = kerl.getKeyState(identifier);
        if (lookup.isEmpty()) {
            log.warn("Identifier has no key state: {}", identifier);
            return Optional.empty();
        }
        return Optional.of(new ControlledIdentifierImpl<D>(lookup.get()));
    }

    @Override
    public Optional<KeyState> getKeyState(EventCoordinates eventCoordinates) {
        return kerl.getKeyState(eventCoordinates);
    }

    @Override
    public Optional<Verifier> getVerifier(KeyCoordinates coordinates) {
        var state = getKeyState(coordinates);
        if (state.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(new Verifier.DefaultVerifier(state.get().getKeys().get(coordinates.getKeyIndex())));
    }

    @Override
    public Optional<ControlledIdentifier<? extends Identifier>> newIdentifier() {
        return newIdentifier(IdentifierSpecification.newBuilder());
    }

    @Override
    public Optional<ControlledIdentifier<? extends Identifier>> newIdentifier(IdentifierSpecification.Builder<? super Identifier> spec) {
        var event = inception(Identifier.NONE, spec);
        KeyState state;
        try {
            state = kerl.append(event).get();
        } catch (InterruptedException | ExecutionException e) {
            log.warn("Unable to append inception event for identifier: {}", event.getIdentifier(), e);
            return Optional.empty();
        }

        if (state == null) {
            log.warn("Unable to append inception event for identifier: {}", event.getIdentifier());
            return Optional.empty();
        }
        ControlledIdentifier<?> cid = new ControlledIdentifierImpl<>(state);

        log.info("New {} identifier: {} coordinates: {}", spec.getWitnesses().isEmpty() ? "Private" : "Public",
                 cid.getIdentifier(), cid.getCoordinates());
        return Optional.of(cid);

    }

    private Optional<KeyPair> getKeyPair(KeyCoordinates keyCoords) {
        return keyStore.getKey(keyCoords);
    }

    private Optional<KeyPair> getKeyPair(KeyState state, int keyIndex,
                                         Optional<EstablishmentEvent> lastEstablishmentEvent) {
        if (lastEstablishmentEvent.isEmpty()) {
            return Optional.empty();
        }
        KeyCoordinates keyCoords = KeyCoordinates.of(lastEstablishmentEvent.get(), keyIndex);
        return getKeyPair(keyCoords);
    }

    private Optional<EstablishmentEvent> getLastEstablishingEvent(KeyState state) {
        return kerl.getKeyEvent(state.getLastEstablishmentEvent()).map(ke -> (EstablishmentEvent) ke);
    }

    private Optional<Signer> getSigner(KeyState state) {
        var identifier = state.getIdentifier();
        var signers = new PrivateKey[state.getKeys().size()];
        var lastEstablishingEvent = getLastEstablishingEvent(state);
        for (int i = 0; i < signers.length; i++) {
            Optional<KeyPair> keyPair = getKeyPair(state, i, lastEstablishingEvent);
            if (keyPair.isEmpty()) {
                log.warn("Last establishment event not found in KEL: {} : {} missing: {}", identifier,
                         state.getCoordinates(), state.getLastEstablishmentEvent());
                return Optional.empty();
            }
            signers[i] = keyPair.get().getPrivate();
        }
        return Optional.of(new Signer.SignerImpl(signers));
    }

    private <D extends Identifier> InceptionEvent inception(Identifier delegatingIdentifier,
                                                            IdentifierSpecification.Builder<D> spec) {
        IdentifierSpecification.Builder<D> specification = spec.clone();

        var initialKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        var nextKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);

        specification.addKey(initialKeyPair.getPublic())
                     .setSigningThreshold(unweighted(1))
                     .setNextKeys(List.of(nextKeyPair.getPublic()))
                     .setSigner(new Signer.SignerImpl(initialKeyPair.getPrivate()));

        InceptionEvent event = eventFactory.inception(delegatingIdentifier, specification.build());

        KeyCoordinates keyCoordinates = KeyCoordinates.of(event, 0);

        keyStore.storeKey(keyCoordinates, initialKeyPair);
        keyStore.storeNextKey(keyCoordinates, nextKeyPair);
        return event;
    }

    private KeyEvent interaction(KeyState state, InteractionSpecification.Builder spec) {
        InteractionSpecification.Builder specification = spec.clone();
        var identifier = state.getIdentifier();
        Optional<EstablishmentEvent> lastEstablishmentEvent = getLastEstablishingEvent(state);
        if (lastEstablishmentEvent.isEmpty()) {
            log.warn("missing establishment event: {} can't find: {}", kerl.getKeyEvent(state.getCoordinates()).get(),
                     state.getLastEstablishmentEvent());
            return null;
        }
        KeyCoordinates currentKeyCoordinates = KeyCoordinates.of(lastEstablishmentEvent.get(), 0);

        Optional<KeyPair> keyPair = keyStore.getKey(currentKeyCoordinates);

        if (keyPair.isEmpty()) {
            log.warn("Key pair for identifier not found in keystore: {}", identifier);
        }

        specification.setPriorEventDigest(state.getDigest())
                     .setLastEvent(state.getCoordinates())
                     .setIdentifier(identifier)
                     .setSigner(new SignerImpl(keyPair.get().getPrivate()));

        KeyEvent event = eventFactory.interaction(specification.build());
        return event;
    }

    private <I extends Identifier> Optional<ControlledIdentifier<I>> newIdentifier(ControlledIdentifier<? extends Identifier> delegator,
                                                                                   IdentifierSpecification.Builder<I> spec) {
        // The delegated inception
        var event = inception(delegator.getIdentifier(), spec);

        // Seal we need to verify the inception, based on the delegated inception
        // location
        var seals = InteractionSpecification.newBuilder()
                                            .addAllSeals(Arrays.asList(EventSeal.construct(event.getIdentifier(),
                                                                                           event.hash(kerl.getDigestAlgorithm()),
                                                                                           event.getSequenceNumber()
                                                                                                .longValue())));

        // Interaction event with the seal
        var interaction = interaction(delegator, seals);

        // Attachment of the interaction event, verifying the delegated inception
        var attachment = eventFactory.attachment(event,
                                                 new AttachmentImpl(EventSeal.construct(interaction.getIdentifier(),
                                                                                        interaction.hash(kerl.getDigestAlgorithm()),
                                                                                        interaction.getSequenceNumber()
                                                                                                   .longValue())));

        // Append the states - this is direct mode, all in our local KERL for now
        List<KeyState> states;
        try {
            states = kerl.append(Arrays.asList(event, interaction), Arrays.asList(attachment)).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Unable to create new identifier", e);
            return Optional.empty();
        }

        // The new key state for the delegated identifier
        KeyState delegatedState = states.get(0);

        if (delegatedState == null) {
            log.warn("Unable to append inception event for identifier: {}", event.getIdentifier());
            return Optional.empty();
        }

        // Update delegating state. Bit of a hack at the moment
        KeyState delegatingState = states.get(1);
        if (delegator instanceof ControlledIdentifierImpl<?> controller) {
            controller.setState(delegatingState);
        }

        // Finally, the new delegated identifier
        ControlledIdentifier<I> cid = new ControlledIdentifierImpl<I>(delegatedState);

        log.info("New {} delegator: {} identifier: {} coordinates: {}",
                 spec.getWitnesses().isEmpty() ? "Private" : "Public", cid.getDelegatingIdentifier(),
                 cid.getIdentifier(), cid.getCoordinates());
        return Optional.of(cid);
    }

    private Optional<KeyState> rotate(KeyState state) {
        return rotate(state, RotationSpecification.newBuilder());
    }

    private Optional<KeyState> rotate(KeyState state, RotationSpecification.Builder spec) {
        var delegatingIdentifier = state.getDelegatingIdentifier();
        return (delegatingIdentifier.isEmpty() ||
                delegatingIdentifier.get().equals(Identifier.NONE)) ? rotateUndelegated(state, spec)
                                                                    : rotateDelegated(state, spec);
    }

    private RotationEvent rotate(RotationSpecification.Builder spec, KeyState state, boolean delegated) {
        RotationSpecification.Builder specification = spec.clone();
        var identifier = state.getIdentifier();

        if (state.getNextKeyConfigurationDigest().isEmpty()) {
            log.warn("Identifier cannot be rotated: {}", identifier);
            return null;
        }

        var lastEstablishing = getLastEstablishingEvent(state);
        if (lastEstablishing.isEmpty()) {
            log.warn("Identifier cannot be rotated: {} estatblishment event missing", identifier);
            return null;
        }
        EstablishmentEvent establishing = lastEstablishing.get();
        var currentKeyCoordinates = KeyCoordinates.of(establishing, 0);

        KeyPair nextKeyPair = keyStore.getNextKey(currentKeyCoordinates)
                                      .orElseThrow(() -> new IllegalArgumentException("next key pair for identifier not found in keystore: "
                                      + currentKeyCoordinates));

        KeyPair newNextKeyPair = spec.getSignatureAlgorithm().generateKeyPair(entropy);

        specification.setSigningThreshold(unweighted(1))
                     .setIdentifier(identifier)
                     .setDigestAlgorithm(kerl.getDigestAlgorithm())
                     .setCurrentCoords(state.getCoordinates())
                     .setCurrentDigest(state.getDigest())
                     .setKey(nextKeyPair.getPublic())
                     .setNextKeys(List.of(newNextKeyPair.getPublic()))
                     .setSigner(new SignerImpl(nextKeyPair.getPrivate()));

        RotationEvent event = eventFactory.rotation(specification.build(), delegated);
        KeyCoordinates nextKeyCoordinates = KeyCoordinates.of(event, 0);

        keyStore.storeKey(nextKeyCoordinates, nextKeyPair);
        keyStore.storeNextKey(nextKeyCoordinates, newNextKeyPair);

        keyStore.removeKey(currentKeyCoordinates);
        keyStore.removeNextKey(currentKeyCoordinates);
        return event;
    }

    private Optional<KeyState> rotateDelegated(KeyState state, RotationSpecification.Builder spec) {
        RotationEvent event = rotate(spec, state, true);
        if (event == null) {
            return Optional.empty();
        }

        KeyState newState;
        try {
            newState = kerl.append(event).get();
        } catch (InterruptedException | ExecutionException e) {
            log.warn("Identifier cannot be rotated: {} cannot append event", state.getIdentifier(), e);
            return Optional.empty();
        }

        var delegator = newState.getDelegatingIdentifier();
        log.info("Rotated delegator: {} identifier: {} coordinates: {} old coordinates: {}", delegator.get(),
                 state.getIdentifier(), newState.getCoordinates(), state.getCoordinates());

        return Optional.of(newState);
    }

    private Optional<KeyState> rotateUndelegated(KeyState state, RotationSpecification.Builder spec) {
        RotationEvent event = rotate(spec, state, false);
        if (event == null) {
            return Optional.empty();
        }

        KeyState newState;
        try {
            newState = kerl.append(event).get();
        } catch (InterruptedException | ExecutionException e) {
            log.warn("Identifier cannot be rotated: {} cannot append event", state.getIdentifier(), e);
            return Optional.empty();
        }

        log.info("Rotated identifier: {} coordinates: {} old coordinates: {}", state.getIdentifier(),
                 newState.getCoordinates(), state.getCoordinates());

        return Optional.of(newState);
    }

    private Optional<KeyState> seal(KeyState state, InteractionSpecification.Builder spec) {
        KeyEvent event = interaction(state, spec);
        if (event == null) {
            return Optional.empty();
        }
        KeyState newKeyState;
        try {
            newKeyState = kerl.append(event).get();
        } catch (InterruptedException | ExecutionException e) {
            log.warn("Cannot append seal event for: {}", state.getIdentifier(), e);
            return Optional.empty();
        }
        if (newKeyState == null) {
            return Optional.empty();
        }
        return Optional.of(newKeyState);
    }
}
