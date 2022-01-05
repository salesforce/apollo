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

import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.salesforce.apollo.stereotomy.event.AttachmentEvent.Attachment.AttachmentImpl;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.EventFactory;
import com.salesforce.apollo.stereotomy.event.Format;
import com.salesforce.apollo.stereotomy.event.InceptionEvent;
import com.salesforce.apollo.stereotomy.event.InceptionEvent.ConfigurationTrait;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.RotationEvent;
import com.salesforce.apollo.stereotomy.event.Seal;
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
    private abstract class AbstractCtrlId implements KeyState {

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
        public long getSequenceNumber() {
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

    private class BoundControllableIdentifier extends AbstractCtrlId implements BoundIdentifier {
        volatile KeyState state;

        public BoundControllableIdentifier(KeyState state) {
            this.state = state;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!super.equals(obj)) {
                return false;
            }
            if (!(obj instanceof BoundControllableIdentifier other)) {
                return false;
            }
            if (!getEnclosingInstance().equals(other.getEnclosingInstance())) {
                return false;
            }
            return Objects.equals(state, other.state);
        }

        @Override
        public Optional<Verifier> getVerifier() {
            return Optional.of(new Verifier.DefaultVerifier(state.getKeys()));
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result + getEnclosingInstance().hashCode();
            result = prime * result + Objects.hash(state);
            return result;
        }

        @Override
        KeyState getState() {
            return state;
        }

        private StereotomyImpl getEnclosingInstance() {
            return StereotomyImpl.this;
        }

    }

    private class ControlledIdentifierImpl extends BoundControllableIdentifier implements ControlledIdentifier {

        public ControlledIdentifierImpl(KeyState state) {
            super(state);
        }

        @Override
        public BoundIdentifier bind() {
            return new BoundControllableIdentifier(getState());
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!super.equals(obj)) {
                return false;
            }
            if (!(obj instanceof ControlledIdentifierImpl other)) {
                return false;
            }
            if (!getEnclosingInstance().equals(other.getEnclosingInstance())) {
                return false;
            }
            return Objects.equals(state, other.state);
        }

        @Override
        public Optional<Signer> getSigner() {
            return StereotomyImpl.this.getSigner(state);
        }

        @Override
        public Optional<Verifier> getVerifier() {
            return Optional.of(new Verifier.DefaultVerifier(state.getKeys()));
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result + getEnclosingInstance().hashCode();
            result = prime * result + Objects.hash(state);
            return result;
        }

        @Override
        public Optional<ControlledIdentifier> newIdentifier(IdentifierSpecification.Builder spec) {
            return StereotomyImpl.this.newIdentifier(this, spec);
        }

        @Override
        public Optional<CertificateWithPrivateKey> provision(InetSocketAddress endpoint, Instant validFrom,
                                                             Duration valid, List<CertExtension> extensions,
                                                             SignatureAlgorithm algo) {

            var lastEstablishing = getState().getLastEstablishmentEvent();
            var keyCoordinates = new KeyCoordinates(lastEstablishing, 0);
            var signer = getSigner();
            if (signer.isEmpty()) {
                log.warn("Cannot get signer for: {}", getIdentifier());
                return Optional.empty();
            }

            KeyPair keyPair = algo.generateKeyPair(entropy);

            var signature = signer.get().sign(qb64(new BasicIdentifier(keyPair.getPublic())));

            var dn = new BcX500NameDnImpl(String.format("CN=%s, L=%s, UID=%s, DC=%s", endpoint.getHostName(),
                                                        endpoint.getPort(), qb64(keyCoordinates), qb64(signature)));

            return Optional.of(new CertificateWithPrivateKey(Certificates.selfSign(false, dn, entropy, keyPair,
                                                                                   validFrom, validFrom.plus(valid),
                                                                                   extensions),
                                                             keyPair.getPrivate()));
        }

        @Override
        public void rotate() {
            final var rotated = StereotomyImpl.this.rotate(state);
            if (rotated.isEmpty()) {
                throw new IllegalStateException("could not rotate the state for identifier: " + getIdentifier());
            }
            state = rotated.get();
        }

        @Override
        public void rotate(Builder spec) {
            final var rotated = StereotomyImpl.this.rotate(state, spec);
            if (rotated.isEmpty()) {
                throw new IllegalStateException("could not rotate the state for identifier: " + getIdentifier());
            }
            state = rotated.get();
        }

        @Override
        public void seal(InteractionSpecification.Builder spec) {
            final var sealed = StereotomyImpl.this.seal(state, spec);
            if (sealed.isEmpty()) {
                throw new IllegalStateException("could not generate seal for identifier: " + getIdentifier());
            }
            state = sealed.get();
        }

        @Override
        KeyState getState() {
            final var current = state;
            return current;
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
    public Optional<BoundIdentifier> bindingOf(EventCoordinates coordinates) {
        final var lookup = kerl.getKeyState(coordinates);
        if (lookup.isEmpty()) {
            log.warn("Identifier has no key state: {}", coordinates);
            return Optional.empty();
        }
        return Optional.of(new ControlledIdentifierImpl(lookup.get()));
    }

    @Override
    public Optional<ControlledIdentifier> controlOf(Identifier identifier) {
        final var lookup = kerl.getKeyState(identifier);
        if (lookup.isEmpty()) {
            log.warn("Identifier has no key state: {}", identifier);
            return Optional.empty();
        }
        return Optional.of(new ControlledIdentifierImpl(lookup.get()));
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
    public Optional<ControlledIdentifier> newIdentifier() {
        return newIdentifier(IdentifierSpecification.newBuilder());
    }

    @Override
    public Optional<ControlledIdentifier> newIdentifier(IdentifierSpecification.Builder spec) {
        var event = inception(Identifier.NONE, spec);
        KeyState state;
        try {
            state = kerl.append(event).get();
        } catch (InterruptedException | ExecutionException e) {
            return Optional.empty();
        }

        if (state == null) {
            log.warn("Unable to append inception event for identifier: {}", event.getIdentifier());
            return Optional.empty();
        }
        ControlledIdentifier cid = new ControlledIdentifierImpl(state);

        log.info("New {} identifier: {} coordinates: {} cur key: {} next key: {}",
                 spec.getWitnesses().isEmpty() ? "Private" : "Public", cid.getIdentifier(), cid.getCoordinates());
        return Optional.of(cid);

    }

    private Optional<KeyPair> getKeyPair(KeyCoordinates keyCoords) {
        return keyStore.getKey(keyCoords);
    }

    private Optional<KeyPair> getKeyPair(KeyState state, int keyIndex, Optional<KeyEvent> lastEstablishmentEvent) {
        if (lastEstablishmentEvent.isEmpty()) {
            return Optional.empty();
        }
        KeyCoordinates keyCoords = KeyCoordinates.of((EstablishmentEvent) lastEstablishmentEvent.get(), keyIndex);
        return getKeyPair(keyCoords);
    }

    private Optional<Signer> getSigner(KeyState state) {
        var identifier = state.getIdentifier();
        PrivateKey[] signers = new PrivateKey[state.getKeys().size()];
        for (int i = 0; i < signers.length; i++) {
            Optional<KeyPair> keyPair = getKeyPair(state, i, kerl.getKeyEvent(state.getLastEstablishmentEvent()));
            if (keyPair.isEmpty()) {
                log.warn("Last establishment event not found in KEL: {} : {} missing: {}", identifier,
                         state.getCoordinates(), state.getLastEstablishmentEvent());
                return Optional.empty();
            }
            signers[i] = keyPair.get().getPrivate();
        }
        return Optional.of(new Signer.SignerImpl(signers));
    }

    private InceptionEvent inception(Identifier delegatingIdentifier, IdentifierSpecification.Builder spec) {
        IdentifierSpecification.Builder specification = spec.clone();

        var initialKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        var nextKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);

        specification.addKey(initialKeyPair.getPublic())
                     .setSigningThreshold(unweighted(1))
                     .setNextKeys(List.of(nextKeyPair.getPublic()))
                     .setSigner(new Signer.SignerImpl(initialKeyPair.getPrivate()));

        InceptionEvent event = this.eventFactory.inception(delegatingIdentifier, specification.build());

        KeyCoordinates keyCoordinates = KeyCoordinates.of(event, 0);

        keyStore.storeKey(keyCoordinates, initialKeyPair);
        keyStore.storeNextKey(keyCoordinates, nextKeyPair);
        return event;
    }

    private Optional<ControlledIdentifier> newIdentifier(ControlledIdentifier delegator,
                                                         IdentifierSpecification.Builder spec) {
        var delegatingIdentifier = delegator.getIdentifier();

        InceptionEvent event = inception(delegatingIdentifier, spec);

        KeyState state;
        try {
            state = kerl.append(event).get();
        } catch (InterruptedException | ExecutionException e) {
            return Optional.empty();
        }

        if (state == null) {
            log.warn("Unable to append inception event for identifier: {}", event.getIdentifier());
            return Optional.empty();
        }

        ControlledIdentifier cid = new ControlledIdentifierImpl(state);

        delegator.seal(InteractionSpecification.newBuilder()
                                               .addAllSeals(Collections.singletonList(Seal.EventSeal.construct(cid.getIdentifier(),
                                                                                                               cid.getDigest(),
                                                                                                               cid.getSequenceNumber()))));
        var attachment = new AttachmentImpl(Seal.EventSeal.construct(delegator.getIdentifier(), delegator.getDigest(),
                                                                     delegator.getSequenceNumber()));
        var attachmentEvent = eventFactory.attachment(event, attachment);
        try {
            kerl.append(attachmentEvent).get();
        } catch (InterruptedException | ExecutionException e) {
            return Optional.empty();
        }

        log.info("New {} delegator: {} identifier: {} coordinates: {} cur key: {} next key: {}",
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

        var lastEstablishing = kerl.getKeyEvent(state.getLastEstablishmentEvent());
        if (lastEstablishing.isEmpty()) {
            log.warn("Identifier cannot be rotated: {} estatblishment event missing", identifier);
            return null;
        }
        EstablishmentEvent establishing = (EstablishmentEvent) lastEstablishing.get();
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
        InteractionSpecification.Builder specification = spec.clone();
        var identifier = state.getIdentifier();
        Optional<KeyEvent> lastEstablishmentEvent = kerl.getKeyEvent(state.getLastEstablishmentEvent());
        if (lastEstablishmentEvent.isEmpty()) {
            log.warn("missing establishment event: {} can't find: {}", kerl.getKeyEvent(state.getCoordinates()).get(),
                     state.getLastEstablishmentEvent());
            return Optional.empty();
        }
        KeyCoordinates currentKeyCoordinates = KeyCoordinates.of((EstablishmentEvent) lastEstablishmentEvent.get(), 0);

        Optional<KeyPair> keyPair = keyStore.getKey(currentKeyCoordinates);

        if (keyPair.isEmpty()) {
            log.warn("Key pair for identifier not found in keystore: {}", identifier);
        }

        specification.setPriorEventDigest(state.getDigest())
                     .setLastEvent(state.getCoordinates())
                     .setIdentifier(identifier)
                     .setSigner(new SignerImpl(keyPair.get().getPrivate()));

        KeyEvent event = eventFactory.interaction(specification.build());
        KeyState newKeyState;
        try {
            newKeyState = kerl.append(event).get();
        } catch (InterruptedException | ExecutionException e) {
            log.warn("Cannot append seal event for: {}", identifier, e);
            return Optional.empty();
        }
        if (newKeyState == null) {
            return Optional.empty();
        }
        return Optional.of(newKeyState);
    }
}
