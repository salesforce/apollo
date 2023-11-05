/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import com.salesfoce.apollo.stereotomy.event.proto.KeyState_;
import com.salesforce.apollo.crypto.*;
import com.salesforce.apollo.crypto.Signer.SignerImpl;
import com.salesforce.apollo.crypto.cert.BcX500NameDnImpl;
import com.salesforce.apollo.crypto.cert.CertExtension;
import com.salesforce.apollo.crypto.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.crypto.cert.Certificates;
import com.salesforce.apollo.stereotomy.KERL.EventWithAttachments;
import com.salesforce.apollo.stereotomy.event.*;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent.AttachmentImpl;
import com.salesforce.apollo.stereotomy.event.InceptionEvent.ConfigurationTrait;
import com.salesforce.apollo.stereotomy.event.Seal.EventSeal;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.BasicIdentifier;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.InteractionSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification.Builder;
import org.joou.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static com.salesforce.apollo.crypto.SigningThreshold.unweighted;
import static com.salesforce.apollo.stereotomy.identifier.QualifiedBase64Identifier.qb64;

/**
 * Direct mode implementation of a Stereotomy controller. This controller keeps
 * it's own KEL/KERL and does not cooperate with other controllers
 *
 * @author hal.hildebrand
 */
public class StereotomyImpl implements Stereotomy {
    private static final Logger log = LoggerFactory.getLogger(StereotomyImpl.class);
    private final SecureRandom entropy;
    private final EventFactory eventFactory;
    private final KERL kerl;
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
    public <D extends Identifier> BoundIdentifier<D> bindingOf(EventCoordinates coordinates) {
        KeyState lookup = kerl.getKeyState(coordinates);
        return new ControlledIdentifierImpl<D>(lookup);
    }

    @Override
    public ControlledIdentifier<SelfAddressingIdentifier> commit(DelegatedInceptionEvent delegation,
                                                                 AttachmentEvent commitment) {
        List<KeyState> ks = kerl.append(Arrays.asList(delegation), Arrays.asList(commitment));
        var cid = new ControlledIdentifierImpl<SelfAddressingIdentifier>(ks.get(0));
        log.info("New delegated identifier: {} coordinates: {}", cid.getIdentifier(), cid.getCoordinates());
        return cid;
    }

    @Override
    public <D extends Identifier> ControlledIdentifier<D> controlOf(D identifier) {
        KeyState lookup = kerl.getKeyState(identifier);
        return new ControlledIdentifierImpl<D>(lookup);
    }

    @Override
    public DigestAlgorithm digestAlgorithm() {
        return kerl.getDigestAlgorithm();
    }

    @Override
    public KeyState getKeyState(EventCoordinates eventCoordinates) {
        return kerl.getKeyState(eventCoordinates);
    }

    @Override
    public Verifier getVerifier(KeyCoordinates coordinates) {
        KeyState state = getKeyState(coordinates);
        return new Verifier.DefaultVerifier(state.getKeys()
                .get(coordinates.getKeyIndex()));
    }

    @Override
    public DelegatedInceptionEvent newDelegatedIdentifier(Identifier controller,
                                                          IdentifierSpecification.Builder<SelfAddressingIdentifier> specification) {
        return (DelegatedInceptionEvent) inception(controller, specification);
    }

    @Override
    public ControlledIdentifier<SelfAddressingIdentifier> newIdentifier() {
        return newIdentifier(IdentifierSpecification.newBuilder());
    }

    @Override
    public <T extends Identifier> ControlledIdentifier<T> newIdentifier(Identifier controller,
                                                                        IdentifierSpecification.Builder<T> spec) {
        var event = inception(controller, spec);
        KeyState ks = kerl.append(event);
        var cid = new ControlledIdentifierImpl<T>(ks);
        log.info("New {} identifier: {} coordinates: {}", spec.getWitnesses().isEmpty() ? "Private" : "Public",
                cid.getIdentifier(), cid.getCoordinates());
        return cid;
    }

    @Override
    public <T extends Identifier> ControlledIdentifier<T> newIdentifier(IdentifierSpecification.Builder<T> spec) {
        return newIdentifier(Identifier.NONE, spec);
    }

    private Optional<KeyPair> getKeyPair(KeyCoordinates keyCoords) {
        return keyStore.getKey(keyCoords);
    }

    private Optional<KeyPair> getKeyPair(KeyState state, int keyIndex, EstablishmentEvent lastEstablishmentEvent) {
        if (lastEstablishmentEvent == null) {
            return Optional.empty();
        }
        KeyCoordinates keyCoords = KeyCoordinates.of(lastEstablishmentEvent, keyIndex);
        return getKeyPair(keyCoords);
    }

    private EstablishmentEvent getLastEstablishingEvent(KeyState state) {
        KeyEvent ke = kerl.getKeyEvent(state.getLastEstablishmentEvent());
        return (EstablishmentEvent) ke;
    }

    private Signer getSigner(KeyState state) {
        var identifier = state.getIdentifier();
        var signers = new PrivateKey[state.getKeys().size()];
        EstablishmentEvent e = getLastEstablishingEvent(state);
        for (int i = 0; i < signers.length; i++) {
            Optional<KeyPair> keyPair = getKeyPair(state, i, e);
            if (keyPair.isEmpty()) {
                log.warn("Last establishment event not found in KEL: {} : {} missing: {}", identifier,
                        state.getCoordinates(), state.getLastEstablishmentEvent());
                return null;
            }
            signers[i] = keyPair.get().getPrivate();
        }
        return new Signer.SignerImpl(signers);
    }

    private <D extends Identifier> InceptionEvent inception(Identifier delegatingIdentifier,
                                                            IdentifierSpecification.Builder<D> spec) {
        IdentifierSpecification.Builder<D> specification = spec.clone();

        var initialKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        KeyPair nextKeyPair = null;

        nextKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);

        specification.addKey(initialKeyPair.getPublic())
                .setSigningThreshold(unweighted(1))
                .setSigner(new Signer.SignerImpl(initialKeyPair.getPrivate()));

        if (nextKeyPair != null) {
            specification.setNextKeys(List.of(nextKeyPair.getPublic()));
        }

        InceptionEvent event = eventFactory.inception(delegatingIdentifier, specification.build());

        KeyCoordinates keyCoordinates = KeyCoordinates.of(event, 0);

        keyStore.storeKey(keyCoordinates, initialKeyPair);
        if (nextKeyPair != null) {
            keyStore.storeNextKey(keyCoordinates, nextKeyPair);
        }
        return event;
    }

    private KeyEvent interaction(KeyState state, InteractionSpecification.Builder spec) {
        InteractionSpecification.Builder specification = spec.clone();
        var identifier = state.getIdentifier();

        EstablishmentEvent le = getLastEstablishingEvent(state);
        KeyCoordinates currentKeyCoordinates = KeyCoordinates.of(le, 0);

        Optional<KeyPair> keyPair = keyStore.getKey(currentKeyCoordinates);

        if (keyPair.isEmpty()) {
            log.warn("Key pair for identifier not found in keystore: {}", identifier);
        }

        specification.setPriorEventDigest(state.getDigest())
                .setLastEvent(state.getCoordinates())
                .setIdentifier(identifier)
                .setSigner(new SignerImpl(keyPair.get().getPrivate()));

        return eventFactory.interaction(specification.build());
    }

    @SuppressWarnings("unchecked")
    private <I extends Identifier> ControlledIdentifier<I> newIdentifier(ControlledIdentifier<? extends Identifier> delegator,
                                                                         IdentifierSpecification.Builder<I> spec) {
        log.warn("New identifier, controller: {}", delegator.getIdentifier());
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
        KeyState ks = kerl.append(event);
        var interaction = interaction(delegator, seals);
        // Attachment of the interaction event, verifying the delegated inception
        var attachment = eventFactory.attachment(event,
                new AttachmentImpl(EventSeal.construct(interaction.getIdentifier(),
                        interaction.hash(kerl.getDigestAlgorithm()),
                        interaction.getSequenceNumber()
                                .longValue())));
        var s = kerl.append(Collections.singletonList(interaction), Collections.singletonList(attachment));
        var delegatedState = kerl.append(event);
        if (delegatedState == null) {
            log.warn("Unable to append inception event for identifier: {}", event.getIdentifier());
            return null;
        }

        // Finally, the new delegated identifier
        ControlledIdentifier<I> cid = new ControlledIdentifierImpl<I>(delegatedState);

        log.info("New {} delegator: {} identifier: {} coordinates: {}",
                spec.getWitnesses().isEmpty() ? "Private" : "Public", cid.getDelegatingIdentifier().get(),
                cid.getIdentifier(), cid.getCoordinates());
        return cid;
    }

    private KeyState rotate(KeyState state) {
        return rotate(state, RotationSpecification.newBuilder());
    }

    private KeyState rotate(KeyState state, RotationSpecification.Builder spec) {
        var delegatingIdentifier = state.getDelegatingIdentifier();
        return (delegatingIdentifier.isEmpty() ||
                delegatingIdentifier.get().equals(Identifier.NONE)) ? rotateUndelegated(state, spec)
                : rotateDelegated(state, spec);
    }

    private RotationEvent rotate(RotationSpecification.Builder spec, KeyState state,
                                 boolean delegated) {
        RotationSpecification.Builder specification = spec.clone();
        var identifier = state.getIdentifier();

        if (state.getNextKeyConfigurationDigest().isEmpty()) {
            log.warn("Identifier cannot be rotated: {}", identifier);
            return null;
        }

        EstablishmentEvent establishing = getLastEstablishingEvent(state);
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

    private KeyState rotateDelegated(KeyState state, RotationSpecification.Builder spec) {
        RotationEvent re = rotate(spec, state, true);
        var ks = kerl.append(re);
        var delegator = ks.getDelegatingIdentifier();
        log.info("Rotated delegated: {} identifier: {} coordinates: {} old coordinates: {}", delegator.get(),
                state.getIdentifier(), ks.getCoordinates(), state.getCoordinates());
        return ks;
    }

    private KeyState rotateUndelegated(KeyState state, RotationSpecification.Builder spec) {
        RotationEvent event = rotate(spec, state, false);
        KeyState ks = kerl.append(event);

        log.info("Rotated identifier: {} coordinates: {} old coordinates: {}", state.getIdentifier(),
                ks.getCoordinates(), state.getCoordinates());
        return ks;
    }

    private KeyState seal(KeyState state, InteractionSpecification.Builder spec) {
        KeyEvent event = interaction(state, spec);
        return kerl.append(event);
    }

    private abstract static class AbstractCtrlId implements KeyState {

        @Override
        public Set<ConfigurationTrait> configurationTraits() {
            return getState().configurationTraits();
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
        public EstablishmentEvent getLastEstablishingEvent() {
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
        public Void commit(DelegatedRotationEvent delegation, AttachmentEvent commitment) {
            List<KeyState> ks = kerl.append(Collections.singletonList(delegation), Collections.singletonList(commitment));
            setState(ks.getFirst());
            return null;
        }

        @Override
        public DelegatedRotationEvent delegateRotate(Builder spec) {
            RotationEvent rot = StereotomyImpl.this.rotate(spec, getState(), true);
            return (DelegatedRotationEvent) rot;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!super.equals(obj) || !(obj instanceof @SuppressWarnings("rawtypes")ControlledIdentifierImpl other) ||
                    !getEnclosingInstance().equals(other.getEnclosingInstance())) {
                return false;
            }
            return Objects.equals(getState(), other.getState());
        }

        @Override
        public List<EventWithAttachments> getKerl() {
            return kerl.kerl(getIdentifier());
        }

        @Override
        public Signer getSigner() {
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
        public Optional<KeyPair> newEphemeral() {
            IdentifierSpecification.Builder<BasicIdentifier> builder = IdentifierSpecification.newBuilder().setBasic();
            return Optional.ofNullable(builder.getSignatureAlgorithm().generateKeyPair(entropy));
        }

        @Override
        public <I extends Identifier> ControlledIdentifier<I> newIdentifier(IdentifierSpecification.Builder<I> spec) {
            return StereotomyImpl.this.newIdentifier(this, spec);
        }

        @Override
        public CertificateWithPrivateKey provision(Instant validFrom, Duration valid,
                                                   List<CertExtension> extensions,
                                                   SignatureAlgorithm algo) {
            Signer signer = getSigner();
            KeyPair keyPair = algo.generateKeyPair(entropy);

            var signature = signer.sign(qb64(new BasicIdentifier(keyPair.getPublic())));

            var dn = new BcX500NameDnImpl(String.format("UID=%s, DC=%s",
                    Base64.getUrlEncoder()
                            .encodeToString((getState().getCoordinates()
                                    .toEventCoords()
                                    .toByteArray())),
                    qb64(signature)));

            return new CertificateWithPrivateKey(Certificates.selfSign(false, dn, keyPair, validFrom,
                    validFrom.plus(valid), extensions),
                    keyPair.getPrivate());
        }

        @Override
        public Void rotate() {
            KeyState state = StereotomyImpl.this.rotate(getState());
            setState(state);
            return null;
        }

        @Override
        public Void rotate(Builder spec) {
            KeyState state = StereotomyImpl.this.rotate(getState(), spec);
            setState(state);
            return null;
        }

        @Override
        public EventCoordinates seal(InteractionSpecification.Builder spec) {
            final var state = getState();
            KeyState ks = StereotomyImpl.this.seal(state, spec);
            setState(ks);
            log.info("Seal interaction identifier: {} coordinates: {} old coordinates: {}", ks.getIdentifier(),
                    state.getCoordinates(), ks.getCoordinates());
            return ks.getCoordinates();
        }

        private StereotomyImpl getEnclosingInstance() {
            return StereotomyImpl.this;
        }
    }
}
