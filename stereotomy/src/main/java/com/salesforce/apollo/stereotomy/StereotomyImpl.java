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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.joou.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.stereotomy.event.proto.KeyState_;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
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
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent.AttachmentImpl;
import com.salesforce.apollo.stereotomy.event.DelegatedInceptionEvent;
import com.salesforce.apollo.stereotomy.event.DelegatedRotationEvent;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.EventFactory;
import com.salesforce.apollo.stereotomy.event.InceptionEvent;
import com.salesforce.apollo.stereotomy.event.InceptionEvent.ConfigurationTrait;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.RotationEvent;
import com.salesforce.apollo.stereotomy.event.Seal.EventSeal;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.BasicIdentifier;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
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
        public CompletableFuture<EstablishmentEvent> getLastEstablishingEvent() {
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
        public CompletableFuture<Void> commit(DelegatedRotationEvent delegation, AttachmentEvent commitment) {
            return kerl.append(Collections.singletonList(delegation), Collections.singletonList(commitment))
                       .thenApply(ks -> {
                           setState(ks.get(0));
                           return null;
                       });
        }

        @Override
        public CompletableFuture<DelegatedRotationEvent> delegateRotate(Builder spec) {
            return StereotomyImpl.this.rotate(spec, getState(), true).thenApply(rot -> (DelegatedRotationEvent) rot);
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
        public CompletableFuture<List<EventWithAttachments>> getKerl() {
            return kerl.kerl(getIdentifier());
        }

        @Override
        public CompletableFuture<Signer> getSigner() {
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
        public <I extends Identifier> CompletableFuture<ControlledIdentifier<I>> newIdentifier(IdentifierSpecification.Builder<I> spec) {
            return StereotomyImpl.this.newIdentifier(this, spec);
        }

        @Override
        public CompletableFuture<CertificateWithPrivateKey> provision(Instant validFrom, Duration valid,
                                                                      List<CertExtension> extensions,
                                                                      SignatureAlgorithm algo) {

            return getSigner().thenApply(signer -> {
                KeyPair keyPair = algo.generateKeyPair(entropy);

                var signature = signer.sign(qb64(new BasicIdentifier(keyPair.getPublic())));

                var dn = new BcX500NameDnImpl(String.format("UID=%s, DC=%s", qb64(getState().getIdentifier()),
                                                            qb64(signature)));

                return new CertificateWithPrivateKey(Certificates.selfSign(false, dn, keyPair, validFrom,
                                                                           validFrom.plus(valid), extensions),
                                                     keyPair.getPrivate());
            });
        }

        @Override
        public CompletableFuture<Void> rotate() {
            return StereotomyImpl.this.rotate(getState()).thenApply(state -> {
                setState(state);
                return null;
            });
        }

        @Override
        public CompletableFuture<Void> rotate(Builder spec) {
            return StereotomyImpl.this.rotate(getState(), spec).thenApply(state -> {
                setState(state);
                return null;
            });
        }

        @Override
        public CompletableFuture<EventCoordinates> seal(InteractionSpecification.Builder spec) {
            final var state = getState();
            return StereotomyImpl.this.seal(state, spec).thenApply(ks -> {
                setState(ks);
                log.info("Seal interaction identifier: {} coordinates: {} old coordinates: {}", ks.getIdentifier(),
                         state.getCoordinates(), ks.getCoordinates());
                return ks.getCoordinates();
            });
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
    public <D extends Identifier> CompletableFuture<BoundIdentifier<D>> bindingOf(EventCoordinates coordinates) {
        return kerl.getKeyState(coordinates).thenApply(lookup -> new ControlledIdentifierImpl<D>(lookup));
    }

    @Override
    public CompletableFuture<ControlledIdentifier<SelfAddressingIdentifier>> commit(DelegatedInceptionEvent delegation,
                                                                                    AttachmentEvent commitment) {
        return kerl.append(Arrays.asList(delegation), Arrays.asList(commitment)).thenApply(ks -> {
            var cid = new ControlledIdentifierImpl<SelfAddressingIdentifier>(ks.get(0));
            log.info("New delegated identifier: {} coordinates: {}", cid.getIdentifier(), cid.getCoordinates());
            return cid;
        });
    }

    @Override
    public <D extends Identifier> CompletableFuture<ControlledIdentifier<D>> controlOf(D identifier) {
        return kerl.getKeyState(identifier).thenApply(lookup -> new ControlledIdentifierImpl<D>(lookup));
    }

    @Override
    public DigestAlgorithm digestAlgorithm() {
        return kerl.getDigestAlgorithm();
    }

    @Override
    public CompletableFuture<KeyState> getKeyState(EventCoordinates eventCoordinates) {
        return kerl.getKeyState(eventCoordinates);
    }

    @Override
    public CompletableFuture<Verifier> getVerifier(KeyCoordinates coordinates) {
        return getKeyState(coordinates).thenApply(state -> new Verifier.DefaultVerifier(state.getKeys()
                                                                                             .get(coordinates.getKeyIndex())));
    }

    @Override
    public DelegatedInceptionEvent newDelegatedIdentifier(Identifier controller,
                                                          IdentifierSpecification.Builder<SelfAddressingIdentifier> specification) {
        return (DelegatedInceptionEvent) inception(controller, specification);
    }

    @Override
    public CompletableFuture<ControlledIdentifier<SelfAddressingIdentifier>> newIdentifier() {
        return newIdentifier(IdentifierSpecification.newBuilder());
    }

    @Override
    public <T extends Identifier> CompletableFuture<ControlledIdentifier<T>> newIdentifier(Identifier controller,
                                                                                           IdentifierSpecification.Builder<T> spec) {
        var event = inception(controller, spec);
        return kerl.append(event).thenApply(ks -> {
            var cid = new ControlledIdentifierImpl<T>(ks);
            log.info("New {} identifier: {} coordinates: {}", spec.getWitnesses().isEmpty() ? "Private" : "Public",
                     cid.getIdentifier(), cid.getCoordinates());
            return cid;
        });
    }

    @Override
    public <T extends Identifier> CompletableFuture<ControlledIdentifier<T>> newIdentifier(IdentifierSpecification.Builder<T> spec) {
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

    private CompletableFuture<EstablishmentEvent> getLastEstablishingEvent(KeyState state) {
        return kerl.getKeyEvent(state.getLastEstablishmentEvent()).thenApply(ke -> (EstablishmentEvent) ke);
    }

    private CompletableFuture<Signer> getSigner(KeyState state) {
        var identifier = state.getIdentifier();
        var signers = new PrivateKey[state.getKeys().size()];
        return getLastEstablishingEvent(state).thenApply(e -> {
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
        });
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

    private CompletableFuture<KeyEvent> interaction(KeyState state, InteractionSpecification.Builder spec) {
        InteractionSpecification.Builder specification = spec.clone();
        var identifier = state.getIdentifier();

        return getLastEstablishingEvent(state).thenApply(le -> {
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
        });
    }

    @SuppressWarnings("unchecked")
    private <I extends Identifier> CompletableFuture<ControlledIdentifier<I>> newIdentifier(ControlledIdentifier<? extends Identifier> delegator,
                                                                                            IdentifierSpecification.Builder<I> spec) {
        log.warn("New identifier, controller: {}", delegator);
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
        return kerl.append(event).thenCompose(ks -> interaction(delegator, seals)).thenCompose(interaction -> {
            // Attachment of the interaction event, verifying the delegated inception
            var attachment = eventFactory.attachment(event,
                                                     new AttachmentImpl(EventSeal.construct(interaction.getIdentifier(),
                                                                                            interaction.hash(kerl.getDigestAlgorithm()),
                                                                                            interaction.getSequenceNumber()
                                                                                                       .longValue())));
            return kerl.append(Arrays.asList(event, interaction), Arrays.asList(attachment));
        }).thenApply(states -> {
            // The new key state for the delegated identifier
            KeyState delegatedState = states.get(0);

            if (delegatedState == null) {
                log.warn("Unable to append inception event for identifier: {}", event.getIdentifier());
                return Optional.empty();
            }

            // Finally, the new delegated identifier
            ControlledIdentifier<I> cid = new ControlledIdentifierImpl<I>(delegatedState);

            log.info("New {} delegator: {} identifier: {} coordinates: {}",
                     spec.getWitnesses().isEmpty() ? "Private" : "Public", cid.getDelegatingIdentifier().get(),
                     cid.getIdentifier(), cid.getCoordinates());
            return cid;
        }).thenApply(cid -> (ControlledIdentifier<I>) cid);
    }

    private CompletableFuture<KeyState> rotate(KeyState state) {
        return rotate(state, RotationSpecification.newBuilder());
    }

    private CompletableFuture<KeyState> rotate(KeyState state, RotationSpecification.Builder spec) {
        var delegatingIdentifier = state.getDelegatingIdentifier();
        return (delegatingIdentifier.isEmpty() ||
                delegatingIdentifier.get().equals(Identifier.NONE)) ? rotateUndelegated(state, spec)
                                                                    : rotateDelegated(state, spec);
    }

    private CompletableFuture<RotationEvent> rotate(RotationSpecification.Builder spec, KeyState state,
                                                    boolean delegated) {
        RotationSpecification.Builder specification = spec.clone();
        var identifier = state.getIdentifier();

        if (state.getNextKeyConfigurationDigest().isEmpty()) {
            log.warn("Identifier cannot be rotated: {}", identifier);
            return null;
        }

        return getLastEstablishingEvent(state).thenApply(establishing -> {
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
        });
    }

    private CompletableFuture<KeyState> rotateDelegated(KeyState state, RotationSpecification.Builder spec) {
        return rotate(spec, state, true).thenCompose(re -> kerl.append(re)).thenApply(ks -> {
            var delegator = ks.getDelegatingIdentifier();
            log.info("Rotated delegated: {} identifier: {} coordinates: {} old coordinates: {}", delegator.get(),
                     state.getIdentifier(), ks.getCoordinates(), state.getCoordinates());
            return ks;
        });
    }

    private CompletableFuture<KeyState> rotateUndelegated(KeyState state, RotationSpecification.Builder spec) {
        return rotate(spec, state, false).thenCompose(event -> kerl.append(event)).thenApply(ks -> {

            log.info("Rotated identifier: {} coordinates: {} old coordinates: {}", state.getIdentifier(),
                     ks.getCoordinates(), state.getCoordinates());
            return ks;
        });
    }

    private CompletableFuture<KeyState> seal(KeyState state, InteractionSpecification.Builder spec) {
        return interaction(state, spec).thenCompose(event -> kerl.append(event));
    }
}
