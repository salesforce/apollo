/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;
import static com.salesforce.apollo.crypto.QualifiedBase64.shortQb64;
import static com.salesforce.apollo.stereotomy.event.SigningThreshold.unweighted;
import static com.salesforce.apollo.stereotomy.identifier.QualifiedBase64Identifier.qb64;

import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.crypto.cert.BcX500NameDnImpl;
import com.salesforce.apollo.crypto.cert.CertExtension;
import com.salesforce.apollo.crypto.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.crypto.cert.Certificates;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
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

/**
 * Default implementation of a Stereotomy controller
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
        private final KeyState state;

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
        public Optional<Verifier> getVerifier(int keyIndex) {
            return StereotomyImpl.this.getVerifier(getIdentifier(), keyIndex);
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

    private class ControllableIdentifierImpl extends AbstractCtrlId implements ControllableIdentifier {

        private volatile KeyState state;

        public ControllableIdentifierImpl(KeyState state) {
            this.state = state;
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
            if (!(obj instanceof ControllableIdentifierImpl other)) {
                return false;
            }
            if (!getEnclosingInstance().equals(other.getEnclosingInstance())) {
                return false;
            }
            return Objects.equals(state, other.state);
        }

        @Override
        public Optional<Signer> getSigner(int keyIndex) {
            return StereotomyImpl.this.getSigner(getIdentifier(), keyIndex);
        }

        @Override
        public Optional<Verifier> getVerifier(int keyIndex) {
            return StereotomyImpl.this.getVerifier(getIdentifier(), keyIndex);
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
        public Optional<CertificateWithPrivateKey> provision(InetSocketAddress endpoint, Instant validFrom,
                                                             Duration valid, List<CertExtension> extensions,
                                                             SignatureAlgorithm algo) {

            var lastEstablishing = getState().getLastEstablishmentEvent();
            var keyCoordinates = new KeyCoordinates(lastEstablishing, 0);
            var signer = getSigner(0);
            if (signer.isEmpty()) {
                log.warn("Cannot get signer for key 0 for: {}", getIdentifier());
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
            final var rotated = StereotomyImpl.this.rotate(getIdentifier());
            if (rotated.isEmpty()) {
                throw new IllegalStateException("could not rotate the state for identifier: " + getIdentifier());
            }
            state = rotated.get();
        }

        @Override
        public void rotate(Builder spec) {
            final var rotated = StereotomyImpl.this.rotate(getIdentifier(), spec);
            if (rotated.isEmpty()) {
                throw new IllegalStateException("could not rotate the state for identifier: " + getIdentifier());
            }
            state = rotated.get();
        }

        @Override
        public void rotate(List<Seal> seals) {
            final var rotated = StereotomyImpl.this.rotate(getIdentifier(), seals);
            if (rotated.isEmpty()) {
                throw new IllegalStateException("could not rotate the state for identifier: " + getIdentifier());
            }
            state = rotated.get();
        }

        @Override
        public void seal(InteractionSpecification.Builder spec) {
            final var sealed = StereotomyImpl.this.seal(getIdentifier(), spec);
            if (sealed.isEmpty()) {
                throw new IllegalStateException("could not generate seal for identifier: " + getIdentifier());
            }
            state = sealed.get();
        }

        @Override
        public void seal(List<Seal> seals) {
            final var sealed = StereotomyImpl.this.seal(getIdentifier(), seals);
            if (sealed.isEmpty()) {
                throw new IllegalStateException("could not generate seal for identifier: " + getIdentifier());
            }
            state = sealed.get();
        }

        @Override
        public Optional<EventSignature> sign(KeyEvent event) {
            return StereotomyImpl.this.sign(getIdentifier(), event);
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
    public Optional<BoundIdentifier> bindingOf(EventCoordinates coordinates) {
        final var lookup = kerl.getKeyState(coordinates);
        if (lookup.isEmpty()) {
            log.warn("Identifier has no key state: {}", coordinates);
            return Optional.empty();
        }
        return Optional.of(new ControllableIdentifierImpl(lookup.get()));
    }

    @Override
    public Optional<ControllableIdentifier> controlOf(Identifier identifier) {
        final var lookup = kerl.getKeyState(identifier);
        if (lookup.isEmpty()) {
            log.warn("Identifier has no key state: {}", identifier);
            return Optional.empty();
        }
        return Optional.of(new ControllableIdentifierImpl(lookup.get()));
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
    public Optional<ControllableIdentifier> newDelegatedIdentifier(Identifier delegator) {
        return null;
    }

    @Override
    public Optional<ControllableIdentifier> newIdentifier(Identifier identifier, BasicIdentifier... witnesses) {
        return newIdentifier(identifier, IdentifierSpecification.newBuilder().setWitnesses(Arrays.asList(witnesses)));
    }

    @Override
    public Optional<ControllableIdentifier> newIdentifier(Identifier identifier, IdentifierSpecification.Builder spec) {
        IdentifierSpecification.Builder specification = spec.clone();

        var initialKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        var nextKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);

        specification.addKey(initialKeyPair.getPublic()).setSigningThreshold(unweighted(1))
                     .setNextKeys(List.of(nextKeyPair.getPublic())).setSigner(0, initialKeyPair.getPrivate());

        InceptionEvent event = this.eventFactory.inception(identifier, specification.build());
        KeyState state = kerl.append(event);
        if (state == null) {
            log.warn("Invalid event produced creating: {}", identifier);
            return Optional.empty();
        }

        KeyCoordinates keyCoordinates = KeyCoordinates.of(event, 0);

        keyStore.storeKey(keyCoordinates, initialKeyPair);
        keyStore.storeNextKey(keyCoordinates, nextKeyPair);
        ControllableIdentifier cid = new ControllableIdentifierImpl(state);

        log.info("New {} Identifier: {} prefix: {} coordinates: {} cur key: {} next key: {}",
                 specification.getWitnesses().isEmpty() ? "Private" : "Public", identifier, cid.getIdentifier(),
                 keyCoordinates, shortQb64(initialKeyPair.getPublic()), shortQb64(nextKeyPair.getPublic()));
        return Optional.of(cid);
    }

    protected Optional<Signer> getSigner(Identifier identifier, int keyIndex) {
        Optional<KeyState> state = kerl.getKeyState(identifier);
        if (state.isEmpty()) {
            log.warn("Identifier not found in KEL: {}", identifier);
            return Optional.empty();
        }
        Optional<KeyPair> keyPair = getKeyPair(identifier, keyIndex, state.get(),
                                               kerl.getKeyEvent(state.get().getLastEstablishmentEvent()));
        if (keyPair.isEmpty()) {
            log.warn("Last establishment event not found in KEL: {} : {} missing: {}", identifier,
                     state.get().getCoordinates(), state.get().getLastEstablishmentEvent());
            return Optional.empty();
        }

        return Optional.of(new Signer.SignerImpl(keyIndex, keyPair.get().getPrivate()));
    }

    protected Optional<Verifier> getVerifier(Identifier identifier, int keyIndex) {
        Optional<KeyState> state = kerl.getKeyState(identifier);
        if (state.isEmpty()) {
            log.warn("Identifier not found in KEL: {}", identifier);
            return Optional.empty();
        }

        final var publicKey = state.get().getKeys().get(keyIndex);
        var ops = SignatureAlgorithm.lookup(publicKey);
        return Optional.of(new Verifier.DefaultVerifier(ops, publicKey));
    }

    protected Optional<KeyState> rotate(Identifier identifier) {
        return rotate(identifier, RotationSpecification.newBuilder());
    }

    protected Optional<KeyState> rotate(Identifier identifier, List<Seal> seals) {
        Builder builder = RotationSpecification.newBuilder().addAllSeals(seals);
        return rotate(identifier, builder);
    }

    protected Optional<KeyState> rotate(Identifier identifier, RotationSpecification.Builder spec) {
        RotationSpecification.Builder specification = spec.clone();

        Optional<KeyState> state = kerl.getKeyState(identifier);

        if (state.isEmpty()) {
            log.warn("Identifier not found in KEL: {}", identifier);
            return Optional.empty();
        }

        if (state.get().getNextKeyConfigurationDigest().isEmpty()) {
            log.warn("Identifier cannot be rotated: {}", identifier);
            return Optional.empty();
        }

        var lastEstablishing = kerl.getKeyEvent(state.get().getLastEstablishmentEvent());
        if (lastEstablishing.isEmpty()) {
            log.warn("Identifier cannot be rotated: {} estatblishment event missing: {}", identifier);
            return Optional.empty();
        }
        EstablishmentEvent establishing = (EstablishmentEvent) lastEstablishing.get();
        var currentKeyCoordinates = KeyCoordinates.of(establishing, 0);

        KeyPair nextKeyPair = keyStore.getNextKey(currentKeyCoordinates)
                                      .orElseThrow(() -> new IllegalArgumentException("next key pair for identifier not found in keystore: "
                                      + currentKeyCoordinates));

        KeyPair newNextKeyPair = spec.getSignatureAlgorithm().generateKeyPair(entropy);

        specification.setSigningThreshold(unweighted(1)).setIdentifier(identifier)
                     .setDigestAlgorithm(kerl.getDigestAlgorithm()).setCurrentCoords(state.get().getCoordinates())
                     .setCurrentDigest(state.get().getDigest()).setKey(nextKeyPair.getPublic())
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

        return Optional.of(newState);
    }

    protected Optional<KeyState> seal(Identifier identifier, InteractionSpecification.Builder spec) {
        InteractionSpecification.Builder specification = spec.clone();
        Optional<KeyState> state = kerl.getKeyState(identifier);

        if (state.isEmpty()) {
            log.warn("Identifier not found in KEL: {}", identifier);
            return Optional.empty();
        }
        Optional<KeyEvent> lastEstablishmentEvent = kerl.getKeyEvent(state.get().getLastEstablishmentEvent());
        if (lastEstablishmentEvent.isEmpty()) {
            log.warn("missing establishment event: {} can't find: {}",
                     kerl.getKeyEvent(state.get().getCoordinates()).get(), state.get().getLastEstablishmentEvent());
            return Optional.empty();
        }
        KeyCoordinates currentKeyCoordinates = KeyCoordinates.of((EstablishmentEvent) lastEstablishmentEvent.get(), 0);

        Optional<KeyPair> keyPair = keyStore.getKey(currentKeyCoordinates);

        if (keyPair.isEmpty()) {
            log.warn("Key pair for identifier not found in keystore: {}", identifier);
        }

        specification.setPriorEventDigest(state.get().getDigest()).setLastEvent(state.get().getLastEvent())
                     .setIdentifier(identifier).setSigner(0, keyPair.get().getPrivate());

        KeyEvent event = eventFactory.interaction(specification.build());
        KeyState newKeyState = kerl.append(event);
        return Optional.of(newKeyState);
    }

    protected Optional<KeyState> seal(Identifier identifier, List<Seal> seals) {
        InteractionSpecification.Builder builder = InteractionSpecification.newBuilder();
        builder.setseals(seals);
        return seal(identifier, builder);
    }

    protected Optional<EventSignature> sign(Identifier identifier, KeyEvent event) {
        Optional<KeyState> state = kerl.getKeyState(identifier);
        if (state.isEmpty()) {
            return Optional.empty();
        }
        byte[] bs = event.getBytes();

        return Optional.of(new EventSignature(event.getCoordinates(),
                                              kerl.getKeyEvent(state.get().getLastEstablishmentEvent()).get()
                                                  .getCoordinates(),
                                              IntStream.range(0, state.get().getKeys().size() - 1)
                                                       .mapToObj(i -> getSigner(identifier, i))
                                                       .filter(s -> s.isPresent()).map(s -> s.get())
                                                       .collect(Collectors.toMap(s -> s.keyIndex(), s -> s.sign(bs)))));
    }

    private Optional<KeyPair> getKeyPair(Identifier identifier, int keyIndex, KeyState state,
                                         Optional<KeyEvent> lastEstablishmentEvent) {
        if (lastEstablishmentEvent.isEmpty()) {
            return Optional.empty();
        }
        KeyCoordinates keyCoords = KeyCoordinates.of((EstablishmentEvent) lastEstablishmentEvent.get(), keyIndex);
        return getKeyPair(identifier, keyCoords);
    }

    private Optional<KeyPair> getKeyPair(Identifier identifier, KeyCoordinates keyCoords) {
        return keyStore.getKey(keyCoords);
    }
}
