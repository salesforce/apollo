/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.mvlog;

import static com.salesforce.apollo.crypto.QualifiedBase64.digest;
import static com.salesforce.apollo.crypto.QualifiedBase64.publicKey;
import static com.salesforce.apollo.stereotomy.identifier.QualifiedBase64Identifier.identifier;

import java.security.PublicKey;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.Format;
import com.salesforce.apollo.stereotomy.event.InceptionEvent.ConfigurationTrait;
import com.salesforce.apollo.stereotomy.event.SigningThreshold;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.BasicIdentifier;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * @author hal.hildebrand
 *
 */
public class KeyStateImpl implements KeyState {

    private final com.salesfoce.apollo.stereotomy.event.proto.KeyState state;

    public KeyStateImpl(com.salesfoce.apollo.stereotomy.event.proto.KeyState state) {
        this.state = state;
    }

    @Override
    public Set<ConfigurationTrait> configurationTraits() {
        return state.getConfigurationTraitsList()
                    .stream()
                    .map(s -> ConfigurationTrait.valueOf(s))
                    .collect(Collectors.toSet());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T convertTo(Format format) {
        if (format == Format.PROTOBUF) {
            return (T) state;
        }
        throw new IllegalArgumentException("Cannot transform into format: " + format);
    }

    @Override
    public EventCoordinates getCoordinates() {
        com.salesfoce.apollo.stereotomy.event.proto.EventCoordinates coordinates = state.getCoordinates();
        return new EventCoordinates(coordinates.getIlk(), identifier(coordinates.getIdentifier()),
                coordinates.getSequenceNumber(), digest(coordinates.getDigest()));
    }

    @Override
    public Optional<Identifier> getDelegatingIdentifier() {
        Identifier identifier = identifier(state.getDelegatingIdentifier());
        return identifier.isNone() ? Optional.empty() : Optional.of(identifier);
    }

    @Override
    public Digest getDigest() {
        return digest(state.getDigest());
    }

    @Override
    public List<PublicKey> getKeys() {
        return state.getKeysList().stream().map(s -> publicKey(s)).collect(Collectors.toList());
    }

    @Override
    public EventCoordinates getLastEstablishmentEvent() {
        return ProtobufEventFactory.toCoordinates(state.getLastEstablishmentEvent());
    }

    @Override
    public EventCoordinates getLastEvent() {
        return ProtobufEventFactory.toCoordinates(state.getLastEvent());
    }

    @Override
    public Optional<Digest> getNextKeyConfigurationDigest() {
        ByteString nextKeyConfigurationDigest = state.getNextKeyConfigurationDigest();
        return nextKeyConfigurationDigest.isEmpty() ? Optional.empty()
                : Optional.of(digest(nextKeyConfigurationDigest));
    }

    @Override
    public SigningThreshold getSigningThreshold() {
        return ProtobufEventFactory.toSigningThreshold(state.getSigningThreshold());
    }

    @Override
    public List<BasicIdentifier> getWitnesses() {
        return state.getWitnessesList()
                    .stream()
                    .map(s -> identifier(s))
                    .filter(i -> i instanceof BasicIdentifier)
                    .filter(i -> i != null)
                    .map(i -> (BasicIdentifier) i)
                    .collect(Collectors.toList());
    }

    @Override
    public int getWitnessThreshold() {
        return state.getWitnessThreshold();
    }

    @Override
    public String toString() {
        return "KeyStateImpl\n" + state + "\n";
    }

}
