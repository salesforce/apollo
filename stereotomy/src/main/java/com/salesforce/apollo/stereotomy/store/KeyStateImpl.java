/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.store;

import static com.salesforce.apollo.crypto.QualifiedBase64.digest;
import static com.salesforce.apollo.crypto.QualifiedBase64.publicKey;
import static com.salesforce.apollo.stereotomy.identifier.QualifiedBase64Identifier.identifier;

import java.security.PublicKey;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.salesfoce.apollo.stereotomy.event.proto.StoredKeyState;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.EventCoordinates;
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

    private final StoredKeyState state;

    public KeyStateImpl(StoredKeyState state) {
        this.state = state;
    }

    @Override
    public Set<ConfigurationTrait> configurationTraits() {
        return state.getConfigurationTraitsList()
                    .stream()
                    .map(s -> ConfigurationTrait.valueOf(s))
                    .collect(Collectors.toSet());
    }

    @Override
    public EventCoordinates getCoordinates() {
        return new EventCoordinates(identifier(state.getCoordinates().getIdentifier()),
                state.getCoordinates().getSequenceNumber(), digest(state.getCoordinates().getDigest()));
    }

    @Override
    public Optional<Identifier> getDelegatingIdentifier() {
        return state.getDelegatingIdentifier().isEmpty() ? Optional.empty()
                : Optional.of(identifier(state.getDelegatingIdentifier()));
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
        String nextKeyConfigurationDigest = state.getNextKeyConfigurationDigest();
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

}
