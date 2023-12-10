/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event.protobuf;

import static com.salesforce.apollo.cryptography.QualifiedBase64.digest;
import static com.salesforce.apollo.cryptography.QualifiedBase64.publicKey;

import java.security.PublicKey;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.salesforce.apollo.stereotomy.event.proto.Establishment;
import com.salesforce.apollo.stereotomy.event.proto.EventCommon;
import com.salesforce.apollo.stereotomy.event.proto.Header;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.SigningThreshold;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;

/**
 * @author hal.hildebrand
 */
abstract public class EstablishmentEventImpl extends KeyEventImpl implements EstablishmentEvent {

    private final Establishment establishment;

    public EstablishmentEventImpl(Header header, EventCommon common, Establishment establishment) {
        super(header, common);
        this.establishment = establishment;
    }

    @Override
    public List<PublicKey> getKeys() {
        return establishment.getKeysList().stream().map(bs -> publicKey(bs)).collect(Collectors.toList());
    }

    @Override
    public Optional<Digest> getNextKeysDigest() {
        return establishment.hasNextKeysDigest() ? Optional.of(digest(establishment.getNextKeysDigest()))
                                                 : Optional.empty();
    }

    @Override
    public SigningThreshold getSigningThreshold() {
        return ProtobufEventFactory.toSigningThreshold(establishment.getSigningThreshold());
    }

    @Override
    public int getWitnessThreshold() {
        return establishment.getWitnessThreshold();
    }
}
