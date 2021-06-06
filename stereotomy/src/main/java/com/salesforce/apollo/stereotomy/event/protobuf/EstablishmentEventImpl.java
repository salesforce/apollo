/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event.protobuf;

import static com.salesforce.apollo.crypto.QualifiedBase64.digest;
import static com.salesforce.apollo.crypto.QualifiedBase64.publicKey;

import java.security.PublicKey;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.salesfoce.apollo.stereotomy.event.proto.Establishment;
import com.salesfoce.apollo.stereotomy.event.proto.Header;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.SigningThreshold;

/**
 * @author hal.hildebrand
 *
 */
abstract public class EstablishmentEventImpl extends KeyEventImpl implements EstablishmentEvent {

    private final Establishment establishment;

    public EstablishmentEventImpl(Header header, Establishment establishment) {
        super(header);
        this.establishment = establishment;
    }

    @Override
    public List<PublicKey> getKeys() {
        return establishment.getKeysList().stream().map(bs -> publicKey(bs)).collect(Collectors.toList());
    }

    @Override
    public Optional<Digest> getNextKeyConfiguration() {
        return establishment.getNextKeyConfiguration().isEmpty() ? Optional.empty()
                : Optional.of(digest(establishment.getNextKeyConfiguration()));
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
