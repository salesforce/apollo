/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import java.security.PublicKey;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.stereotomy.event.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.InceptionEvent.ConfigurationTrait;
import com.salesforce.apollo.stereotomy.event.SigningThreshold;
import com.salesforce.apollo.stereotomy.identifier.BasicIdentifier;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * @author hal.hildebrand
 *
 */

public interface KeyState {

    Set<ConfigurationTrait> configurationTraits();

    EventCoordinates getCoordinates();

    default boolean getDelegated() {
        return this.getDelegatingIdentifier().isPresent();
    }

    Optional<Identifier> getDelegatingIdentifier();

    default Digest getDigest() {
        return this.getCoordinates().getDigest();
    }

    default Identifier getIdentifier() {
        return this.getCoordinates().getIdentifier();
    }

    List<PublicKey> getKeys();

    EventCoordinates getLastEstablishmentEvent();

    EventCoordinates getLastEvent();

    Optional<Digest> getNextKeyConfigurationDigest();

    default long getSequenceNumber() {
        return this.getCoordinates().getSequenceNumber();
    }

    SigningThreshold getSigningThreshold();

    List<BasicIdentifier> getWitnesses();

    int getWitnessThreshold();

    default boolean isTransferable() {
        return this.getCoordinates().getIdentifier().isTransferable()
                && this.getNextKeyConfigurationDigest().isPresent();
    }

}
