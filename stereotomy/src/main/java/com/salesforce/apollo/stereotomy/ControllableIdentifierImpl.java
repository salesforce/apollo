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
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.InceptionEvent.ConfigurationTrait;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.Seal;
import com.salesforce.apollo.stereotomy.event.SigningThreshold;
import com.salesforce.apollo.stereotomy.identifier.BasicIdentifier;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * @author hal.hildebrand
 *
 */
public class ControllableIdentifierImpl implements ControllableIdentifier {
    private final Controller controller;
    private final KeyState   state;

    public ControllableIdentifierImpl(Controller controller, KeyState state) {
        this.controller = controller;
        this.state = state;
    }

    @Override
    public Set<ConfigurationTrait> configurationTraits() {
        return state.configurationTraits();
    }

    @Override
    public EventCoordinates getCoordinates() {
        return state.getCoordinates();
    }

    @Override
    public boolean getDelegated() {
        return state.getDelegated();
    }

    @Override
    public Optional<Identifier> getDelegatingIdentifier() {
        return state.getDelegatingIdentifier();
    }

    @Override
    public Digest getDigest() {
        return state.getDigest();
    }

    @Override
    public Identifier getIdentifier() {
        return state.getIdentifier();
    }

    @Override
    public List<PublicKey> getKeys() {
        return state.getKeys();
    }

    @Override
    public EstablishmentEvent getLastEstablishmentEvent() {
        return state.getLastEstablishmentEvent();
    }

    @Override
    public KeyEvent getLastEvent() {
        return state.getLastEvent();
    }

    @Override
    public Optional<Digest> getNextKeyConfigurationDigest() {
        return state.getNextKeyConfigurationDigest();
    }

    @Override
    public long getSequenceNumber() {
        return state.getSequenceNumber();
    }

    @Override
    public SigningThreshold getSigningThreshold() {
        return state.getSigningThreshold();
    }

    @Override
    public List<BasicIdentifier> getWitnesses() {
        return state.getWitnesses();
    }

    @Override
    public int getWitnessThreshold() {
        return state.getWitnessThreshold();
    }

    @Override
    public boolean isTransferable() {
        return state.isTransferable();
    }

    @Override
    public void rotate() {
        // TODO Auto-generated method stub

    }

    @Override
    public void rotate(List<Seal> seals) {
        // TODO Auto-generated method stub

    }

    @Override
    public void seal(List<Seal> seals) {
        // TODO Auto-generated method stub

    }

    @Override
    public EventSignature sign(KeyEvent event) {
        // TODO Auto-generated method stub
        return null;
    }
}
