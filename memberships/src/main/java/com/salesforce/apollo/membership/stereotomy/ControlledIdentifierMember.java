/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.stereotomy;

import java.io.InputStream;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.SigningThreshold;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;

/**
 * @author hal.hildebrand
 *
 */
public class ControlledIdentifierMember implements SigningMember {

    private final ControlledIdentifier<SelfAddressingIdentifier> identifier;

    public ControlledIdentifierMember(ControlledIdentifier<SelfAddressingIdentifier> identifier) {
        this.identifier = identifier;
    }

    @Override
    public SignatureAlgorithm algorithm() {
        var signer = identifier.getSigner();
        if (signer.isEmpty()) {
            return SignatureAlgorithm.NULL_SIGNATURE;
        }
        return signer.get().algorithm();
    }

    @Override
    public int compareTo(Member o) {
        return getId().compareTo(o.getId());
    }

    @Override
    // The id of a member uniquely identifies it
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if ((obj == null) || !(obj instanceof Member))
            return false;
        return getId().equals(((Member) obj).getId());
    }

    @Override
    public Filtered filtered(SigningThreshold threshold, JohnHancock signature, InputStream message) {
        var verifier = identifier.getVerifier();
        if (verifier.isEmpty()) {
            return null;
        }
        return verifier.get().filtered(threshold, signature, message);
    }

    public EstablishmentEvent getEvent() {
        return identifier.getLastEstablishingEvent().get();
    }

    @Override
    public Digest getId() {
        return identifier.getIdentifier().getDigest();
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }

    @Override
    public JohnHancock sign(InputStream message) {
        var signer = identifier.getSigner();
        if (signer.isEmpty()) {
            throw new IllegalStateException("cannot obtain signer for: " + getId());
        }
        return signer.get().sign(message);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + getId();
    }

    @Override
    public boolean verify(JohnHancock signature, InputStream message) {
        var verifier = identifier.getVerifier();
        if (verifier.isEmpty()) {
            return false;
        }
        return verifier.get().verify(signature, message);
    }

    @Override
    public boolean verify(SigningThreshold threshold, JohnHancock signature, InputStream message) {
        var verifier = identifier.getVerifier();
        if (verifier.isEmpty()) {
            return false;
        }
        return verifier.get().verify(threshold, signature, message);
    }

}
