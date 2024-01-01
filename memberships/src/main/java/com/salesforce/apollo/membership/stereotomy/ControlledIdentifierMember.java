/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.stereotomy;

import com.salesforce.apollo.stereotomy.event.proto.KERL_;
import com.salesforce.apollo.cryptography.*;
import com.salesforce.apollo.cryptography.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.KERL.EventWithAttachments;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;

import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * @author hal.hildebrand
 */
public class ControlledIdentifierMember implements SigningMember {

    private final Digest                                         id;
    private final ControlledIdentifier<SelfAddressingIdentifier> identifier;

    public ControlledIdentifierMember(ControlledIdentifier<SelfAddressingIdentifier> identifier) {
        this.identifier = identifier;
        this.id = identifier.getIdentifier().getDigest();
    }

    @Override
    public SignatureAlgorithm algorithm() {
        Signer signer = identifier.getSigner();
        return signer.algorithm();
    }

    @Override
    public int compareTo(Member o) {
        return id.compareTo(o.getId());
    }

    @Override
    // The id of a member uniquely identifies it
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if ((obj == null) || !(obj instanceof Member))
            return false;
        return id.equals(((Member) obj).getId());
    }

    @Override
    public Filtered filtered(SigningThreshold threshold, JohnHancock signature, InputStream message) {
        var verifier = identifier.getVerifier();
        if (verifier.isEmpty()) {
            return null;
        }
        return verifier.get().filtered(threshold, signature, message);
    }

    public CertificateWithPrivateKey getCertificateWithPrivateKey(Instant validFrom, Duration valid,
                                                                  SignatureAlgorithm signatureAlgorithm) {
        return identifier.provision(validFrom, valid, signatureAlgorithm);
    }

    public EstablishmentEvent getEvent() {
        return identifier.getLastEstablishingEvent();
    }

    @Override
    public Digest getId() {
        return id;
    }

    public ControlledIdentifier<SelfAddressingIdentifier> getIdentifier() {
        return identifier;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    public KERL_ kerl() {
        List<EventWithAttachments> ker = identifier.getKerl();
        return kerl(ker);
    }

    @Override
    public JohnHancock sign(InputStream message) {
        Signer signer = identifier.getSigner();
        return signer.sign(message);
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

    private KERL_ kerl(List<EventWithAttachments> kerl) {
        return KERL_.newBuilder().addAllEvents(kerl.stream().map(ewa -> ewa.toKeyEvente()).toList()).build();
    }
}
