/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.stereotomy;

import java.io.InputStream;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SigningThreshold;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;

/**
 * @author hal.hildebrand
 *
 */
public class IdentifierMember implements Member {

    private final EstablishmentEvent event;

    public IdentifierMember(EstablishmentEvent event) {
        if (!(event.getIdentifier() instanceof SelfAddressingIdentifier)) {
            throw new IllegalArgumentException("Event identifier must be self identifying: "
            + event.getIdentifier().getClass());
        }
        this.event = event;
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
        return verifier().filtered(threshold, signature, message);
    }

    @Override
    public Digest getId() {
        return ((SelfAddressingIdentifier) event.getIdentifier()).getDigest();
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + getId();
    }

    @Override
    public boolean verify(JohnHancock signature, InputStream message) {
        return verifier().verify(signature, message);
    }

    @Override
    public boolean verify(SigningThreshold threshold, JohnHancock signature, InputStream message) {
        return verifier().verify(threshold, signature, message);
    }

    private Verifier verifier() {
        return new DefaultVerifier(event.getKeys());
    }
}
