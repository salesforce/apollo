/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.stereotomy;

import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.JohnHancock;
import com.salesforce.apollo.cryptography.SigningThreshold;
import com.salesforce.apollo.cryptography.Verifier;
import com.salesforce.apollo.membership.Member;

import java.io.InputStream;

/**
 *
 */
public class IdentifierMember implements Member {

    private final Verifier verifier;
    private final Digest   id;

    public IdentifierMember(Digest id, Verifier verifier) {
        this.id = id;
        this.verifier = verifier;
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
        return id.equals(((Member) obj).getId());
    }

    @Override
    public Filtered filtered(SigningThreshold threshold, JohnHancock signature, InputStream message) {
        return verifier().filtered(threshold, signature, message);
    }

    @Override
    public Digest getId() {
        return id;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
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
        return verifier;
    }
}
