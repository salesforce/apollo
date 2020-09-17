/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.security.cert.X509Certificate;

import com.salesforce.apollo.protocols.HashKey;

/**
 * A member of the view
 * 
 * @author hal.hildebrand
 * @since 220
 */
public class Member implements Comparable<Member> {

    /**
     * Signing identity
     */
    private final X509Certificate certificate;

    /**
     * Unique ID of the memmber
     */
    private final HashKey id;

    protected Member(HashKey id, X509Certificate c) {
        certificate = c;
        this.id = id;
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
        if (obj == null)
            return false;
        if (!(obj instanceof Member))
            return false;
        return id.equals(((Member) obj).id);
    }

    /**
     * Answer the Signature, initialized with the member's public key, using the
     * supplied signature algorithm.
     * 
     * @param signatureAlgorithm
     * @return the signature, initialized for verification
     */
    public Signature forVerification(String signatureAlgorithm) {
        Signature signature;
        try {
            signature = Signature.getInstance(signatureAlgorithm);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("no such algorithm: " + signatureAlgorithm, e);
        }
        try {
            signature.initVerify(certificate.getPublicKey());
        } catch (InvalidKeyException e) {
            throw new IllegalStateException("invalid public key", e);
        }
        return signature;
    }

    /**
     * @return the identifying certificate of the member
     */
    public X509Certificate getCertificate() {
        return certificate;
    }

    /**
     * @return the unique id of this member
     */
    public HashKey getId() {
        return id;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return "Member[" + id + "]";
    }

}
