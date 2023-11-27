/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.identifier;

import static com.salesforce.apollo.cryptography.QualifiedBase64.bs;
import static com.salesforce.apollo.cryptography.QualifiedBase64.publicKey;
import static com.salesforce.apollo.cryptography.QualifiedBase64.shortQb64;

import java.io.InputStream;
import java.security.PublicKey;
import java.util.Objects;

import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.cryptography.proto.PubKey;
import com.salesforce.apollo.cryptography.JohnHancock;
import com.salesforce.apollo.cryptography.SigningThreshold;
import com.salesforce.apollo.cryptography.Verifier;

/**
 * @author hal.hildebrand
 *
 */
public class BasicIdentifier implements Identifier, Verifier {
    private final PublicKey publicKey;

    public BasicIdentifier(PubKey pk) {
        this(publicKey(pk));
    }

    public BasicIdentifier(PublicKey publicKey) {
        this.publicKey = publicKey;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof BasicIdentifier other)) {
            return false;
        }
        return Objects.equals(publicKey, other.publicKey);
    }

    @Override
    public Filtered filtered(SigningThreshold threshold, JohnHancock signature, InputStream message) {
        return new Verifier.DefaultVerifier(publicKey).filtered(threshold, signature, message);
    }

    public PublicKey getPublicKey() {
        return publicKey;
    }

    @Override
    public int hashCode() {
        return Objects.hash(publicKey);
    }

    @Override
    public byte identifierCode() {
        return 2;
    }

    @Override
    public boolean isTransferable() {
        return false;
    }

    @Override
    public Ident toIdent() {
        return Ident.newBuilder().setBasic(bs(publicKey)).build();
    }

    @Override
    public String toString() {
        return "B" + shortQb64(publicKey);
    }

    @Override
    public boolean verify(JohnHancock signature, InputStream message) {
        return new Verifier.DefaultVerifier(publicKey).verify(signature, message);
    }

    @Override
    public boolean verify(SigningThreshold threshold, JohnHancock signature, InputStream message) {
        return new Verifier.DefaultVerifier(publicKey).verify(threshold, signature, message);
    }
}
