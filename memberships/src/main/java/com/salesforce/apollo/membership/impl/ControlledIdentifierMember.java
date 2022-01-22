/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.impl;

import java.io.InputStream;
import java.security.Provider;
import java.security.cert.X509Certificate;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.SigningThreshold;
import com.salesforce.apollo.crypto.ssl.CertificateValidator;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;

import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;

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
    public int compareTo(Member o) {
        return getId().compareTo(o.getId());
    }

    @Override
    public X509Certificate getCertificate() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Digest getId() {
        return identifier.getIdentifier().getDigest();
    }

    @Override
    public boolean verify(JohnHancock signature, InputStream message) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Filtered filtered(SigningThreshold threshold, JohnHancock signature, InputStream message) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean verify(SigningThreshold threshold, JohnHancock signature, InputStream message) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public SignatureAlgorithm algorithm() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SslContext forServer(ClientAuth clientAuth, String alias, CertificateValidator validator, Provider provider,
                                String tlsVersion) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SslContext forClient(ClientAuth clientAuth, String alias, CertificateValidator validator, Provider provider,
                                String tlsVersion) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public JohnHancock sign(InputStream message) {
        // TODO Auto-generated method stub
        return null;
    }

}
