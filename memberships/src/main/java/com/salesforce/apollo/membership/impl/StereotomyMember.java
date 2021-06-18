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
import com.salesforce.apollo.crypto.ssl.CertificateValidator;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.stereotomy.LimitedController;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;

import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;

/**
 * @author hal.hildebrand
 *
 */
public class StereotomyMember implements SigningMember {

    private final SelfAddressingIdentifier identifier;
    private final LimitedController        controller;

    public StereotomyMember(SelfAddressingIdentifier identifier, LimitedController controller) {
        this.identifier = identifier;
        this.controller = controller;
    }

    @Override
    public int compareTo(Member o) {
        return identifier.getDigest().compareTo(o.getId());
    }

    @Override
    public X509Certificate getCertificate() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SelfAddressingIdentifier getIdentifier() {
        return identifier;
    }

    @Override
    public Digest getId() {
        return identifier.getDigest();
    }

    @Override
    public boolean verify(JohnHancock signature, InputStream message) {
        return controller.verify(identifier, signature, message);
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
        return controller.sign(identifier, message);
    }

}
