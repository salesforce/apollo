/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm;

import java.net.SocketAddress;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

import com.salesforce.apollo.crypto.ssl.CertificateValidator;
import com.salesforce.apollo.membership.Member;

import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;

/**
 * @author hal.hildebrand
 *
 */
public class StandardEpProvider implements EndpointProvider {

    private final SocketAddress   bindAddress;
    private final X509Certificate certificate;
    private final ClientAuth      clientAuth;
    private final PrivateKey      privateKey;
    private final CertificateValidator       validator;

    public StandardEpProvider(SocketAddress bindAddress, X509Certificate certificate, PrivateKey privateKey,
            ClientAuth clientAuth, CertificateValidator validator) {
        this.bindAddress = bindAddress;
        this.certificate = certificate;
        this.privateKey = privateKey;
        this.clientAuth = clientAuth;
        this.validator = validator;
    }

    @Override
    public SocketAddress addressFor(Member to) {
        return Member.portsFrom(to.getCertificate());
    }

    @Override
    public String getAlias() {
        return "node";
    }

    @Override
    public SocketAddress getBindAddress() {
        return bindAddress;
    }

    @Override
    public X509Certificate getCertificate() {
        return certificate;
    }

    @Override
    public ClientAuth getClientAuth() {
        return clientAuth;
    }

    @Override
    public PrivateKey getPrivateKey() {
        return privateKey;
    }

    @Override
    public CertificateValidator getValiator() {
        return validator;
    }

}
