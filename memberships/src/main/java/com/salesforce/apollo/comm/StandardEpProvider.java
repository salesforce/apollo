/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm;

import java.net.SocketAddress;

import com.salesforce.apollo.crypto.ssl.CertificateValidator;
import com.salesforce.apollo.membership.Member;

import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;

/**
 * @author hal.hildebrand
 *
 */
public class StandardEpProvider implements EndpointProvider {

    private final SocketAddress        bindAddress;
    private final ClientAuth           clientAuth;
    private final CertificateValidator validator;

    public StandardEpProvider(SocketAddress bindAddress, ClientAuth clientAuth, CertificateValidator validator) {
        this.bindAddress = bindAddress;
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
    public ClientAuth getClientAuth() {
        return clientAuth;
    }

    @Override
    public CertificateValidator getValiator() {
        return validator;
    }

}
