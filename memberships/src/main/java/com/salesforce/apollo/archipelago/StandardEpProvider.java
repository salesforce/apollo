/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipelago;

import com.google.common.net.HostAndPort;
import com.salesforce.apollo.cryptography.ssl.CertificateValidator;
import com.salesforce.apollo.membership.Member;
import io.netty.handler.ssl.ClientAuth;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.function.Function;

/**
 * @author hal.hildebrand
 */
public class StandardEpProvider implements EndpointProvider {

    private final SocketAddress            bindAddress;
    private final ClientAuth               clientAuth;
    private final Function<Member, String> resolver;
    private final CertificateValidator     validator;

    public StandardEpProvider(String bindAddress, ClientAuth clientAuth, CertificateValidator validator,
                              Function<Member, String> resolver) {
        this.bindAddress = reify(bindAddress);
        this.clientAuth = clientAuth;
        this.validator = validator;
        this.resolver = resolver;
    }

    @Override
    public SocketAddress addressFor(Member to) {
        return reify(resolver.apply(to));
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

    private SocketAddress reify(String encoded) {
        var hnp = HostAndPort.fromString(encoded);
        return new InetSocketAddress(hnp.getHost(), hnp.getPort());
    }
}
