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
import com.salesforce.apollo.utils.Utils;
import io.netty.handler.ssl.ClientAuth;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;

/**
 * @author hal.hildebrand
 */
public interface EndpointProvider {
    static String allocatePort() {
        InetSocketAddress addr = null;
        try {
            addr = new InetSocketAddress(InetAddress.getLocalHost(), Utils.allocatePort(InetAddress.getLocalHost()));
        } catch (UnknownHostException e) {
            throw new IllegalStateException("Cannot resolve localhost!", e);
        }
        return HostAndPort.fromParts(addr.getHostName(), addr.getPort()).toString();
    }

    static <T extends SocketAddress> T reify(String encoded) {
        var hnp = HostAndPort.fromString(encoded);
        return (T) new InetSocketAddress(hnp.getHost(), hnp.getPort());
    }

    SocketAddress addressFor(Member to);

    String getAlias();

    SocketAddress getBindAddress();

    ClientAuth getClientAuth();

    CertificateValidator getValidator();

}
