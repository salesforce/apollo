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

import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * @author hal.hildebrand
 */
public interface EndpointProvider {
    static String allocatePort() {
        var addr = new InetSocketAddress(Utils.allocatePort());
        return HostAndPort.fromParts(addr.getHostName(), addr.getPort()).toString();
    }

    SocketAddress addressFor(Member to);

    String getAlias();

    SocketAddress getBindAddress();

    ClientAuth getClientAuth();

    CertificateValidator getValiator();

}
