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
public interface EndpointProvider {

    SocketAddress addressFor(Member to);

    String getAlias();

    SocketAddress getBindAddress();

    X509Certificate getCertificate();

    ClientAuth getClientAuth();

    PrivateKey getPrivateKey();

    CertificateValidator getValiator();

}
