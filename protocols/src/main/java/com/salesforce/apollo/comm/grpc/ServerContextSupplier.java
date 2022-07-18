/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm.grpc;

import java.security.Provider;
import java.security.cert.X509Certificate;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.ssl.CertificateValidator;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;

/**
 * @author hal.hildebrand
 *
 */
public interface ServerContextSupplier {

    SslContext forServer(ClientAuth clientAuth, String alias, CertificateValidator validator, Provider provider,
                         String tlsVersion);

    Digest getMemberId(X509Certificate key);

}
