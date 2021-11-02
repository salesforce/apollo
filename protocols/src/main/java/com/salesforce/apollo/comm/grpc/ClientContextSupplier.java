/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm.grpc;

import java.security.Provider;

import com.salesforce.apollo.crypto.ssl.CertificateValidator;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;

/**
 * @author hal.hildebrand
 *
 */
public interface ClientContextSupplier {

    SslContext forClient(ClientAuth clientAuth, String alias, CertificateValidator validator, Provider provider,
                         String tlsVersion);
}
