/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.cryptography.ssl;

import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;

public class Trust extends X509ExtendedTrustManager {
    private final CertificateValidator validator;

    public Trust(CertificateValidator validator) {
        this.validator = validator;
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        validator.validateClient(chain);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType,
                                   Socket socket) throws CertificateException {
        validator.validateClient(chain);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType,
                                   SSLEngine engine) throws CertificateException {
        validator.validateClient(chain);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        validator.validateServer(chain);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType,
                                   Socket socket) throws CertificateException {
        validator.validateServer(chain);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType,
                                   SSLEngine arg2) throws CertificateException {
        validator.validateServer(chain);
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
    }

}
