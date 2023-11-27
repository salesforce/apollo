/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.cryptography.ssl;

import java.security.InvalidAlgorithmParameterException;
import java.security.KeyStore;
import java.security.KeyStoreException;

import javax.net.ssl.ManagerFactoryParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactorySpi;

public class NodeTrustManagerFactorySpi extends TrustManagerFactorySpi {

    private final CertificateValidator validator;

    public NodeTrustManagerFactorySpi(CertificateValidator validator) {
        this.validator = validator;
    }

    @Override
    protected TrustManager[] engineGetTrustManagers() {
        return new TrustManager[] { new Trust(validator) };
    }

    @Override
    protected void engineInit(KeyStore ks) throws KeyStoreException {
    }

    @Override
    protected void engineInit(ManagerFactoryParameters spec) throws InvalidAlgorithmParameterException {
    }

}
