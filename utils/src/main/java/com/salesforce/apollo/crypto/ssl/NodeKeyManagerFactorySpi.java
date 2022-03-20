/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.crypto.ssl;

import java.security.InvalidAlgorithmParameterException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.X509Certificate;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactorySpi;
import javax.net.ssl.ManagerFactoryParameters;

public class NodeKeyManagerFactorySpi extends KeyManagerFactorySpi {

    private final String          alias;
    private final X509Certificate certificate;
    private final PrivateKey      privateKey;

    public NodeKeyManagerFactorySpi(String alias, X509Certificate certificate, PrivateKey privateKey) {
        assert alias != null;
        assert certificate != null;
        assert privateKey != null;
        this.alias = alias;
        this.certificate = certificate;
        this.privateKey = privateKey;
    }

    @Override
    protected KeyManager[] engineGetKeyManagers() {
        return new KeyManager[] { new Keys(alias, certificate, privateKey) };
    }

    @Override
    protected void engineInit(KeyStore ks, char[] password) throws KeyStoreException, NoSuchAlgorithmException,
                                                            UnrecoverableKeyException {
    }

    @Override
    protected void engineInit(ManagerFactoryParameters spec) throws InvalidAlgorithmParameterException {
    }

}