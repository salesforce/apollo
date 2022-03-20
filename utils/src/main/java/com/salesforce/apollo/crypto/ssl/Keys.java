/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.crypto.ssl;

import java.net.Socket;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedKeyManager;

public class Keys extends X509ExtendedKeyManager {
    private final String          alias;
    private final X509Certificate certificate;
    private final PrivateKey      privateKey;

    public Keys(String alias, X509Certificate certificate, PrivateKey privateKey) {
        assert privateKey != null;
        assert certificate != null;
        assert alias != null;
        this.alias = alias;
        this.certificate = certificate;
        this.privateKey = privateKey;
    }

    @Override
    public String chooseClientAlias(String[] keyType, Principal[] principals, Socket socket) {
        return alias;
    }

    @Override
    public String chooseEngineClientAlias(String[] keyType, Principal[] issuers, SSLEngine engine) {
        return alias;
    }

    @Override
    public String chooseEngineServerAlias(String keyType, Principal[] issuers, SSLEngine engine) {
        return alias;
    }

    @Override
    public String chooseServerAlias(String s, Principal[] principals, Socket socket) {
        return alias;
    }

    @Override
    public X509Certificate[] getCertificateChain(String s) {
        return new X509Certificate[] { certificate };
    }

    @Override
    public String[] getClientAliases(String keyType, Principal[] principals) {
        return new String[] { alias };
    }

    @Override
    public PrivateKey getPrivateKey(String alias) {
        if (this.alias.equals(alias)) {
            return privateKey;
        }
        return null;
    }

    @Override
    public String[] getServerAliases(String s, Principal[] principals) {
        return new String[] { alias };
    }
}