/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.crypto.ssl;

import java.security.PrivateKey;
import java.security.Provider;
import java.security.cert.X509Certificate;

import javax.net.ssl.KeyManagerFactory;

public class NodeKeyManagerFactory extends KeyManagerFactory {

    public NodeKeyManagerFactory(String alias, X509Certificate certificate, PrivateKey privateKey, Provider provider) {
        super(new NodeKeyManagerFactorySpi(alias, certificate, privateKey), provider, "Keys");
    }

}
