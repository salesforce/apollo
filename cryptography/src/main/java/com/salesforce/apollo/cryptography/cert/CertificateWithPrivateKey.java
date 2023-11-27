/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.cryptography.cert;

import java.security.PrivateKey;
import java.security.cert.X509Certificate;

/**
 * @author hal.hildebrand
 *
 */
public class CertificateWithPrivateKey {
    private final X509Certificate cert;

    private final PrivateKey privateKey;

    public CertificateWithPrivateKey(X509Certificate cert, PrivateKey privateKey) {
        this.cert = cert;
        this.privateKey = privateKey;
    }

    public PrivateKey getPrivateKey() {
        return privateKey;
    }

    public X509Certificate getX509Certificate() {
        return cert;
    }
}
