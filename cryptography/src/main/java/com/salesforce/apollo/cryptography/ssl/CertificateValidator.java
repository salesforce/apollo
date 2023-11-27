/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.cryptography.ssl;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

/**
 * @author hal.hildebrand
 *
 */
public interface CertificateValidator {

    static final CertificateValidator NONE = new CertificateValidator() {

        @Override
        public void validateClient(X509Certificate[] chain) throws CertificateException {
        }

        @Override
        public void validateServer(X509Certificate[] chain) throws CertificateException {
        }
    };

    void validateClient(X509Certificate[] chain) throws CertificateException;

    void validateServer(X509Certificate[] chain) throws CertificateException;
}
