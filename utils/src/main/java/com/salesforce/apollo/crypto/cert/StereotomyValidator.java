/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.crypto.cert;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import com.salesforce.apollo.crypto.ssl.CertificateValidator;

/**
 * @author hal.hildebrand
 *
 */
public class StereotomyValidator implements CertificateValidator {

    @Override
    public void validateClient(X509Certificate[] chain) throws CertificateException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void validateServer(X509Certificate[] chain) throws CertificateException {
        // TODO Auto-generated method stub
        
    }

}
