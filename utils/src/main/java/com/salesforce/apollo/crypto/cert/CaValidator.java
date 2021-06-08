/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.crypto.cert;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SignatureException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import com.salesforce.apollo.crypto.ssl.CertificateValidator;

/**
 * @author hal.hildebrand
 *
 */
public class CaValidator implements CertificateValidator {
    private final X509Certificate ca;

    public CaValidator(X509Certificate ca) {
        this.ca = ca;
    }

    @Override
    public void validateClient(X509Certificate[] chain) throws CertificateException {
        validateServer(chain);
    }

    @Override
    public void validateServer(X509Certificate[] chain) throws CertificateException {
        chain[0].checkValidity();
        try {
            chain[0].verify(ca.getPublicKey());
        } catch (NoSuchAlgorithmException | InvalidKeyException | SignatureException | CertificateException
                | NoSuchProviderException e) {
            throw new CertificateException("Invalid cert: " + chain[0].getSubjectDN());
        }
    }

}
