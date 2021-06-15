/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.test.pregen;

import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import com.salesforce.apollo.crypto.cert.CertificateWithPrivateKey;

/**
 * @author hal.hildebrand
 *
 */
public class Util {

    public static CertificateWithPrivateKey loadFrom(final InputStream stream, final char[] password,
                                                     final String alias) {
        try {
            final KeyStore keystore = KeyStore.getInstance("PKCS12");
            keystore.load(stream, password);
            return loadFrom(keystore, alias);
        } catch (KeyStoreException | NoSuchAlgorithmException | CertificateException | IOException e) {
            throw new IllegalStateException("Cannot load from stream", e);
        }
    }

    public static CertificateWithPrivateKey loadFrom(KeyStore keystore, String alias) {
        try {
            final Certificate certificate = keystore.getCertificate(alias);
            final PrivateKey privateKey = (PrivateKey) keystore.getKey(alias, null);
            if (certificate == null || privateKey == null)
                throw new IllegalStateException("Keystore does not contain certificate and key for alias " + alias);
            return new CertificateWithPrivateKey((X509Certificate) certificate, privateKey);
        } catch (KeyStoreException | NoSuchAlgorithmException | UnrecoverableKeyException e) {
            throw new IllegalStateException(e);
        }
    }
}
