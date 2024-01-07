/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import com.salesforce.apollo.stereotomy.jks.FileKeyStore;

import java.io.File;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.function.Supplier;

/**
 * @author hal.hildebrand
 */
public class FileKeyStoreTest extends StereotomyTests {

    @Override
    protected FileKeyStore initializeKeyStore() {
        var file = new File("target/test-keystore.jks ");
        file.delete();
        final Supplier<char[]> passwordProvider = () -> new char[] { 'f', 'o', 'o' };
        try {
            final var ks = KeyStore.getInstance("jceks");
            ks.load(null, passwordProvider.get());
            return new FileKeyStore(ks, passwordProvider, file);
        } catch (KeyStoreException | NoSuchAlgorithmException | CertificateException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

}
