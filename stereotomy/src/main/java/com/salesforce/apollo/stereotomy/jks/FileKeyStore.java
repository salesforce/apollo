/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.jks;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hal.hildebrand
 *
 */
public class FileKeyStore extends JksKeyStore {
    private static final Logger log = LoggerFactory.getLogger(FileKeyStore.class);

    private final File file;

    public FileKeyStore(KeyStore keyStore, Supplier<char[]> passwordProvider, File file) {
        super(keyStore, passwordProvider);
        this.file = file;
    }

    @Override
    protected void store(String alias, KeyPair keyPair) {
        super.store(alias, keyPair);
        try (var fos = new FileOutputStream(file)) {
            keyStore.store(fos, passwordProvider.get());
        } catch (IOException | KeyStoreException | NoSuchAlgorithmException | CertificateException e) {
            log.error("Cannot store to: {}", file.getAbsolutePath(), e);
            throw new IllegalStateException(e);
        }
    }

}
