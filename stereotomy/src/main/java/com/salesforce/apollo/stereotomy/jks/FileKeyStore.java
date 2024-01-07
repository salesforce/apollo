/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.jks;

import com.salesforce.apollo.stereotomy.KeyCoordinates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.function.Supplier;

/**
 * @author hal.hildebrand
 */
public class FileKeyStore extends JksKeyStore {
    private static final Logger log = LoggerFactory.getLogger(FileKeyStore.class);

    private final File file;

    /**
     * Provided KeyStore is assumed to be loaded
     */
    public FileKeyStore(KeyStore keyStore, Supplier<char[]> passwordProvider, File file) {
        super(keyStore, passwordProvider);
        this.file = file;
    }

    @Override
    public void removeKey(String alias) {
        super.removeKey(alias);
        save();
    }

    @Override
    public void removeKey(KeyCoordinates keyCoordinates) {
        super.removeKey(keyCoordinates);
        save();
    }

    @Override
    public void removeNextKey(KeyCoordinates keyCoordinates) {
        super.removeNextKey(keyCoordinates);
        save();
    }

    @Override
    public void storeKey(String alias, KeyPair keyPair) {
        super.storeKey(alias, keyPair);
        save();
    }

    private void save() {
        try (var fos = new FileOutputStream(file)) {
            keyStore.store(fos, passwordProvider.get());
        } catch (IOException | KeyStoreException | NoSuchAlgorithmException | CertificateException e) {
            log.error("Cannot store to: {}", file.getAbsolutePath(), e);
            throw new IllegalStateException(e);
        }
    }

}
