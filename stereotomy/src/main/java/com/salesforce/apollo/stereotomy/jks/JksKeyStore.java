/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.jks;

import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;
import static com.salesforce.apollo.stereotomy.identifier.QualifiedBase64Identifier.qb64;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.crypto.cert.BcX500NameDnImpl;
import com.salesforce.apollo.crypto.cert.CertExtension;
import com.salesforce.apollo.crypto.cert.Certificates;
import com.salesforce.apollo.stereotomy.KeyCoordinates;
import com.salesforce.apollo.stereotomy.StereotomyKeyStore;

/**
 * @author hal.hildebrand
 *
 */
public class JksKeyStore implements StereotomyKeyStore {
    private static Logger log = LoggerFactory.getLogger(JksKeyStore.class);

    public static String coordinateOrdering(KeyCoordinates coords) {
        var eventCoords = coords.getEstablishmentEvent();
        return qb64(eventCoords.getIdentifier()) + ':' + eventCoords.getSequenceNumber() + ':'
        + qb64(eventCoords.getDigest()) + ":" + Integer.toString(coords.getKeyIndex());
    }

    private static String current(KeyCoordinates keyCoordinates) {
        return String.format("%s:%s", coordinateOrdering(keyCoordinates), "0");
    }

    private static String next(KeyCoordinates keyCoordinates) {
        return String.format("%s:%s", coordinateOrdering(keyCoordinates), "1");
    }

    protected final KeyStore         keyStore;
    protected final Supplier<char[]> passwordProvider;

    public JksKeyStore(KeyStore keyStore, Supplier<char[]> passwordProvider) {
        this.keyStore = keyStore;
        this.passwordProvider = passwordProvider;
    }

    @Override
    public Optional<KeyPair> getKey(KeyCoordinates keyCoordinates) {
        return get(current(keyCoordinates), keyCoordinates);
    }

    @Override
    public Optional<KeyPair> getNextKey(KeyCoordinates keyCoordinates) {
        return get(next(keyCoordinates), keyCoordinates);
    }

    @Override
    public void removeKey(KeyCoordinates keyCoordinates) {
        try {
            keyStore.deleteEntry(next(keyCoordinates));
        } catch (KeyStoreException e) {
            throw new IllegalStateException("Error deleting current: " + keyCoordinates, e);
        }
    }

    @Override
    public void removeNextKey(KeyCoordinates keyCoordinates) {
        try {
            keyStore.deleteEntry(next(keyCoordinates));
        } catch (KeyStoreException e) {
            throw new IllegalStateException("Error deleting next: " + keyCoordinates, e);
        }
    }

    @Override
    public void storeKey(KeyCoordinates keyCoordinates, KeyPair keyPair) {
        final var alias = current(keyCoordinates);
        store(alias, keyPair);
    }

    @Override
    public void storeNextKey(KeyCoordinates keyCoordinates, KeyPair keyPair) {
        store(next(keyCoordinates), keyPair);
    }

    protected void store(final String alias, KeyPair keyPair) {
        BcX500NameDnImpl dn = new BcX500NameDnImpl("CN=noop");
        BigInteger sn = BigInteger.valueOf(Long.MAX_VALUE);
        var notBefore = Instant.now();
        var notAfter = Instant.now().plusSeconds(2_000_000_000);
        List<CertExtension> extensions = Collections.emptyList();
        X509Certificate selfSignedCert = Certificates.selfSign(true, dn, sn, keyPair, notBefore, notAfter, extensions);
        try {
            keyStore.setKeyEntry(alias, keyPair.getPrivate(), passwordProvider.get(),
                                 new Certificate[] { selfSignedCert });
        } catch (KeyStoreException e) {
            throw new IllegalStateException(e);
        }
    }

    private Optional<KeyPair> get(String alias, KeyCoordinates keyCoordinates) {
        try {
            if (!keyStore.containsAlias(alias)) {
                return Optional.empty();
            }
        } catch (KeyStoreException e) {
            log.error("Unable to query keystore for: {}", keyCoordinates, e);
            return Optional.empty();
        }
        Certificate cert;
        try {
            cert = keyStore.getCertificate(alias);
        } catch (KeyStoreException e) {
            log.error("Unable to retrieve certificate for: {}", keyCoordinates, e);
            return Optional.empty();
        }
        var publicKey = cert.getPublicKey();
        PrivateKey privateKey;
        try {
            privateKey = (PrivateKey) keyStore.getKey(alias, passwordProvider.get());
        } catch (UnrecoverableKeyException | KeyStoreException | NoSuchAlgorithmException e) {
            log.error("Unable to retrieve certificate for: {}", keyCoordinates, e);
            return Optional.empty();
        }
        return Optional.of(new KeyPair(publicKey, privateKey));
    }
}
