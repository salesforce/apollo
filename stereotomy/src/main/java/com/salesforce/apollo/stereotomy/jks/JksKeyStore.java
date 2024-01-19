/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.jks;

import com.salesforce.apollo.cryptography.cert.BcX500NameDnImpl;
import com.salesforce.apollo.cryptography.cert.CertExtension;
import com.salesforce.apollo.cryptography.cert.Certificates;
import com.salesforce.apollo.stereotomy.KeyCoordinates;
import com.salesforce.apollo.stereotomy.StereotomyKeyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static com.salesforce.apollo.cryptography.QualifiedBase64.qb64;
import static com.salesforce.apollo.stereotomy.identifier.QualifiedBase64Identifier.qb64;

/**
 * @author hal.hildebrand
 */
public class JksKeyStore implements StereotomyKeyStore {
    private static final Logger           log  = LoggerFactory.getLogger(JksKeyStore.class);
    protected final      KeyStore         keyStore;
    protected final      Supplier<char[]> passwordProvider;
    private final        Lock             lock = new ReentrantLock();

    public JksKeyStore(KeyStore keyStore, Supplier<char[]> passwordProvider) {
        this.keyStore = keyStore;
        this.passwordProvider = passwordProvider;
    }

    public static String coordinateOrdering(KeyCoordinates coords) {
        var eventCoords = coords.getEstablishmentEvent();
        return qb64(eventCoords.getIdentifier()) + ':' + eventCoords.getSequenceNumber() + ':' + qb64(
        eventCoords.getDigest()) + ":" + coords.getKeyIndex();
    }

    private static String current(KeyCoordinates keyCoordinates) {
        return String.format("%s:%s", coordinateOrdering(keyCoordinates), "0");
    }

    private static String next(KeyCoordinates keyCoordinates) {
        return String.format("%s:%s", coordinateOrdering(keyCoordinates), "1");
    }

    @Override
    public Optional<KeyPair> getKey(String alias) {
        return get(alias, null);
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
            keyStore.deleteEntry(current(keyCoordinates));
        } catch (KeyStoreException e) {
            throw new IllegalStateException("Error deleting current: " + keyCoordinates, e);
        }
    }

    @Override
    public void removeKey(String alias) {
        try {
            keyStore.deleteEntry(alias);
        } catch (KeyStoreException e) {
            throw new IllegalStateException("Error deleting: " + alias, e);
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

    public void storeKey(final String alias, KeyPair keyPair) {
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

    @Override
    public void storeKey(KeyCoordinates keyCoordinates, KeyPair keyPair) {
        lock.lock();
        try {
            storeKey(current(keyCoordinates), keyPair);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void storeNextKey(KeyCoordinates keyCoordinates, KeyPair keyPair) {
        lock.lock();
        try {
            storeKey(next(keyCoordinates), keyPair);
        } finally {
            lock.unlock();
        }
    }

    private Optional<KeyPair> get(String alias, KeyCoordinates keyCoordinates) {
        try {
            if (!keyStore.containsAlias(alias)) {
                return Optional.empty();
            }
        } catch (KeyStoreException e) {
            log.error("Unable to query keystore for: {} : {}", keyCoordinates != null ? keyCoordinates : alias,
                      e.getMessage());
            return Optional.empty();
        }
        Certificate cert;
        try {
            cert = keyStore.getCertificate(alias);
        } catch (KeyStoreException e) {
            log.error("Unable to retrieve certificate for: {} : {}", keyCoordinates != null ? keyCoordinates : alias,
                      e.getMessage());
            return Optional.empty();
        }
        var publicKey = cert.getPublicKey();
        PrivateKey privateKey;
        try {
            privateKey = (PrivateKey) keyStore.getKey(alias, passwordProvider.get());
        } catch (UnrecoverableKeyException | KeyStoreException | NoSuchAlgorithmException e) {
            log.error("Unable to retrieve certificate for: {} : {}", keyCoordinates != null ? keyCoordinates : alias,
                      e.getMessage());
            return Optional.empty();
        }
        return Optional.of(new KeyPair(publicKey, privateKey));
    }
}
