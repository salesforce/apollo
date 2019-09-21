package io.github.olivierlemasle.ca;

import java.io.File;
import java.io.FileInputStream;
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

final class RootCertificateLoader {
    private RootCertificateLoader() {}

    static RootCertificateImpl loadRootCertificate(final String keystorePath, final char[] password,
            final String alias) {
        final File file = new File(keystorePath);
        return loadRootCertificate(file, password, alias);
    }

    static RootCertificateImpl loadRootCertificate(final File keystoreFile, final char[] password, final String alias) {
        try (InputStream stream = new FileInputStream(keystoreFile)) {
            return loadRootCertificate(stream, password, alias);
        } catch (IOException e) {
            throw new CaException(e);
        } 
    }

    public static RootCertificateImpl loadRootCertificate(InputStream stream, final char[] password,
            final String alias) {
        try {
            final KeyStore keystore = KeyStore.getInstance(RootCertificateImpl.KEYSTORE_TYPE);
            keystore.load(stream, password);
            return loadRootCertificate(keystore, alias);
        } catch (KeyStoreException | NoSuchAlgorithmException | CertificateException | IOException e) {
            throw new CaException(e);
        }
    }

    static RootCertificateImpl loadRootCertificate(final KeyStore keystore, final String alias) {
        try {
            final Certificate certificate = keystore.getCertificate(alias);
            final PrivateKey privateKey = (PrivateKey)keystore.getKey(alias, null);
            if (certificate == null || privateKey == null)
                throw new CaException("Keystore does not contain certificate and key for alias " + alias);
            return new RootCertificateImpl((X509Certificate)certificate, privateKey);
        } catch (KeyStoreException | NoSuchAlgorithmException | UnrecoverableKeyException e) {
            throw new CaException(e);
        }
    }

}
