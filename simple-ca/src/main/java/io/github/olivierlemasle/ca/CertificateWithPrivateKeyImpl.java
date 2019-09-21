package io.github.olivierlemasle.ca;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import org.bouncycastle.openssl.jcajce.JcaPEMWriter;

public class CertificateWithPrivateKeyImpl extends CertificateImpl implements CertificateWithPrivateKey {
  static final String KEYSTORE_TYPE = "PKCS12";

  private final PrivateKey privateKey;

  public CertificateWithPrivateKeyImpl(final X509Certificate certificate, final PrivateKey privateKey) {
    super(certificate);
    this.privateKey = privateKey;
  }

  @Override
  public KeyStore addToKeystore(KeyStore keyStore, String alias) {
    try {
      final X509Certificate certificate = getX509Certificate();
      final Certificate[] chain = new Certificate[] { certificate };
      keyStore.setKeyEntry(alias, privateKey, null, chain);

      return keyStore;
    } catch (final KeyStoreException e) {
      throw new CaException(e);
    }
  }

  @Override
  public KeyStore saveInPkcs12Keystore(final String alias) {
    try {
      // init keystore
      final KeyStore keyStore = KeyStore.getInstance(KEYSTORE_TYPE);
      keyStore.load(null, null);

      addToKeystore(keyStore, alias);

      return keyStore;
    } catch (KeyStoreException | NoSuchAlgorithmException | CertificateException | IOException e) {
      throw new CaException(e);
    }
  }

  @Override
  public void exportPkcs12(final String keystorePath, final char[] keystorePassword,
      final String alias) {
    final File file = new File(keystorePath);
    exportPkcs12(file, keystorePassword, alias);
  }

  @Override
  public void exportPkcs12(final File keystoreFile, final char[] keystorePassword,
      final String alias) {
    try {
      final KeyStore keyStore;
      if (keystoreFile.exists() && keystoreFile.isFile()) {
        // Load existing keystore
        keyStore = KeyStore.getInstance(KEYSTORE_TYPE);
        try (InputStream stream = new FileInputStream(keystoreFile)) {
          keyStore.load(stream, keystorePassword);
        }
        addToKeystore(keyStore, alias);
      } else {
        keyStore = saveInPkcs12Keystore(alias);
      }
      try (OutputStream stream = new FileOutputStream(keystoreFile)) {
        keyStore.store(stream, keystorePassword);
      }
    } catch (KeyStoreException | IOException | CertificateException | NoSuchAlgorithmException e) {
      throw new CaException(e);
    }
  }

  @Override
  public PrivateKey getPrivateKey() {
    return privateKey;
  }

  @Override
  public String printKey() {
    final StringWriter sw = new StringWriter();
    try {
      try (JcaPEMWriter writer = new JcaPEMWriter(sw)) {
        writer.writeObject(privateKey);
        writer.flush();
        return sw.toString();
      }
    } catch (final IOException e) {
      throw new CaException(e);
    }
  }

  @Override
  public void saveKey(File file) {
    try {
      try (BufferedWriter fw = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8,
          StandardOpenOption.CREATE)) {
        try (JcaPEMWriter writer = new JcaPEMWriter(fw)) {
          writer.writeObject(privateKey);
          writer.flush();
        }
      }
    } catch (final IOException e) {
      throw new CaException(e);
    }
  }

  @Override
  public void saveKey(String fileName) {
    final File file = new File(fileName);
    saveKey(file);
  }

}
