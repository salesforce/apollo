package io.github.olivierlemasle.ca;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

class KeyStoreReaderImpl implements KeyStoreReader {
  static final String KEYSTORE_TYPE = "PKCS12";

  @Override
  public List<String> listAliases(String keyStorePath, char[] password) {
    final File file = new File(keyStorePath);
    return listAliases(file, password);
  }

  @Override
  public List<String> listAliases(File keyStoreFile, char[] password) {
    try {
      if (!keyStoreFile.exists() || !keyStoreFile.isFile())
        return new ArrayList<>();

      final KeyStore keyStore = KeyStore.getInstance(KEYSTORE_TYPE);
      try (InputStream stream = new FileInputStream(keyStoreFile)) {
        keyStore.load(stream, password);
      }
      return listAliases(keyStore);
    } catch (KeyStoreException | IOException | CertificateException | NoSuchAlgorithmException e) {
      throw new CaException(e);
    }

  }

  @Override
  public List<String> listAliases(KeyStore keyStore) {
    final List<String> res = new ArrayList<>();
    try {
      for (final Enumeration<String> aliases = keyStore.aliases(); aliases.hasMoreElements();) {
        res.add(aliases.nextElement());
      }
      return res;
    } catch (final KeyStoreException e) {
      throw new CaException(e);
    }

  }

}
