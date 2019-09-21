package io.github.olivierlemasle.ca;

import java.io.File;
import java.security.KeyStore;
import java.util.List;

public interface KeyStoreReader {

  public List<String> listAliases(String keyStorePath, char[] password);

  public List<String> listAliases(File keyStoreFile, char[] password);

  public List<String> listAliases(KeyStore keyStore);

}
