package io.github.olivierlemasle.ca;

import java.security.InvalidParameterException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;

public final class KeysUtil {
  private static final String ALGORITHM = "RSA";
  private static final int DEFAULT_KEY_SIZE = 2048;

  private KeysUtil() {
  }

  public static KeyPair generateKeyPair() {
    return generateKeyPair(DEFAULT_KEY_SIZE);
  }

  public static KeyPair generateKeyPair(final int keySize) {
    try {
      final KeyPairGenerator gen = KeyPairGenerator.getInstance(ALGORITHM);
      gen.initialize(keySize);
      return gen.generateKeyPair();
    } catch (final NoSuchAlgorithmException | InvalidParameterException e) {
      throw new CaException(e);
    }
  }
}
