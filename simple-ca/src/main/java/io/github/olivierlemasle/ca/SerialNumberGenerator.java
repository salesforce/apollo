package io.github.olivierlemasle.ca;

import java.math.BigInteger;
import java.security.SecureRandom;

class SerialNumberGenerator {
  private static final int DEFAULT_SERIAL_LENGTH = 128;

  private final SecureRandom random;
  private final int length;

  SerialNumberGenerator() {
    this(new SecureRandom(), DEFAULT_SERIAL_LENGTH);
  }

  SerialNumberGenerator(final SecureRandom random, final int length) {
    this.random = random;
    this.length = length;
  }

  BigInteger generateRandomSerialNumber() {
    return new BigInteger(length, random);
  }

  static SerialNumberGenerator instance = new SerialNumberGenerator();

}
