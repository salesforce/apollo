package io.github.olivierlemasle.ca;

import java.security.PrivateKey;

public interface CsrWithPrivateKey extends CSR {

  public PrivateKey getPrivateKey();
}
