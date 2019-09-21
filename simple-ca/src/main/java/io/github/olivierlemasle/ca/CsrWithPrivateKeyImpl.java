package io.github.olivierlemasle.ca;

import java.security.PrivateKey;

import org.bouncycastle.pkcs.PKCS10CertificationRequest;

class CsrWithPrivateKeyImpl extends CsrImpl implements CsrWithPrivateKey {
  private final PrivateKey privateKey;

  CsrWithPrivateKeyImpl(final PKCS10CertificationRequest request, final PrivateKey privateKey) {
    super(request);
    this.privateKey = privateKey;
  }

  @Override
  public PrivateKey getPrivateKey() {
    return privateKey;
  }

}
