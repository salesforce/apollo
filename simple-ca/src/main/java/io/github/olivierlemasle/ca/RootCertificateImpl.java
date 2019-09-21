package io.github.olivierlemasle.ca;

import static io.github.olivierlemasle.ca.CA.dn;

import java.io.IOException;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;

import org.bouncycastle.cert.X509CertificateHolder;

class RootCertificateImpl extends CertificateWithPrivateKeyImpl implements RootCertificate { 

  private final X509CertificateHolder caCertificateHolder;

  RootCertificateImpl(final X509Certificate caCertificate, final PrivateKey caPrivateKey) {
    super(caCertificate, caPrivateKey);
    try {
      this.caCertificateHolder = new X509CertificateHolder(caCertificate.getEncoded());
    } catch (CertificateEncodingException | IOException e) {
      throw new CaException(e);
    }
  }

  @Override
  public Signer signCsr(final CSR request) {
    final KeyPair pair = new KeyPair(getX509Certificate().getPublicKey(), getPrivateKey());
    final DistinguishedName signerSubject = dn(caCertificateHolder.getSubject());
    return new SignerImpl(pair, signerSubject, request.getPublicKey(), request.getSubject());
  }

}
