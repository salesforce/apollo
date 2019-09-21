package io.github.olivierlemasle.ca;

import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.time.ZonedDateTime;

import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;

import io.github.olivierlemasle.ca.Signer.SignerWithSerial;
import io.github.olivierlemasle.ca.ext.CrlDistPointExtension;
import io.github.olivierlemasle.ca.ext.KeyUsageExtension;
import io.github.olivierlemasle.ca.ext.KeyUsageExtension.KeyUsage;

class RootCertificateBuilderImpl implements RootCertificateBuilder {

  private String crlUri = null;

  private final KeyPair pair;
  private final SignerWithSerial signer;

  RootCertificateBuilderImpl(final DistinguishedName subject) {
    pair = KeysUtil.generateKeyPair();
    signer = new SignerImpl(pair, subject, pair.getPublic(), subject)
        .setRandomSerialNumber();
  }

  @Override
  public RootCertificateBuilder setNotBefore(final ZonedDateTime notBefore) {
    signer.setNotBefore(notBefore);
    return this;
  }

  @Override
  public RootCertificateBuilder setNotAfter(final ZonedDateTime notAfter) {
    signer.setNotAfter(notAfter);
    return this;
  }

  @Override
  public RootCertificateBuilder validDuringYears(final int years) {
    signer.validDuringYears(years);
    return this;
  }

  @Override
  public RootCertificateBuilder setCrlUri(final String crlUri) {
    this.crlUri = crlUri;
    return this;
  }

  @Override
  public RootCertificate build() {
    signer.addExtension(KeyUsageExtension.create(
        KeyUsage.KEY_CERT_SIGN,
        KeyUsage.CRL_SIGN));

    if (crlUri != null) {
      signer.addExtension(CrlDistPointExtension.create(crlUri));
    }

    // This is a CA
    signer.addExtension(Extension.basicConstraints, false, new BasicConstraints(true));

    final X509Certificate rootCertificate = signer.sign(true).getX509Certificate();

    return new RootCertificateImpl(rootCertificate, pair.getPrivate());
  }

}
