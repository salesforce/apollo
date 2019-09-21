package io.github.olivierlemasle.ca.ext;

import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.ASN1ObjectIdentifier;

public class CertExtension {
  private final ASN1ObjectIdentifier oid;
  private final boolean isCritical;
  private final ASN1Encodable value;

  public CertExtension(final ASN1ObjectIdentifier oid, final boolean isCritical,
      final ASN1Encodable value) {
    this.oid = oid;
    this.isCritical = isCritical;
    this.value = value;
  }

  public ASN1ObjectIdentifier getOid() {
    return oid;
  }

  public boolean isCritical() {
    return isCritical;
  }

  public ASN1Encodable getValue() {
    return value;
  }

  @Override
  public String toString() {
    return "Extension [" + oid + "=" + value + "]";
  }

}
