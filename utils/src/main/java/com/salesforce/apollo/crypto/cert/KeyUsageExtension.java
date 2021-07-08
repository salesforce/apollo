package com.salesforce.apollo.crypto.cert;

import org.bouncycastle.asn1.x509.Extension;

public class KeyUsageExtension extends CertExtension {

  public static enum KeyUsage {
    CRL_SIGN(org.bouncycastle.asn1.x509.KeyUsage.cRLSign),
    DATA_ENCIPHERMENT(org.bouncycastle.asn1.x509.KeyUsage.dataEncipherment),
    DECIPHER_ONLY(org.bouncycastle.asn1.x509.KeyUsage.encipherOnly),
    DIGITAL_SIGNATURE(org.bouncycastle.asn1.x509.KeyUsage.digitalSignature),
    ENCIPHER_ONLY(org.bouncycastle.asn1.x509.KeyUsage.encipherOnly),
    KEY_AGREEMENT(org.bouncycastle.asn1.x509.KeyUsage.keyAgreement),
    KEY_CERT_SIGN(org.bouncycastle.asn1.x509.KeyUsage.keyCertSign),
    KEY_ENCIPHERMENT(org.bouncycastle.asn1.x509.KeyUsage.keyEncipherment),
    NON_REPUDIATION(org.bouncycastle.asn1.x509.KeyUsage.nonRepudiation);

    private final int keyUsage;

    private KeyUsage(final int keyUsage) {
      this.keyUsage = keyUsage;
    }
  }

  public static KeyUsageExtension create(final KeyUsage... usages) {
    return new KeyUsageExtension(usages);
  }

  private static int getUsages(final KeyUsage[] usages) {
    int u = 0;
    for (final KeyUsage ku : usages) {
      u = u | ku.keyUsage;
    }
    return u;
  }

  KeyUsageExtension(final int keyUsages) {
    super(Extension.keyUsage, false, new org.bouncycastle.asn1.x509.KeyUsage(keyUsages));
  }

  KeyUsageExtension(final KeyUsage... usages) {
    this(getUsages(usages));
  }

}
