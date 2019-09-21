package io.github.olivierlemasle.ca.ext;

import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.KeyPurposeId;

public class ExtKeyUsageExtension extends CertExtension {

  ExtKeyUsageExtension(final ExtendedKeyUsage extendedKeyUsage) {
    super(Extension.extendedKeyUsage, false, extendedKeyUsage);
  }

  ExtKeyUsageExtension(final KeyPurposeId usage) {
    this(new ExtendedKeyUsage(usage));
  }

  ExtKeyUsageExtension(final KeyPurposeId[] usages) {
    this(new ExtendedKeyUsage(usages));
  }

  public static ExtKeyUsageExtension create(final KeyPurposeId usage) {
    return new ExtKeyUsageExtension(usage);
  }

  public static ExtKeyUsageExtension create(final KeyPurposeId... usages) {
    return new ExtKeyUsageExtension(usages);
  }

}
