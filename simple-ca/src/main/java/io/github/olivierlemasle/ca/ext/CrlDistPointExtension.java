package io.github.olivierlemasle.ca.ext;

import org.bouncycastle.asn1.x509.CRLDistPoint;
import org.bouncycastle.asn1.x509.DistributionPoint;
import org.bouncycastle.asn1.x509.DistributionPointName;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.ReasonFlags;

/**
 * CRL Distribution Points
 */
public class CrlDistPointExtension extends CertExtension {

  CrlDistPointExtension(final DistributionPoint... points) {
    super(Extension.cRLDistributionPoints, false, new CRLDistPoint(points));
  }

  /**
   * Creates a {@link CrlDistPointExtension} with only a {@code distributionPoint} URI (no {@code reasons}, no
   * {@code cRLIssuer} specified).
   */
  public static CrlDistPointExtension create(final String uri) {
    return create(NameType.URI, uri);
  }

  /**
   * Creates a {@link CrlDistPointExtension} with only a {@code distributionPoint} {@link GeneralName} (no
   * {@code reasons}, no {@code cRLIssuer} specified).
   */
  public static CrlDistPointExtension create(final NameType type, final String name) {
    return create(type, name, null, null, null);
  }

  public static CrlDistPointExtension create(final NameType distribPointNameType,
      final String distribPointName,
      final NameType crlIssuerNameType,
      final String crlIssuer,
      final ReasonFlags reasons) {
    final DistributionPointName dp = new DistributionPointName(
        distribPointNameType.generalNames(distribPointName));
    final GeneralNames crl;
    if (crlIssuerNameType != null && crlIssuer != null) {
      crl = crlIssuerNameType.generalNames(crlIssuer);
    } else {
      crl = null;
    }
    return create(dp, reasons, crl);
  }

  public static CrlDistPointExtension create(final DistributionPointName distributionPoint,
      final ReasonFlags reasons,
      final GeneralNames cRLIssuer) {
    final DistributionPoint p = new DistributionPoint(distributionPoint, reasons, cRLIssuer);
    return create(p);
  }

  public static CrlDistPointExtension create(final DistributionPoint... points) {
    return new CrlDistPointExtension(points);
  }

}
