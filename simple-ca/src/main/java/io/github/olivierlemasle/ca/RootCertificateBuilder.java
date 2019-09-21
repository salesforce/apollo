package io.github.olivierlemasle.ca;

import java.time.ZonedDateTime;

public interface RootCertificateBuilder {

  public RootCertificateBuilder setNotBefore(final ZonedDateTime notBefore);

  public RootCertificateBuilder setNotAfter(final ZonedDateTime notAfter);

  public RootCertificateBuilder validDuringYears(final int years);

  public RootCertificateBuilder setCrlUri(final String crlUri);

  public RootCertificate build();

}
