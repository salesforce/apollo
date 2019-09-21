package io.github.olivierlemasle.ca;

import java.io.File;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

public interface Certificate {

  public X509Certificate getX509Certificate();

  public String print();

  public void save(File file);

  public void save(String fileName);

  public CertificateWithPrivateKey attachPrivateKey(PrivateKey privateKey);

}
