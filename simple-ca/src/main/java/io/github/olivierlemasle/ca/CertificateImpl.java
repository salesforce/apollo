package io.github.olivierlemasle.ca;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

import org.bouncycastle.openssl.jcajce.JcaPEMWriter;

class CertificateImpl implements Certificate {
  private final X509Certificate certificate;

  CertificateImpl(final X509Certificate certificate) {
    this.certificate = certificate;
  }

  @Override
  public X509Certificate getX509Certificate() {
    return certificate;
  }

  @Override
  public String print() {
    final StringWriter sw = new StringWriter();
    try {
      try (JcaPEMWriter writer = new JcaPEMWriter(sw)) {
        writer.writeObject(certificate);
        writer.flush();
        return sw.toString();
      }
    } catch (final IOException e) {
      throw new CaException(e);
    }
  }

  @Override
  public void save(final File file) {
    try {
      try (BufferedWriter fw = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8,
          StandardOpenOption.CREATE)) {
        try (JcaPEMWriter writer = new JcaPEMWriter(fw)) {
          writer.writeObject(certificate);
          writer.flush();
        }
      }
    } catch (final IOException e) {
      throw new CaException(e);
    }
  }

  @Override
  public void save(final String fileName) {
    final File file = new File(fileName);
    save(file);
  }

  @Override
  public CertificateWithPrivateKey attachPrivateKey(PrivateKey privateKey) {
    return new CertificateWithPrivateKeyImpl(certificate, privateKey);
  }

}
