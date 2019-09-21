package io.github.olivierlemasle.ca;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;

class CsrLoaderImpl implements CsrLoader {
  private final File file;

  CsrLoaderImpl(final File file) {
    this.file = file;
  }

  CsrLoaderImpl(final String fileName) {
    this(new File(fileName));
  }

  @Override
  public CSR getCsr() {
    try {
      try (Reader pemReader = Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8)) {
        try (final PEMParser pemParser = new PEMParser(pemReader)) {
          final Object parsedObj = pemParser.readObject();

          if (parsedObj instanceof PKCS10CertificationRequest) {
            final PKCS10CertificationRequest csr = (PKCS10CertificationRequest) parsedObj;
            return new CsrImpl(csr);
          } else
            throw new CaException("Not a PKCS10CertificationRequest");
        }
      }
    } catch (final IOException e) {
      throw new CaException(e);
    }
  }
}
