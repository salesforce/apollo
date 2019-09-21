package io.github.olivierlemasle.ca;

import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.bouncycastle.pkcs.PKCS10CertificationRequestBuilder;
import org.bouncycastle.pkcs.jcajce.JcaPKCS10CertificationRequestBuilder;

class CsrBuilderImpl implements CsrBuilder {
  private static final String SIGNATURE_ALGORITHM = "SHA256withRSA";

  @Override
  public CsrWithPrivateKey generateRequest(final DistinguishedName dn) {
    final KeyPair pair = KeysUtil.generateKeyPair();
    try {
      final PrivateKey privateKey = pair.getPrivate();
      final PublicKey publicKey = pair.getPublic();
      final X500Name x500Name = dn.getX500Name();
      final ContentSigner signGen = new JcaContentSignerBuilder(SIGNATURE_ALGORITHM)
          .build(privateKey);
      final PKCS10CertificationRequestBuilder builder = new JcaPKCS10CertificationRequestBuilder(
          x500Name, publicKey);
      final PKCS10CertificationRequest csr = builder.build(signGen);
      return new CsrWithPrivateKeyImpl(csr, privateKey);
    } catch (final OperatorCreationException e) {
      throw new CaException(e);
    }
  }

}
