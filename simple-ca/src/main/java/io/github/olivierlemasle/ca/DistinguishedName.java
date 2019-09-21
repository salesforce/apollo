package io.github.olivierlemasle.ca;

import javax.security.auth.x500.X500Principal;

import org.bouncycastle.asn1.x500.X500Name;

public interface DistinguishedName {

  public X500Name getX500Name();

  public X500Principal getX500Principal();

  public byte[] getEncoded();

  public String getName();
}
