package io.github.olivierlemasle.ca;

public interface CsrBuilder {

  public CsrWithPrivateKey generateRequest(DistinguishedName name);

}
