package io.github.olivierlemasle.ca.ext;

import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;

public enum NameType {
  OTHER_NAME(GeneralName.otherName),
  RFC_822_NAME(GeneralName.rfc822Name),
  DNS_NAME(GeneralName.dNSName),
  X400_NAME(GeneralName.x400Address),
  DIRECTORY_NAME(GeneralName.directoryName),
  EDI_PARTY_NAME(GeneralName.ediPartyName),
  /**
   * URI : Uniform Resource Identifier
   */
  URI(GeneralName.uniformResourceIdentifier),
  IP_ADDRESS(GeneralName.iPAddress),
  REGISTERED_ID(GeneralName.registeredID);

  private final int id;

  private NameType(final int id) {
    this.id = id;
  }

  public GeneralName generalName(final String name) {
    return new GeneralName(id, name);
  }

  public GeneralNames generalNames(final String name) {
    return new GeneralNames(generalName(name));
  }
}
