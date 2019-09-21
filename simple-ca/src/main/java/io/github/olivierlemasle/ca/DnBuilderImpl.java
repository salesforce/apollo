package io.github.olivierlemasle.ca;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;

class DnBuilderImpl implements DnBuilder {
  private final X500NameBuilder builder;

  DnBuilderImpl() {
    builder = new X500NameBuilder();
  }

  @Override
  public DnBuilder setCn(final String cn) {
    builder.addRDN(BCStyle.CN, cn);
    return this;
  }

  @Override
  public DnBuilder setCommonName(final String cn) {
    return setCn(cn);
  }

  @Override
  public DnBuilder setL(final String l) {
    builder.addRDN(BCStyle.L, l);
    return this;
  }

  @Override
  public DnBuilder setLocalityName(final String l) {
    return setL(l);
  }

  @Override
  public DnBuilder setSt(final String st) {
    builder.addRDN(BCStyle.ST, st);
    return this;
  }

  @Override
  public DnBuilder setStateOrProvinceName(final String st) {
    return setSt(st);
  }

  @Override
  public DnBuilder setO(final String o) {
    builder.addRDN(BCStyle.O, o);
    return this;
  }

  @Override
  public DnBuilder setOrganizationName(final String o) {
    return setO(o);
  }

  @Override
  public DnBuilder setOu(final String ou) {
    builder.addRDN(BCStyle.OU, ou);
    return this;
  }

  @Override
  public DnBuilder setOrganizationalUnitName(final String ou) {
    return setOu(ou);
  }

  @Override
  public DnBuilder setC(final String c) {
    builder.addRDN(BCStyle.C, c);
    return this;
  }

  @Override
  public DnBuilder setCountryName(final String c) {
    return setC(c);
  }

  @Override
  public DnBuilder setStreet(final String street) {
    builder.addRDN(BCStyle.STREET, street);
    return this;
  }

  @Override
  public DistinguishedName build() {
    final X500Name name = builder.build();
    return new BcX500NameDnImpl(name);
  }

}
