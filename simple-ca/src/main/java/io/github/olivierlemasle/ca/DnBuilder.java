package io.github.olivierlemasle.ca;

public interface DnBuilder {

  public DnBuilder setCn(String cn);

  public DnBuilder setCommonName(String cn);

  public DnBuilder setL(String l);

  public DnBuilder setLocalityName(String l);

  public DnBuilder setSt(String st);

  public DnBuilder setStateOrProvinceName(String st);

  public DnBuilder setO(String o);

  public DnBuilder setOrganizationName(String o);

  public DnBuilder setOu(String ou);

  public DnBuilder setOrganizationalUnitName(String ou);

  public DnBuilder setC(String c);

  public DnBuilder setCountryName(String c);

  public DnBuilder setStreet(String street);

  public DistinguishedName build();

}
