package com.salesforce.apollo.cryptography.cert;

import java.io.IOException;

import javax.security.auth.x500.X500Principal;

import org.bouncycastle.asn1.x500.X500Name;

public class BcX500NameDnImpl {
    private final X500Name x500Name;

    public BcX500NameDnImpl(final String name) {
        this.x500Name = new X500Name(name);
    }

    public BcX500NameDnImpl(final X500Name name) {
        this.x500Name = name;
    }

    BcX500NameDnImpl(final X500Principal principal) {
        this.x500Name = X500Name.getInstance(principal.getEncoded());
    }

    public byte[] getEncoded() {
        try {
            return x500Name.getEncoded();
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public String getName() {
        return x500Name.toString();
    }

    public X500Name getX500Name() {
        return x500Name;
    }

    public X500Principal getX500Principal() {
        try {
            return new X500Principal(x500Name.getEncoded());
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String toString() {
        return getName();
    }

}
