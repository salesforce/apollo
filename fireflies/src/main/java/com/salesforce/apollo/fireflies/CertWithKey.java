/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.apollo.fireflies;

import java.security.PrivateKey;
import java.security.cert.X509Certificate;

/**
 * @author hhildebrand
 */
public class CertWithKey {

    private final X509Certificate certificate;
    private final PrivateKey privateKey;

    public CertWithKey(X509Certificate certificate, PrivateKey privateKey) {
        this.certificate = certificate;
        this.privateKey = privateKey;
    }

    public X509Certificate getCertificate() {
        return certificate;
    }

    public PrivateKey getPrivateKey() {
        return privateKey;
    }
}
