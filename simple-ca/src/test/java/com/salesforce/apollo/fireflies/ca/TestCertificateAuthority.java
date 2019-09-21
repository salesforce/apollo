/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.apollo.fireflies.ca;

import static io.github.olivierlemasle.ca.CA.createCsr;
import static io.github.olivierlemasle.ca.CA.dn;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import io.github.olivierlemasle.ca.CA;
import io.github.olivierlemasle.ca.CSR;
import io.github.olivierlemasle.ca.Certificate;
import io.github.olivierlemasle.ca.RootCertificate;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class TestCertificateAuthority {

    @Test
    public void testMint() {
        RootCertificate root = CA.createSelfSignedCertificate(dn("CN=Root-Test, O=My Org")).build();
        assertNotNull(root);
        CertificateAuthority ca = new CertificateAuthority(root);
        final CSR csr = createCsr().generateRequest(dn().setCn("test.com")
                                                        .setL("666:667:668")
                                                        .setO("World Company")
                                                        .setOu("IT dep")
                                                        .setSt("CA")
                                                        .setC("US")
                                                        .build());
        Certificate certificate = ca.mintNode(csr);
        assertNotNull(certificate);
    }

}
