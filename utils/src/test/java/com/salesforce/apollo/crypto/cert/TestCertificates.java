/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.crypto.cert;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.crypto.SignatureAlgorithm;

/**
 * @author hal.hildebrand
 *
 */
public class TestCertificates {

    @BeforeAll
    public static void beforeClass() {
        // secp256k1 is considered "unsecure" so you have enable it like this:
        System.setProperty("jdk.sunec.disableNative", "false");
    }

    @Test
    public void selfSigned() throws Exception {
        for (SignatureAlgorithm s : SignatureAlgorithm.values()) {
            KeyPair keyPair = s.generateKeyPair();

            testSelfSigned(keyPair);
        }
    }

    void testSelfSigned(KeyPair keyPair) throws CertificateExpiredException, CertificateNotYetValidException {
        BcX500NameDnImpl dn = new BcX500NameDnImpl("CN=0fgdSAGdx_");
        BigInteger sn = BigInteger.valueOf(Long.MAX_VALUE);
        Date notBefore = Date.from(Instant.now());
        Date notAfter = Date.from(Instant.now().plusSeconds(10_000));
        List<CertExtension> extensions = Collections.emptyList();
        X509Certificate selfSignedCert = Certificates.selfSign(true, dn, sn, keyPair, notBefore, notAfter, extensions);
        assertNotNull(selfSignedCert);
        selfSignedCert.checkValidity();
    }
}
                                                                                                                                                                                                                    