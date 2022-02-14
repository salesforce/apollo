/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.crypto.cert;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.crypto.SignatureAlgorithm;

/**
 * @author hal.hildebrand
 *
 */
public class TestCertificates {

    @Test
    public void selfSigned() throws Exception {
        boolean failed = false;
        for (SignatureAlgorithm s : SignatureAlgorithm.values()) {
            if (s.equals(SignatureAlgorithm.NULL_SIGNATURE)) {
                break;
            }
            KeyPair keyPair;
            try {
                keyPair = s.generateKeyPair();
                testSelfSigned(keyPair);
            } catch (Throwable t) {
                failed = true;
                System.out.println("Unable to generate keypair for: " + s);
                t.printStackTrace();
                continue;
            }
        }
        if (failed) {
            fail("Failed for some algorithms");
        }
    }

    void testSelfSigned(KeyPair keyPair) throws CertificateExpiredException, CertificateNotYetValidException {
        BcX500NameDnImpl dn = new BcX500NameDnImpl("CN=0fgdSAGdx_");
        BigInteger sn = BigInteger.valueOf(Long.MAX_VALUE);
        var notBefore = Instant.now();
        var notAfter = Instant.now().plusSeconds(10_000);
        List<CertExtension> extensions = Collections.emptyList();
        X509Certificate selfSignedCert = Certificates.selfSign(true, dn, sn, keyPair, notBefore, notAfter, extensions);
        assertNotNull(selfSignedCert);
        selfSignedCert.checkValidity();
    }
}
