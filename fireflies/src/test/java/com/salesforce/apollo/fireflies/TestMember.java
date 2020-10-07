/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import static io.github.olivierlemasle.ca.CA.createCsr;
import static io.github.olivierlemasle.ca.CA.dn;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.fireflies.ca.CertificateAuthority;
import com.salesforce.apollo.protocols.HashKey;

import io.github.olivierlemasle.ca.CSR;
import io.github.olivierlemasle.ca.Certificate;
import io.github.olivierlemasle.ca.RootCertificate;

public class TestMember {

    @Test
    public void testMember() {
        RootCertificate root = CertificateAuthority.mint(dn().setCn("test-ca.com")
                                                             .setO("World Company")
                                                             .setOu("IT dep")
                                                             .setSt("CA")
                                                             .setC("US")
                                                             .build(),
                                                         10_000, 0.012, 25, "");
        assertNotNull(root);
        CertificateAuthority ca = new CertificateAuthority(root);
        final CSR csr = createCsr().generateRequest(dn().setCn("test.com")
                                                        .setL("1638:1639:1640")
                                                        .setO("World Company")
                                                        .setOu("IT dep")
                                                        .setSt("CA")
                                                        .setC("US")
                                                        .build());
        Certificate certificate = ca.mintNode(csr);

        FirefliesParameters parameters = new FirefliesParameters(root.getX509Certificate(), 0.25);
        Participant m = new Participant(certificate.getX509Certificate(), parameters);
        HashKey id = m.getId();
        assertNotNull(id);
    }
}
