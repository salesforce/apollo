/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import static io.github.olivierlemasle.ca.CA.createCsr;
import static io.github.olivierlemasle.ca.CA.dn;

import java.io.File;

import com.salesforce.apollo.fireflies.ca.CertificateAuthority;

import io.github.olivierlemasle.ca.CA;
import io.github.olivierlemasle.ca.CertificateWithPrivateKey;
import io.github.olivierlemasle.ca.CsrWithPrivateKey;
import io.github.olivierlemasle.ca.RootCertificate;

/**
 * A utility to pre generate CA and member Cert/Key pairs for testing.
 * 
 * @author hal.hildebrand
 * @since 220
 */
public class PregenPopulation {

    private static final String alias = "foo";
    private static final File caDir = new File("src/test/resources/ca");
    private static final String caKeystoreFile = "ca.p12";
    private static final int cardinality = 100;
    private static final String crlUri = null;
    private static final double faultTolerance = 0.01;
    private static final char[] keystorePassword = "".toCharArray();
    private static final String MEMBER_P12_TEMPLATE = "member-%s.p12";
    private static final File memberDir = new File("src/test/resources/members");
    private static final double probabilityByzantine = .25;

    public static RootCertificate getCa() {
        return CA.loadRootCertificate(PregenPopulation.class.getResourceAsStream("/ca/" + caKeystoreFile),
                                      keystorePassword, alias);
    }

    public static CertWithKey getMember(int index) {
        return Util.loadFrom(PregenPopulation.class.getResourceAsStream("/members/" + memberKeystoreFile(index)),
                             keystorePassword, alias);
    }

    public static void main(String[] argv) {
        caDir.mkdirs();
        memberDir.mkdirs();
        RootCertificate root = CertificateAuthority.mint(dn().setCn("test-ca.com")
                                                             .setO("World Company")
                                                             .setOu("IT dep")
                                                             .setSt("CA")
                                                             .setC("US")
                                                             .build(),
                                                         cardinality, probabilityByzantine, faultTolerance, crlUri);
        root.exportPkcs12(new File(caDir, "ca.p12").getAbsolutePath(), keystorePassword, alias);

        CertificateAuthority ca = new CertificateAuthority(root);

        int startPort = 65535 - 1;
        String host = "localhost";
        for (int i = 1; i <= cardinality; i++) {
            int ffPort = startPort--;
            int gPort = startPort--;
            int aPort = startPort--;
            CsrWithPrivateKey request = createCsr().generateRequest(dn().setCn(host)
                                                                        .setL(String.format(Member.PORT_TEMPLATE,
                                                                                            ffPort, gPort, aPort))
                                                                        .setO("World Company")
                                                                        .setOu("IT dep")
                                                                        .setSt("CA")
                                                                        .setC("US")
                                                                        .build());
            CertificateWithPrivateKey cert = ca.mintNode(request).attachPrivateKey(request.getPrivateKey());
            cert.exportPkcs12(new File(memberDir, memberKeystoreFile(i)), keystorePassword, alias);
        }
    }

    public static String memberKeystoreFile(int index) {
        return String.format(MEMBER_P12_TEMPLATE, index);
    }
}
