/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo;

import static com.salesforce.apollo.ApolloConfiguration.DEFAULT_CA_ALIAS;
import static com.salesforce.apollo.ApolloConfiguration.DEFAULT_IDENTITY_ALIAS;
import static io.github.olivierlemasle.ca.CA.createCsr;
import static io.github.olivierlemasle.ca.CA.dn;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.salesforce.apollo.fireflies.FirefliesParameters;
import com.salesforce.apollo.fireflies.Member;
import com.salesforce.apollo.fireflies.ca.CertificateAuthority;

import io.github.olivierlemasle.ca.CertificateWithPrivateKey;
import io.github.olivierlemasle.ca.CsrWithPrivateKey;
import io.github.olivierlemasle.ca.RootCertificate;

/**
 * A utility to pre generate CA and member keystores for testing.
 * 
 * @author hal.hildebrand
 * @since 220
 */
public class PregenPopulation {

    public static final File caDir = new File("src/test/resources/ca");
    public static final String caKeystoreFile = "ca.p12";
    public static final int cardinality = 9;
    public static final File memberDir = new File("src/test/resources/members");

    private static final String crlUri = null;
    private static final double faultTolerance = 0.01;
    private static final String MEMBER_P12_TEMPLATE = "member-%s.p12";
    private static final double probabilityByzantine = .25;

    public static int getCardinality() {
        return cardinality;
    }

    public static String getMemberDir() {
        return memberDir.getAbsolutePath();
    }

    public static void main(String[] argv) throws Exception {
        caDir.mkdirs();
        memberDir.mkdirs();
        RootCertificate root = CertificateAuthority.mint(dn().setCn("test-ca.com")
                                                             .setO("World Company")
                                                             .setOu("IT dep")
                                                             .setSt("CA")
                                                             .setC("US")
                                                             .build(),
                                                         cardinality, probabilityByzantine, faultTolerance, crlUri);
        root.exportPkcs12(new File(caDir, caKeystoreFile), ApolloConfiguration.DEFAULT_PASSWORD, "ca");

        FirefliesParameters parameters = new FirefliesParameters(root.getX509Certificate());

        CertificateAuthority ca = new CertificateAuthority(root);
        List<CertificateWithPrivateKey> memberCerts = new ArrayList<>();
        Map<File, KeyStore> keyStores = new HashMap<>();
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
            CertificateWithPrivateKey cert = ca.mintNode(request)
                                               .attachPrivateKey(request.getPrivateKey());
            memberCerts.add(cert);
            KeyStore keyStore = KeyStore.getInstance(ApolloConfiguration.DEFAULT_TYPE);
            keyStore.load(null, null);
            keyStore.setCertificateEntry(DEFAULT_IDENTITY_ALIAS, cert.getX509Certificate());
            keyStore.setKeyEntry(DEFAULT_IDENTITY_ALIAS, cert.getPrivateKey(), ApolloConfiguration.DEFAULT_PASSWORD,
                                 new Certificate[] { cert.getX509Certificate() });
            keyStores.put(new File(memberDir, memberKeystoreFile(i)), keyStore);
        }
        keyStores.values().forEach(ks -> {
            try {
                ks.setCertificateEntry(DEFAULT_CA_ALIAS, root.getX509Certificate());
            } catch (KeyStoreException e) {
                throw new IllegalStateException("Cannot store CA cert", e);
            }
        });

        List<X509Certificate> seeds = new ArrayList<>();
        ArrayNode seedIndexes = JsonNodeFactory.instance.arrayNode();

        while (seeds.size() < parameters.toleranceLevel + 1) {
            int index = parameters.entropy.nextInt(memberCerts.size());
            X509Certificate cert = memberCerts.get(index).getX509Certificate();
            if (!seeds.contains(cert)) {
                seeds.add(cert);
                seedIndexes.add(index);
            }
        }

        for (int i = 0; i < seeds.size(); i++) {
            Certificate cert = seeds.get(i);
            String alias = Apollo.SEED_PREFIX + i;
            keyStores.values().forEach(ks -> {
                try {
                    ks.setCertificateEntry(alias, cert);
                } catch (KeyStoreException e) {
                    throw new IllegalStateException("unable to store seed certificate: " + alias, e);
                }
            });
        }

        keyStores.forEach((file, ks) -> {
            try (OutputStream os = new FileOutputStream(file)) {
                ks.store(os, ApolloConfiguration.DEFAULT_PASSWORD);
            } catch (IOException | KeyStoreException | NoSuchAlgorithmException | CertificateException e) {
                throw new IllegalStateException("unable to store keystore", e);
            }
        });
    }

    public static String memberKeystoreFile(int index) {
        return String.format(MEMBER_P12_TEMPLATE, index);
    }

    public static String memberKeystoreResource(int index) {
        return "/members/" + String.format(MEMBER_P12_TEMPLATE, index);
    }
}
