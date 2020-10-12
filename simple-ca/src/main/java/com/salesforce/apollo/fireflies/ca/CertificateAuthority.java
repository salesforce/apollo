/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.ca;

import static io.github.olivierlemasle.ca.CA.dn;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;

import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.SubjectKeyIdentifier;

import io.github.olivierlemasle.ca.CA;
import io.github.olivierlemasle.ca.CSR;
import io.github.olivierlemasle.ca.Certificate;
import io.github.olivierlemasle.ca.DistinguishedName;
import io.github.olivierlemasle.ca.DnBuilder;
import io.github.olivierlemasle.ca.RootCertificate;
import io.github.olivierlemasle.ca.Signer.SignerWithSerial;
import io.github.olivierlemasle.ca.ext.CertExtension;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class CertificateAuthority {

    public static Map<String, String> decodeDN(String dn) {
        LdapName ldapDN;
        try {
            ldapDN = new LdapName(dn);
        } catch (InvalidNameException e) {
            throw new IllegalArgumentException("invalid DN: " + dn, e);
        }
        Map<String, String> decoded = new HashMap<>();
        ldapDN.getRdns().forEach(rdn -> decoded.put(rdn.getType(), (String) rdn.getValue()));
        return decoded;
    }

    public static RootCertificate mint(DistinguishedName dn, int cardinality, double probabilityByzantine,
                                       double faultTolerance, String crlUri) {

        DnBuilder builder = dn();
        decodeDN(dn.getName()).entrySet().stream().filter(e -> !e.getKey().equals("O")).forEach(e -> {
            addToBuilder(builder, e);
        });

        builder.setOrganizationName(String.format("%s:%.6f:%.6f", cardinality, faultTolerance, probabilityByzantine));

        return CA.createSelfSignedCertificate(builder.build()).setCrlUri(crlUri).build();
    }

    private static void addToBuilder(DnBuilder builder, Entry<String, String> e) {
        switch (e.getKey()) {
        case "CN":
            builder.setCommonName(e.getValue());
            break;
        case "OU":
            builder.setOu(e.getValue());
            break;
        case "C":
            builder.setCountryName(e.getValue());
            break;
        case "ST":
            builder.setStreet(e.getValue());
            break;
        case "L":
            builder.setLocalityName(e.getValue());
            break;
        default:
            throw new IllegalArgumentException("Unknown component: " + e.getKey());
        }
    }

    private final SecureRandom entropy;

    private final RootCertificate root;

    public CertificateAuthority(RootCertificate root) {
        this(root, new SecureRandom());
    }

    public CertificateAuthority(RootCertificate root, SecureRandom entropy) {
        this.root = root;
        this.entropy = entropy;
    }

    public X509Certificate getRoot() {
        return root.getX509Certificate();
    }

    /**
     * Mint a certificate request for a new node. Validate the request, assign the
     * generated Certificate the supplied serialNumber. Generate a new id for this
     * request and place that value in the "subject key identifier" of the
     * certificate.
     * 
     * @param request - the CSR
     * @return Certificate - the signed certificate
     */
    public Certificate mintNode(CSR request) {

        byte[] cereal = new byte[16];
        entropy.nextBytes(cereal);
        SignerWithSerial signer = root.signCsr(request).setSerialNumber(new BigInteger(cereal));

        byte[] id = new byte[32];
        entropy.nextBytes(id);
        signer.addExtension(new CertExtension(Extension.subjectKeyIdentifier, false, new SubjectKeyIdentifier(id)));
        signer.addExtension(Extension.basicConstraints, false, new BasicConstraints(false));
        Certificate cert = signer.sign(false);
        return cert;
    }
}
