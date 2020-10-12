/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;

import org.bouncycastle.asn1.oiw.OIWObjectIdentifiers;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.AuthorityKeyIdentifier;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.SubjectKeyIdentifier;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.cert.X509ExtensionUtils;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.DigestCalculator;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.bc.BcDigestCalculatorProvider;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

/**
 * @author hal.hildebrand
 *
 */
public class TestCertUtils {

    public static X509Certificate generate() {
        KeyPairGenerator keyPairGenerator;
        try {
            keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        keyPairGenerator.initialize(512);
        KeyPair keyPair = keyPairGenerator.generateKeyPair();
        try {
            return generate(keyPair, "SHA256withRSA", "localhost", 730);
        } catch (OperatorCreationException | CertificateException | CertIOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static X509Certificate generate(final KeyPair keyPair, final String hashAlgorithm, final String cn,
                                           final int days) throws OperatorCreationException, CertificateException,
                                                           CertIOException {
        final Instant now = Instant.now();
        final Date notBefore = Date.from(now);
        final Date notAfter = Date.from(now.plus(Duration.ofDays(days)));

        final ContentSigner contentSigner = new JcaContentSignerBuilder(hashAlgorithm).build(keyPair.getPrivate());
        final X500Name x500Name = new X500Name("CN=" + cn);
        final X509v3CertificateBuilder certificateBuilder = new JcaX509v3CertificateBuilder(x500Name, BigInteger
                                                                                                                .valueOf(now.toEpochMilli()),
                notBefore, notAfter, x500Name, keyPair.getPublic())
                                                                   .addExtension(Extension.subjectKeyIdentifier, false,
                                                                                 createSubjectKeyId(keyPair.getPublic()))
                                                                   .addExtension(Extension.authorityKeyIdentifier,
                                                                                 false,
                                                                                 createAuthorityKeyId(keyPair.getPublic()))
                                                                   .addExtension(Extension.basicConstraints, true,
                                                                                 new BasicConstraints(true));

        return new JcaX509CertificateConverter().setProvider(new BouncyCastleProvider())
                                                .getCertificate(certificateBuilder.build(contentSigner));
    }

    /**
     * Creates the hash value of the authority public key.
     *
     * @param publicKey of the authority certificate
     *
     * @return AuthorityKeyIdentifier hash
     *
     * @throws OperatorCreationException
     */
    private static AuthorityKeyIdentifier createAuthorityKeyId(final PublicKey publicKey) throws OperatorCreationException {
        final SubjectPublicKeyInfo publicKeyInfo = SubjectPublicKeyInfo.getInstance(publicKey.getEncoded());
        final DigestCalculator digCalc = new BcDigestCalculatorProvider().get(new AlgorithmIdentifier(
                OIWObjectIdentifiers.idSHA1));

        return new X509ExtensionUtils(digCalc).createAuthorityKeyIdentifier(publicKeyInfo);
    }

    /**
     * Creates the hash value of the public key.
     *
     * @param publicKey of the certificate
     *
     * @return SubjectKeyIdentifier hash
     *
     * @throws OperatorCreationException
     */
    private static SubjectKeyIdentifier createSubjectKeyId(final PublicKey publicKey) throws OperatorCreationException {
        final SubjectPublicKeyInfo publicKeyInfo = SubjectPublicKeyInfo.getInstance(publicKey.getEncoded());
        final DigestCalculator digCalc = new BcDigestCalculatorProvider().get(new AlgorithmIdentifier(
                OIWObjectIdentifiers.idSHA1));

        return new X509ExtensionUtils(digCalc).createSubjectKeyIdentifier(publicKeyInfo);
    }
}
