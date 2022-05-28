/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.crypto.cert;

import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Date;
import java.util.List;

import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.CertIOException;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.utils.Entropy;

/**
 * @author hal.hildebrand
 *
 */
public class Certificates {
    public static X509Certificate selfSign(boolean useSubjectKeyIdentifier, BcX500NameDnImpl dn,
                                           BigInteger serialNumber, KeyPair keyPair, Instant notBefore,
                                           Instant notAfter, List<CertExtension> extensions) {
        return sign(useSubjectKeyIdentifier, dn, keyPair, serialNumber, notBefore, notAfter, extensions, dn,
                    keyPair.getPublic());
    }

    public static X509Certificate selfSign(boolean useSubjectKeyIdentifier, BcX500NameDnImpl dn, KeyPair keyPair,
                                           Instant notBefore, Instant notAfter, List<CertExtension> extensions) {
        return sign(useSubjectKeyIdentifier, dn, keyPair, serialNumber(), notBefore, notAfter, extensions, dn,
                    keyPair.getPublic());
    }

    public static X509Certificate sign(boolean useSubjectKeyIdentifier, BcX500NameDnImpl signerDn,
                                       KeyPair signerKeyPair, BigInteger serialNumber, Instant notBefore,
                                       Instant notAfter, List<CertExtension> extensions, BcX500NameDnImpl dn,
                                       PublicKey signedKey) {
        try {
            final ContentSigner sigGen = new JcaContentSignerBuilder(SignatureAlgorithm.lookup(signerKeyPair.getPrivate())
                                                                                       .signatureInstanceName()).build(signerKeyPair.getPrivate());

            final SubjectPublicKeyInfo subPubKeyInfo = SubjectPublicKeyInfo.getInstance(signedKey.getEncoded());

            final JcaX509ExtensionUtils extUtils = new JcaX509ExtensionUtils();
            final X509v3CertificateBuilder certBuilder = new X509v3CertificateBuilder(signerDn.getX500Name(),
                                                                                      serialNumber,
                                                                                      Date.from(notBefore),
                                                                                      Date.from(notAfter), dn
                                                                                                             .getX500Name(),
                                                                                      subPubKeyInfo).addExtension(Extension.authorityKeyIdentifier,
                                                                                                                  false,
                                                                                                                  extUtils.createAuthorityKeyIdentifier(signerKeyPair.getPublic()));

            if (useSubjectKeyIdentifier) {
                certBuilder.addExtension(Extension.subjectKeyIdentifier, false,
                                         extUtils.createSubjectKeyIdentifier(signerKeyPair.getPublic()));
            }

            for (final CertExtension e : extensions) {
                certBuilder.addExtension(e.getOid(), e.isCritical(), e.getValue());
            }

            final X509CertificateHolder holder = certBuilder.build(sigGen);
            final X509Certificate cert = new JcaX509CertificateConverter().getCertificate(holder);

            cert.checkValidity();
            cert.verify(signerKeyPair.getPublic());

            return cert;
        } catch (final OperatorCreationException | CertificateException | InvalidKeyException | NoSuchAlgorithmException
        | NoSuchProviderException | SignatureException | CertIOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static BigInteger serialNumber() {
        byte[] sn = new byte[64];
        Entropy.nextSecureBytes(sn);
        return new BigInteger(sn);
    }
}
