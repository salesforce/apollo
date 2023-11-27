/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.impl;

import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.JohnHancock;
import com.salesforce.apollo.cryptography.SignatureAlgorithm;
import com.salesforce.apollo.cryptography.Signer;
import com.salesforce.apollo.cryptography.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import org.joou.ULong;

import java.io.InputStream;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.X509Certificate;

/**
 * A signiner member of a view. This is a local member to the process that can sign and assert things.
 *
 * @author hal.hildebrand
 */
public class SigningMemberImpl extends MemberImpl implements SigningMember {

    private final Signer signer;

    /**
     * @param cert
     */
    public SigningMemberImpl(CertificateWithPrivateKey cert, ULong sequenceNumber) {
        this(Member.getMemberIdentifier(cert.getX509Certificate()), cert.getX509Certificate(), cert.getPrivateKey(),
             new SignerImpl(cert.getPrivateKey(), sequenceNumber), cert.getX509Certificate().getPublicKey());
    }

    public SigningMemberImpl(Digest id, X509Certificate cert, PrivateKey certKey, Signer signer, PublicKey signerKey) {
        super(id, cert, signerKey);
        this.signer = signer;
    }

    @Override
    public SignatureAlgorithm algorithm() {
        return signer.algorithm();
    }

    @Override
    public JohnHancock sign(InputStream message) {
        return signer.sign(message);
    }
}
