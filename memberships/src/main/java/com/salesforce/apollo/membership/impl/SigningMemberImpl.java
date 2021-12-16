/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.impl;

import java.io.InputStream;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.PublicKey;
import java.security.cert.X509Certificate;

import com.salesforce.apollo.comm.grpc.MtlsServer;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.crypto.ssl.CertificateValidator;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;

import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;

/**
 * A signiner member of a view. This is a local member to the process that can
 * sign and assert things.
 * 
 * @author hal.hildebrand
 *
 */
public class SigningMemberImpl extends MemberImpl implements SigningMember {

    private PrivateKey certKey;
    private Signer     signer;

    /**
     * @param member
     */
    public SigningMemberImpl(CertificateWithPrivateKey cert) {
        this(Member.getMemberIdentifier(cert.getX509Certificate()), cert.getX509Certificate(), cert.getPrivateKey(),
             new SignerImpl(cert.getPrivateKey()), cert.getX509Certificate().getPublicKey());
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
    public SslContext forClient(ClientAuth clientAuth, String alias, CertificateValidator validator, Provider provider,
                                String tlsVersion) {
        return MtlsServer.forClient(clientAuth, alias, certificate, certKey, validator);
    }

    @Override
    public SslContext forServer(ClientAuth clientAuth, String alias, CertificateValidator validator, Provider provider,
                                String tlsVersion) {
        return MtlsServer.forServer(clientAuth, alias, certificate, certKey, validator);
    }

    @Override
    public JohnHancock sign(InputStream message) {
        return signer.sign(message);
    }
}
