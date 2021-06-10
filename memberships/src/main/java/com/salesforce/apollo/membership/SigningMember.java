/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership;

import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;

import java.security.PrivateKey;
import java.security.Provider;
import java.security.PublicKey;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLException;

import com.salesforce.apollo.comm.grpc.ClientContextSupplier;
import com.salesforce.apollo.comm.grpc.ServerContextSupplier;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.ssl.CertificateValidator;
import com.salesforce.apollo.crypto.ssl.NodeKeyManagerFactory;
import com.salesforce.apollo.crypto.ssl.NodeTrustManagerFactory;

import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolConfig;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolConfig.Protocol;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolConfig.SelectedListenerFailureBehavior;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolConfig.SelectorFailureBehavior;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolNames;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;

/**
 * A signiner member of a view. This is a local member to the process that can
 * sign and assert things.
 * 
 * @author hal.hildebrand
 *
 */
public class SigningMember extends Member implements ServerContextSupplier, ClientContextSupplier {

    private PrivateKey certKey;
    private Signer     signer;

    public SigningMember(Digest id, X509Certificate cert, PrivateKey certKey, Signer signer, PublicKey signerKey) {
        super(id, cert, signerKey);
        this.signer = signer;
    }

    @Override
    public SslContext forClient(ClientAuth clientAuth, String alias, CertificateValidator validator, Provider provider,
                                String tlsVersion) {
        SslContextBuilder builder = SslContextBuilder.forClient()
                                                     .keyManager(new NodeKeyManagerFactory(alias, certificate, certKey,
                                                             provider));
        GrpcSslContexts.configure(builder);
        builder.protocols(tlsVersion)
               .sslProvider(SslProvider.JDK)
               .trustManager(new NodeTrustManagerFactory(validator, provider))
               .clientAuth(clientAuth)
               .applicationProtocolConfig(new ApplicationProtocolConfig(Protocol.ALPN,
                       // NO_ADVERTISE is currently the only mode supported by both OpenSsl and JDK
                       // providers.
                       SelectorFailureBehavior.NO_ADVERTISE,
                       // ACCEPT is currently the only mode supported by both OpenSsl and JDK
                       // providers.
                       SelectedListenerFailureBehavior.ACCEPT, ApplicationProtocolNames.HTTP_2,
                       ApplicationProtocolNames.HTTP_1_1));
        try {
            return builder.build();
        } catch (SSLException e) {
            throw new IllegalStateException("Cannot build ssl client context", e);
        }

    }

    @Override
    public SslContext forServer(ClientAuth clientAuth, String alias, CertificateValidator validator, Provider provider,
                                String tlsVersion) {
        SslContextBuilder builder = SslContextBuilder.forServer(new NodeKeyManagerFactory(alias, certificate, certKey,
                provider));
        GrpcSslContexts.configure(builder);
        builder.protocols(tlsVersion)
               .sslProvider(SslProvider.JDK)
               .trustManager(new NodeTrustManagerFactory(validator, provider))
               .clientAuth(clientAuth)
               .applicationProtocolConfig(new ApplicationProtocolConfig(Protocol.ALPN,
                       // NO_ADVERTISE is currently the only mode supported by both OpenSsl and JDK
                       // providers.
                       SelectorFailureBehavior.NO_ADVERTISE,
                       // ACCEPT is currently the only mode supported by both OpenSsl and JDK
                       // providers.
                       SelectedListenerFailureBehavior.ACCEPT, ApplicationProtocolNames.HTTP_2,
                       ApplicationProtocolNames.HTTP_1_1));
        try {
            return builder.build();
        } catch (SSLException e) {
            throw new IllegalStateException("Cannot build ssl client context", e);
        }

    }

    @Override
    public Digest getMemberId(X509Certificate key) {
        return getMemberIdentifier(key);
    }

    public String sign(byte[] message) {
        return qb64(signer.sign(message));
    }
}
