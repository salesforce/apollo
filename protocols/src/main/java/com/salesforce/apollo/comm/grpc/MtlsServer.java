/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm.grpc;

import java.io.IOException;
import java.net.SocketAddress;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.salesforce.apollo.crypto.ssl.CertificateValidator;
import com.salesforce.apollo.crypto.ssl.NodeKeyManagerFactory;
import com.salesforce.apollo.crypto.ssl.NodeTrustManagerFactory;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;

import io.grpc.BindableService;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.util.MutableHandlerRegistry;

/**
 * @author hal.hildebrand
 *
 */
public class MtlsServer implements ClientIdentity {
    /**
     * Currently grpc-java doesn't return compressed responses, even if the client
     * has sent a compressed payload. This turns on gzip compression for all
     * responses.
     */
    public static class EnableCompressionInterceptor implements ServerInterceptor {
        public final static EnableCompressionInterceptor SINGLETON = new EnableCompressionInterceptor();

        @Override
        public <ReqT, RespT> io.grpc.ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                                                                             Metadata headers,
                                                                             ServerCallHandler<ReqT, RespT> next) {
            call.setCompression("gzip");
            return next.startCall(call, headers);
        }
    }

    private class TlsInterceptor implements ServerInterceptor {
        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
                                                                     ServerCallHandler<ReqT, RespT> next) {
            SSLSession sslSession = call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
            if (sslSession == null) {
                return next.startCall(call, headers);
            }
            return Contexts.interceptCall(Context.current().withValue(sslSessionContext, sslSession), call, headers,
                                          next);
        }
    }

    private static final List<String> CIPHERS  = new ArrayList<>();
    private static final Provider     PROVIDER = new BouncyCastleProvider();
    private static final String       TL_SV1_3 = "TLSv1.3";

    static {
        CIPHERS.add("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
        Security.addProvider(PROVIDER);
    }

    public static SslContext forClient(ClientAuth clientAuth, String alias, X509Certificate certificate,
                                       PrivateKey privateKey, CertificateValidator validator) {
        SslContextBuilder builder = SslContextBuilder.forClient()
                                                     .keyManager(new NodeKeyManagerFactory(alias, certificate,
                                                             privateKey, PROVIDER));
        GrpcSslContexts.configure(builder);
        builder.protocols(TL_SV1_3)
               .ciphers(CIPHERS)
               .trustManager(new NodeTrustManagerFactory(validator, PROVIDER))
               .clientAuth(clientAuth);
        try {
            return builder.build();
        } catch (SSLException e) {
            throw new IllegalStateException("Cannot build ssl client context", e);
        }

    }

    public static SslContext forServer(ClientAuth clientAuth, String alias, X509Certificate certificate,
                                       PrivateKey privateKey, CertificateValidator validator) {
        SslContextBuilder builder = SslContextBuilder.forServer(new NodeKeyManagerFactory(alias, certificate,
                privateKey, PROVIDER));
        GrpcSslContexts.configure(builder);
        builder.protocols(TL_SV1_3)
               .ciphers(CIPHERS)
               .trustManager(new NodeTrustManagerFactory(validator, PROVIDER))
               .clientAuth(clientAuth);
        try {
            return builder.build();
        } catch (SSLException e) {
            throw new IllegalStateException("Cannot build ssl client context", e);
        }

    }

    private final LoadingCache<X509Certificate, HashKey> cachedMembership;
    private final TlsInterceptor                         interceptor = new TlsInterceptor();
    private final MutableHandlerRegistry                 registry;
    private final Server                                 server;

    private final Context.Key<SSLSession> sslSessionContext = Context.key("SSLSession");

    public MtlsServer(SocketAddress address, ClientAuth clientAuth, String alias, X509Certificate certificate,
            PrivateKey privateKey, CertificateValidator validator, MutableHandlerRegistry registry, Executor executor) {
        this.registry = registry;
        cachedMembership = CacheBuilder.newBuilder().build(new CacheLoader<X509Certificate, HashKey>() {
            @Override
            public HashKey load(X509Certificate key) throws Exception {
                return Conversion.getMemberId(key);
            }
        });
        NettyServerBuilder builder = NettyServerBuilder.forAddress(address)
                                                       .sslContext(forServer(clientAuth, alias, certificate, privateKey,
                                                                             validator))
                                                       .fallbackHandlerRegistry(registry)
                                                       .withChildOption(ChannelOption.TCP_NODELAY, true)
                                                       .intercept(interceptor)
                                                       .intercept(EnableCompressionInterceptor.SINGLETON);
        builder.executor(executor);
        server = builder.build();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                server.shutdown();
            }
        });
    }

    public void bind(BindableService service) {
        registry.addService(service);
    }

    @Override
    public X509Certificate getCert() {
        return (X509Certificate) getCerts()[0];
    }

    @Override
    public Certificate[] getCerts() {
        try {
            return sslSessionContext.get().getPeerCertificates();
        } catch (SSLPeerUnverifiedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public HashKey getFrom() {
        try {
            return cachedMembership.get(getCert());
        } catch (ExecutionException e) {
            throw new IllegalStateException("Unable to derive member id from cert", e.getCause());
        }
    }

    public void start() throws IOException {
        server.start();
    }

    public void stop() {
        server.shutdown();
    }
}
