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
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.ProviderUtils;
import com.salesforce.apollo.crypto.ssl.CertificateValidator;
import com.salesforce.apollo.crypto.ssl.NodeKeyManagerFactory;
import com.salesforce.apollo.crypto.ssl.NodeTrustManagerFactory;
import com.salesforce.apollo.crypto.ssl.TlsInterceptor;
import com.salesforce.apollo.protocols.ClientIdentity;

import io.grpc.BindableService;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolConfig;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolConfig.Protocol;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolConfig.SelectedListenerFailureBehavior;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolConfig.SelectorFailureBehavior;
import io.grpc.netty.shaded.io.netty.handler.ssl.ApplicationProtocolNames;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider;
import io.grpc.util.MutableHandlerRegistry;

/**
 * @author hal.hildebrand
 *
 */
public class MtlsServer implements ClientIdentity {
    private static final Provider PROVIDER_BCJSSE = ProviderUtils.getProviderBCJSSE();

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

    public static final String TL_SV1_3 = "TLSv1.3";

    public static SslContext forClient(ClientAuth clientAuth, String alias, X509Certificate certificate,
                                       PrivateKey privateKey, CertificateValidator validator) {
        SslContextBuilder builder = SslContextBuilder.forClient()
                                                     .keyManager(new NodeKeyManagerFactory(alias, certificate,
                                                             privateKey, PROVIDER_BCJSSE));
        GrpcSslContexts.configure(builder);
        builder.protocols(TL_SV1_3)
               .sslProvider(SslProvider.JDK)
               .trustManager(new NodeTrustManagerFactory(validator, PROVIDER_BCJSSE))
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

    public static SslContext forServer(ClientAuth clientAuth, String alias, X509Certificate certificate,
                                       PrivateKey privateKey, CertificateValidator validator) {
        SslContextBuilder builder = SslContextBuilder.forServer(new NodeKeyManagerFactory(alias, certificate,
                privateKey, PROVIDER_BCJSSE));
        GrpcSslContexts.configure(builder);
        builder.protocols(TL_SV1_3)
               .sslProvider(SslProvider.JDK)
               .trustManager(new NodeTrustManagerFactory(validator, PROVIDER_BCJSSE))
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

    private final LoadingCache<X509Certificate, Digest> cachedMembership;
    private final TlsInterceptor                        interceptor;
    private final MutableHandlerRegistry                registry;
    private final Server                                server;
    private final Context.Key<SSLSession>               sslSessionContext = Context.key("SSLSession");

    public MtlsServer(SocketAddress address, ClientAuth clientAuth, String alias, ServerContextSupplier supplier,
            CertificateValidator validator, MutableHandlerRegistry registry, Executor executor) {
        this.registry = registry;
        interceptor = new TlsInterceptor(sslSessionContext);
        cachedMembership = CacheBuilder.newBuilder().build(new CacheLoader<X509Certificate, Digest>() {
            @Override
            public Digest load(X509Certificate key) throws Exception {
                return supplier.getMemberId(key);
            }
        });
        NettyServerBuilder builder = NettyServerBuilder.forAddress(address)
                                                       .sslContext(supplier.forServer(clientAuth, alias, validator,
                                                                                      PROVIDER_BCJSSE, TL_SV1_3))
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
    public Digest getFrom() {
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
        try {
            server.awaitTermination();
        } catch (InterruptedException e) {
            throw new IllegalStateException("Unknown server state as we've been interrupted in the process of shutdown",
                    e);
        }
    }
}
