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
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.util.MutableHandlerRegistry;
import io.netty.channel.ChannelOption;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolConfig.Protocol;
import io.netty.handler.ssl.ApplicationProtocolConfig.SelectedListenerFailureBehavior;
import io.netty.handler.ssl.ApplicationProtocolConfig.SelectorFailureBehavior;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

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

    public static final String TL_SV1_3 = "TLSv1.3";

    private static final Provider PROVIDER_JSSE = Security.getProvider("SunJSSE");

    public static SslContext forClient(ClientAuth clientAuth, String alias, X509Certificate certificate,
                                       PrivateKey privateKey, CertificateValidator validator) {
        SslContextBuilder builder = SslContextBuilder.forClient()
                                                     .sslContextProvider(PROVIDER_JSSE)
                                                     .keyManager(new NodeKeyManagerFactory(alias, certificate,
                                                                                           privateKey, PROVIDER_JSSE));
        GrpcSslContexts.configure(builder, SslProvider.JDK);
        builder.protocols(TL_SV1_3)
               .sslContextProvider(PROVIDER_JSSE)
               .trustManager(new NodeTrustManagerFactory(validator, PROVIDER_JSSE))
               .clientAuth(clientAuth)
               .applicationProtocolConfig(new ApplicationProtocolConfig(Protocol.ALPN,
                                                                        // NO_ADVERTISE is currently the only mode
                                                                        // supported by both OpenSsl and JDK
                                                                        // providers.
                                                                        SelectorFailureBehavior.NO_ADVERTISE,
                                                                        // ACCEPT is currently the only mode supported
                                                                        // by both OpenSsl and JDK
                                                                        // providers.
                                                                        SelectedListenerFailureBehavior.ACCEPT,
                                                                        ApplicationProtocolNames.HTTP_2,
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
                                                                                          privateKey, PROVIDER_JSSE));
        GrpcSslContexts.configure(builder, SslProvider.JDK);
        builder.protocols(TL_SV1_3)
               .sslContextProvider(PROVIDER_JSSE)
               .trustManager(new NodeTrustManagerFactory(validator, PROVIDER_JSSE))
               .clientAuth(clientAuth)
               .applicationProtocolConfig(new ApplicationProtocolConfig(Protocol.ALPN,
                                                                        // NO_ADVERTISE is currently the only mode
                                                                        // supported by both OpenSsl and JDK
                                                                        // providers.
                                                                        SelectorFailureBehavior.NO_ADVERTISE,
                                                                        // ACCEPT is currently the only mode supported
                                                                        // by both OpenSsl and JDK
                                                                        // providers.
                                                                        SelectedListenerFailureBehavior.ACCEPT,
                                                                        ApplicationProtocolNames.HTTP_2,
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
                                                       .executor(executor)
                                                       .withOption(ChannelOption.SO_REUSEADDR, true)
                                                       .sslContext(supplier.forServer(clientAuth, alias, validator,
                                                                                      PROVIDER_JSSE, TL_SV1_3))
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
        server.shutdownNow();
    }

    private X509Certificate getCert() {
        try {
            return (X509Certificate) sslSessionContext.get().getPeerCertificates()[0];
        } catch (SSLPeerUnverifiedException e) {
            throw new IllegalStateException(e);
        }
    }
}
