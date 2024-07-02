/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipelago;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.grpc.server.GrpcServerLimiterBuilder;
import com.salesforce.apollo.archipelago.server.FernetServerInterceptor;
import com.salesforce.apollo.comm.grpc.ClientContextSupplier;
import com.salesforce.apollo.comm.grpc.ServerContextSupplier;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.ssl.CertificateValidator;
import com.salesforce.apollo.cryptography.ssl.NodeKeyManagerFactory;
import com.salesforce.apollo.cryptography.ssl.NodeTrustManagerFactory;
import com.salesforce.apollo.cryptography.ssl.TlsInterceptor;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.protocols.LimitsRegistry;
import io.grpc.*;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.util.MutableHandlerRegistry;
import io.netty.channel.ChannelOption;
import io.netty.handler.ssl.*;
import io.netty.handler.ssl.ApplicationProtocolConfig.Protocol;
import io.netty.handler.ssl.ApplicationProtocolConfig.SelectedListenerFailureBehavior;
import io.netty.handler.ssl.ApplicationProtocolConfig.SelectorFailureBehavior;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * @author hal.hildebrand
 */
public class MtlsServer implements RouterSupplier {
    static final         String   TL_SV1_3      = "TLSv1.3";
    private static final Provider PROVIDER_JSSE = Security.getProvider("SunJSSE");
    private static final Logger   log           = LoggerFactory.getLogger(MtlsServer.class);

    private final LoadingCache<X509Certificate, Digest>   cachedMembership;
    private final Function<Member, ClientContextSupplier> contextSupplier;
    private final EndpointProvider                        epProvider;
    private final Member                                  from;
    private final Context.Key<SSLSession>                 sslSessionContext = Context.key("SSLSession");
    private final ServerContextSupplier                   supplier;

    public MtlsServer(Member from, EndpointProvider epProvider, Function<Member, ClientContextSupplier> contextSupplier,
                      ServerContextSupplier supplier) {
        this.from = from;
        this.epProvider = epProvider;
        this.contextSupplier = contextSupplier;
        this.supplier = supplier;
        cachedMembership = CacheBuilder.newBuilder().build(new CacheLoader<X509Certificate, Digest>() {
            @Override
            public Digest load(X509Certificate key) throws Exception {
                return supplier.getMemberId(key);
            }
        });
    }

    public static SslContext forClient(ClientAuth clientAuth, String alias, X509Certificate certificate,
                                       PrivateKey privateKey, CertificateValidator validator) {
        SslContextBuilder builder = SslContextBuilder.forClient()
                                                     .sslContextProvider(PROVIDER_JSSE)
                                                     .keyManager(
                                                     new NodeKeyManagerFactory(alias, certificate, privateKey,
                                                                               PROVIDER_JSSE));
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
        SslContextBuilder builder = SslContextBuilder.forServer(
        new NodeKeyManagerFactory(alias, certificate, privateKey, PROVIDER_JSSE));
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

    @Override
    public RouterImpl router(ServerConnectionCache.Builder cacheBuilder, Supplier<Limit> serverLimit,
                             LimitsRegistry limitsRegistry, List<ServerInterceptor> interceptors,
                             Predicate<FernetServerInterceptor.HashedToken> validator, ExecutorService executor) {
        //        if (executor == null) {
        //            executor = Executors.newVirtualThreadPerTaskExecutor();
        //        }
        var limitsBuilder = new GrpcServerLimiterBuilder().limit(serverLimit.get());
        if (limitsRegistry != null) {
            limitsBuilder.metricRegistry(limitsRegistry);
        }
        NettyServerBuilder serverBuilder = NettyServerBuilder.forAddress(epProvider.getBindAddress())
                                                             //                                                             .executor(executor)
                                                             .withOption(ChannelOption.SO_REUSEADDR, true)
                                                             .sslContext(supplier.forServer(ClientAuth.REQUIRE,
                                                                                            epProvider.getAlias(),
                                                                                            epProvider.getValidator(),
                                                                                            PROVIDER_JSSE))
                                                             .fallbackHandlerRegistry(new MutableHandlerRegistry())
                                                             .withChildOption(ChannelOption.TCP_NODELAY, true)
                                                             .intercept(new TlsInterceptor(sslSessionContext))
                                                             .intercept(EnableCompressionInterceptor.SINGLETON);
        interceptors.forEach(i -> {
            serverBuilder.intercept(i);
        });
        ClientIdentity identity = new ClientIdentity() {

            @Override
            public Digest getFrom() {
                try {
                    var cert = getCert();
                    return cert == null ? null : cachedMembership.get(cert);
                } catch (ExecutionException e) {
                    throw new IllegalStateException("Unable to derive member id from cert", e.getCause());
                }
            }
        };
        return new RouterImpl(from, serverBuilder, cacheBuilder.setFactory(t -> connectTo(t)), identity, c -> {
        }, validator);
    }

    private ManagedChannel connectTo(Member to) {
        var address = epProvider.addressFor(to);
        log.debug("Connecting to: {} address: {} on: {}", to.getId(), address, from.getId());
        return new MtlsClient(address, epProvider.getClientAuth(), epProvider.getAlias(), contextSupplier.apply(from),
                              epProvider.getValidator()).getChannel();
    }

    private X509Certificate getCert() {
        try {
            var sslSession = sslSessionContext.get();
            if (sslSession == null) {
                return null;
            }
            return (X509Certificate) sslSession.getPeerCertificates()[0];
        } catch (SSLPeerUnverifiedException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Currently grpc-java doesn't return compressed responses, even if the client has sent a compressed payload. This
     * turns on gzip compression for all responses.
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
}
