/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipeligo;

import java.security.Provider;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.netflix.concurrency.limits.Limit;
import com.netflix.concurrency.limits.grpc.server.GrpcServerLimiterBuilder;
import com.salesforce.apollo.comm.grpc.ClientContextSupplier;
import com.salesforce.apollo.comm.grpc.MtlsServer.EnableCompressionInterceptor;
import com.salesforce.apollo.comm.grpc.ServerContextSupplier;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.ssl.TlsInterceptor;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.protocols.LimitsRegistry;

import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.util.MutableHandlerRegistry;
import io.netty.channel.ChannelOption;
import io.netty.handler.ssl.ClientAuth;

/**
 * @author hal.hildebrand
 *
 */
public class MtlsServer implements RouterSupplier {
    private static final Provider PROVIDER_JSSE = Security.getProvider("SunJSSE");
    private static final String   TL_SV1_3      = "TLSv1.3";

    private final LoadingCache<X509Certificate, Digest>   cachedMembership;
    private final Function<Member, ClientContextSupplier> contextSupplier;
    private final EndpointProvider                        epProvider;
    private final Executor                                exec;
    private final Member                                  from;
    private final Context.Key<SSLSession>                 sslSessionContext = Context.key("SSLSession");
    private final ServerContextSupplier                   supplier;

    public MtlsServer(Member from, EndpointProvider epProvider, Function<Member, ClientContextSupplier> contextSupplier,
                      ServerContextSupplier supplier, Executor exec) {
        this.from = from;
        this.epProvider = epProvider;
        this.contextSupplier = contextSupplier;
        this.exec = exec;
        this.supplier = supplier;
        cachedMembership = CacheBuilder.newBuilder().build(new CacheLoader<X509Certificate, Digest>() {
            @Override
            public Digest load(X509Certificate key) throws Exception {
                return supplier.getMemberId(key);
            }
        });
    }

    @Override
    public Router router(ServerConnectionCache.Builder cacheBuilder, Supplier<Limit> serverLimit, Executor executor,
                         LimitsRegistry limitsRegistry) {
        var limitsBuilder = new GrpcServerLimiterBuilder().limit(serverLimit.get());
        if (limitsRegistry != null) {
            limitsBuilder.metricRegistry(limitsRegistry);
        }
        NettyServerBuilder serverBuilder = NettyServerBuilder.forAddress(epProvider.getBindAddress())
                                                             .executor(executor)
                                                             .withOption(ChannelOption.SO_REUSEADDR, true)
                                                             .sslContext(supplier.forServer(ClientAuth.REQUIRE,
                                                                                            epProvider.getAlias(),
                                                                                            epProvider.getValiator(),
                                                                                            PROVIDER_JSSE, TL_SV1_3))
                                                             .fallbackHandlerRegistry(new MutableHandlerRegistry())
                                                             .withChildOption(ChannelOption.TCP_NODELAY, true)
                                                             .intercept(new TlsInterceptor(sslSessionContext))
                                                             .intercept(EnableCompressionInterceptor.SINGLETON);
        ClientIdentity identity = new ClientIdentity() {

            @Override
            public Digest getFrom() {
                try {
                    return cachedMembership.get(getCert());
                } catch (ExecutionException e) {
                    throw new IllegalStateException("Unable to derive member id from cert", e.getCause());
                }
            }
        };
        return new Router(serverBuilder, cacheBuilder.setFactory(t -> connectTo(t)), identity);
    }

    private ManagedChannel connectTo(Member to) {
        return new MtlsClient(epProvider.addressFor(to), epProvider.getClientAuth(), epProvider.getAlias(),
                              contextSupplier.apply(from), epProvider.getValiator(), exec).getChannel();
    }

    private X509Certificate getCert() {
        try {
            return (X509Certificate) sslSessionContext.get().getPeerCertificates()[0];
        } catch (SSLPeerUnverifiedException e) {
            throw new IllegalStateException(e);
        }
    }
}
