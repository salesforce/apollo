/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm.grpc;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.security.InvalidAlgorithmParameterException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.Provider;
import java.security.Security;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.KeyManagerFactorySpi;
import javax.net.ssl.ManagerFactoryParameters;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.TrustManagerFactorySpi;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.salesforce.apollo.protocols.CertificateValidator;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.Utils;

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

    public static class NodeKeyManagerFactory extends KeyManagerFactory {

        public NodeKeyManagerFactory(String alias, X509Certificate certificate, PrivateKey privateKey) {
            super(new NodeKeyManagerFactorySpi(alias, certificate, privateKey), PROVIDER, "Keys");
        }

    }

    public static class NodeKeyManagerFactorySpi extends KeyManagerFactorySpi {

        private final String          alias;
        private final X509Certificate certificate;
        private final PrivateKey      privateKey;

        public NodeKeyManagerFactorySpi(String alias, X509Certificate certificate, PrivateKey privateKey) {
            this.alias = alias;
            this.certificate = certificate;
            this.privateKey = privateKey;
        }

        @Override
        protected KeyManager[] engineGetKeyManagers() {
            return new KeyManager[] { new Keys(alias, certificate, privateKey) };
        }

        @Override
        protected void engineInit(KeyStore ks, char[] password) throws KeyStoreException, NoSuchAlgorithmException,
                                                                UnrecoverableKeyException {
        }

        @Override
        protected void engineInit(ManagerFactoryParameters spec) throws InvalidAlgorithmParameterException {
        }

    }

    public static class NodeTrustManagerFactory extends TrustManagerFactory {

        public NodeTrustManagerFactory(CertificateValidator validator) {
            super(new NodeTrustManagerFactorySpi(validator), PROVIDER, "Trust");
        }

    }

    public static class NodeTrustManagerFactorySpi extends TrustManagerFactorySpi {

        private final CertificateValidator validator;

        public NodeTrustManagerFactorySpi(CertificateValidator validator) {
            this.validator = validator;
        }

        @Override
        protected TrustManager[] engineGetTrustManagers() {
            return new TrustManager[] { new Trust(validator) };
        }

        @Override
        protected void engineInit(KeyStore ks) throws KeyStoreException {
        }

        @Override
        protected void engineInit(ManagerFactoryParameters spec) throws InvalidAlgorithmParameterException {
        }

    }

    private static class Keys extends X509ExtendedKeyManager {
        private final String          alias;
        private final X509Certificate certificate;
        private final PrivateKey      privateKey;

        public Keys(String alias, X509Certificate certificate, PrivateKey privateKey) {
            this.alias = alias;
            this.certificate = certificate;
            this.privateKey = privateKey;
        }

        @Override
        public String chooseClientAlias(String[] keyType, Principal[] principals, Socket socket) {
            return alias;
        }

        @Override
        public String chooseEngineClientAlias(String[] keyType, Principal[] issuers, SSLEngine engine) {
            return alias;
        }

        @Override
        public String chooseEngineServerAlias(String keyType, Principal[] issuers, SSLEngine engine) {
            return alias;
        }

        @Override
        public String chooseServerAlias(String s, Principal[] principals, Socket socket) {
            return alias;
        }

        @Override
        public X509Certificate[] getCertificateChain(String s) {
            return new X509Certificate[] { certificate };
        }

        @Override
        public String[] getClientAliases(String keyType, Principal[] principals) {
            return new String[] { alias };
        }

        @Override
        public PrivateKey getPrivateKey(String alias) {
            if (this.alias.equals(alias)) {
                return privateKey;
            }
            return null;
        }

        @Override
        public String[] getServerAliases(String s, Principal[] principals) {
            return new String[] { alias };
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

    private static class Trust extends X509ExtendedTrustManager {
        private final CertificateValidator validator;

        public Trust(CertificateValidator validator) {
            this.validator = validator;
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            validator.validateClient(chain);
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType,
                                       Socket socket) throws CertificateException {
            validator.validateClient(chain);
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType,
                                       SSLEngine engine) throws CertificateException {
            validator.validateClient(chain);
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            validator.validateServer(chain);
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType,
                                       Socket socket) throws CertificateException {
            validator.validateServer(chain);
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType,
                                       SSLEngine arg2) throws CertificateException {
            validator.validateServer(chain);
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }

    }

    private static final List<String> CIPHERS  = new ArrayList<>();
    private static final Provider     PROVIDER = new BouncyCastleProvider();
    private static final String       TL_SV1_2 = "TLSv1.2";

    static {
        CIPHERS.add("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
        Security.addProvider(PROVIDER);
    }

    public static SslContext forClient(ClientAuth clientAuth, String alias, X509Certificate certificate,
                                       PrivateKey privateKey, CertificateValidator validator) {
        SslContextBuilder builder = SslContextBuilder.forClient()
                                                     .keyManager(new NodeKeyManagerFactory(alias, certificate,
                                                             privateKey));
        GrpcSslContexts.configure(builder);
        builder.protocols(TL_SV1_2)
               .ciphers(CIPHERS)
               .trustManager(new NodeTrustManagerFactory(validator))
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
                privateKey));
        GrpcSslContexts.configure(builder);
        builder.protocols(TL_SV1_2)
               .ciphers(CIPHERS)
               .trustManager(new NodeTrustManagerFactory(validator))
               .clientAuth(clientAuth);
        try {
            return builder.build();
        } catch (SSLException e) {
            throw new IllegalStateException("Cannot build ssl client context", e);
        }

    }

    private final LoadingCache<X509Certificate, HashKey> cachedMembership = CacheBuilder.newBuilder()
                                                                                        .build(new CacheLoader<X509Certificate, HashKey>() {

                                                                                                                                                                  @Override
                                                                                                                                                                  public HashKey load(X509Certificate key) throws Exception {
                                                                                                                                                                      return Utils.getMemberId(key);
                                                                                                                                                                  }
                                                                                                                                                              });
    private final TlsInterceptor                         interceptor      = new TlsInterceptor();
    private final MutableHandlerRegistry                 registry;
    private final Server                                 server;

    private final Context.Key<SSLSession> sslSessionContext = Context.key("SSLSession");

    public MtlsServer(SocketAddress address, ClientAuth clientAuth, String alias, X509Certificate certificate,
            PrivateKey privateKey, CertificateValidator validator, MutableHandlerRegistry registry) {
        this.registry = registry;

        NettyServerBuilder builder = NettyServerBuilder.forAddress(address)
                                                       .sslContext(forServer(clientAuth, alias, certificate, privateKey,
                                                                             validator))
                                                       .fallbackHandlerRegistry(registry)
                                                       .withChildOption(ChannelOption.TCP_NODELAY, true)
                                                       .intercept(interceptor)
                                                       .intercept(EnableCompressionInterceptor.SINGLETON);
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
