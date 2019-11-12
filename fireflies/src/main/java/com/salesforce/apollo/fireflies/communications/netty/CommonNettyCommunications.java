/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.communications.netty;

import static com.salesforce.apollo.comm.netty4.MtlsServer.defaultBuiilder;

import java.net.InetSocketAddress;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import javax.net.ssl.SSLException;

import org.apache.avro.ipc.RPCPlugin;
import org.apache.avro.ipc.Responder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.comm.netty4.MtlsServer;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.fireflies.communications.CommonClientCommunications;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContextBuilder;

abstract public class CommonNettyCommunications {
    public static List<String>    CIPHERS = new ArrayList<>();
    protected static final Logger log     = LoggerFactory.getLogger(CommonNettyCommunications.class);

    private static final String TL_SV1_2 = "TLSv1.2";

    static {
        CIPHERS.add("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
    }

    static {
        Security.setProperty("crypto.policy", "unlimited");
    }

    public static SslContextBuilder forClient(Node node) {
        return SslContextBuilder.forClient()
                                .protocols(TL_SV1_2)
                                .ciphers(CIPHERS)
                                // .sslContextProvider(JSSE_PROVIDER)
                                .keyManager(node.getKeyManagerFactory())
                                .trustManager(node.getTrustManagerFactory());
    }

    public static SslContextBuilder forServer(Node node) {
        return SslContextBuilder.forServer(node.getKeyManagerFactory())
                                .protocols(TL_SV1_2)
                                .ciphers(CIPHERS)
                                // .sslContextProvider(JSSE_PROVIDER)
                                .trustManager(node.getTrustManagerFactory());
    }

    protected final EventLoopGroup                  bossGroup;
    protected final EventLoopGroup                  clientGroup;
    protected final Set<CommonClientCommunications> openOutbound = Collections.newSetFromMap(new ConcurrentHashMap<>());
    protected final RPCPlugin                       stats;
    protected final EventLoopGroup                  workerGroup;
    private final AtomicBoolean                     running      = new AtomicBoolean();
    private volatile MtlsServer                     server;

    public CommonNettyCommunications(RPCPlugin stats, EventLoopGroup clientGroup, EventLoopGroup bossGroup,
            EventLoopGroup workerGroup) {
        this.stats = stats;
        this.clientGroup = clientGroup;
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
    }

    public CommonNettyCommunications(String label) {
        this(label, 10, 10, 10);
    }

    public CommonNettyCommunications(String label, int clientThreads, int bossThreads, int workerThreads) {
        this(label, null, clientThreads, bossThreads, workerThreads);
    }

    public CommonNettyCommunications(String label, RPCPlugin stats, int clientThreads, int bossThreads,
            int workerThreads) {
        this(stats, new NioEventLoopGroup(bossThreads, new ThreadFactory() {
            volatile int i = 0;

            @Override
            public Thread newThread(Runnable r) {
                int thread = i++;
                Thread t = new Thread(r, label + " client [" + thread + "]");
                t.setDaemon(true);
                return t;
            }
        }), new NioEventLoopGroup(bossThreads, new ThreadFactory() {
            volatile int i = 0;

            @Override
            public Thread newThread(Runnable r) {
                int thread = i++;
                Thread t = new Thread(r, label + " boss [" + thread + "]");
                t.setDaemon(true);
                return t;
            }
        }), new NioEventLoopGroup(workerThreads, new ThreadFactory() {
            volatile int i = 0;

            @Override
            public Thread newThread(Runnable r) {
                int thread = i++;
                Thread t = new Thread(r, label + " worker[" + thread + "]");
                t.setDaemon(true);
                return t;
            }
        }));
    }

    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        MtlsServer current = server;
        if (current != null) {
            server = null;
            current.close();
            try {
                current.join();
            } catch (InterruptedException e) {
                log.info("Interrupted closing server");
            }
        }
        try {
            clientGroup.shutdownGracefully().get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failure shutting down boss group", e);
        }
        try {
            bossGroup.shutdownGracefully().get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failure shutting down boss group", e);
        }
        try {
            workerGroup.shutdownGracefully().get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failure shutting down worker group", e);
        }
        openOutbound.forEach(e -> e.close());
        openOutbound.clear();
    }

    public void logDiag() {
        log.trace(" outbound: {} inbound: {}", openOutbound.size(),
                  server == null ? 0 : server.getNumActiveConnections());
    }

    public void start() {
        MtlsServer current = server;
        if (current == null) {
            server = newServer();
            server.setStats(stats);
        }
    }

    protected abstract ClientAuth clientAuth();

    protected abstract InetSocketAddress endpoint();

    protected abstract Function<X509Certificate, Responder> provider();

    protected abstract SslContextBuilder sslCtxBuilder();

    private MtlsServer newServer() {

        try {
            return new MtlsServer(endpoint(), sslCtxBuilder().clientAuth(clientAuth()).build(), provider(),
                    defaultBuiilder(), bossGroup, workerGroup);
        } catch (SSLException e) {
            throw new IllegalStateException("Unable to construct SslContex", e);
        }

    }
}
