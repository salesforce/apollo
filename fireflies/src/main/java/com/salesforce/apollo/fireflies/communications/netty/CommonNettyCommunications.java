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
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

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
                                .keyManager(node.getKeyManagerFactory())
                                .trustManager(node.getTrustManagerFactory())
                                .sessionCacheSize(100)
                                .sessionTimeout(10);
    }

    public static SslContextBuilder forServer(Node node) {
        return SslContextBuilder.forServer(node.getKeyManagerFactory())
                                .protocols(TL_SV1_2)
                                .ciphers(CIPHERS)
                                .trustManager(node.getTrustManagerFactory())
                                .sessionCacheSize(100)
                                .sessionTimeout(10);
    }

    public static NioEventLoopGroup newBossGroup(String label, int threads) {
        return newGroup(label + " boss", threads);
    }

    public static NioEventLoopGroup newClientGroup(String label, int threads) {
        return newGroup(label + " client", threads);
    }

    public static NioEventLoopGroup newGroup(String label, int threads) {
        final NioEventLoopGroup nioEventLoopGroup = new NioEventLoopGroup(threads, new ThreadFactory() {
            volatile int i = 0;

            @Override
            public Thread newThread(Runnable r) {
                int thread = i++;
                Thread t = new Thread(r, label + " [" + thread + "]");
                t.setDaemon(true);
                return t;
            }
        });
        nioEventLoopGroup.setIoRatio(100);
        return nioEventLoopGroup;
    }

    public static NioEventLoopGroup newWorkerGroup(String label, int threads) {
        return newGroup(label + " worker", threads);
    }

    protected final EventLoopGroup                  bossGroup;
    protected final EventLoopGroup                  clientGroup;
    protected final EventExecutorGroup              inboundExecutor;
    protected final EventExecutorGroup              outboundExecutor;
    protected final Set<CommonClientCommunications> openOutbound = Collections.newSetFromMap(new ConcurrentHashMap<>());
    protected final RPCPlugin                       stats;
    protected final EventLoopGroup                  workerGroup;
    private final AtomicBoolean                     running      = new AtomicBoolean();
    private volatile MtlsServer                     server;

    public CommonNettyCommunications(RPCPlugin stats, EventLoopGroup clientGroup, EventLoopGroup bossGroup,
            EventLoopGroup workerGroup, EventExecutorGroup inboundExecutor, EventExecutorGroup outboundExecutor) {
        this.stats = stats;
        this.clientGroup = clientGroup;
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.inboundExecutor = inboundExecutor;
        this.outboundExecutor = outboundExecutor;
    }

    public CommonNettyCommunications(String label) {
        this(label, 10, 10, 10, 10, 10);
    }

    public CommonNettyCommunications(String label, int clientThreads, int bossThreads, int workerThreads,
            int inboundExecutorThreads, int outboundExecutorThreads) {
        this(label, null, clientThreads, bossThreads, workerThreads, inboundExecutorThreads, outboundExecutorThreads);
    }

    public CommonNettyCommunications(String label, RPCPlugin stats, int clientThreads, int bossThreads,
            int workerThreads, int inboundExecutorThreads, int outboundExecutorThreads) {
        this(stats, newClientGroup(label, clientThreads), newBossGroup(label, bossThreads),
                newWorkerGroup(label, workerThreads), new DefaultEventExecutorGroup(inboundExecutorThreads),
                new DefaultEventExecutorGroup(outboundExecutorThreads));
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
                    defaultBuiilder(), bossGroup, workerGroup, inboundExecutor);
        } catch (SSLException e) {
            throw new IllegalStateException("Unable to construct SslContex", e);
        }

    }
}
