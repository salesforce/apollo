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
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

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
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

abstract public class CommonNettyCommunications {
    protected static final Logger log = LoggerFactory.getLogger(CommonNettyCommunications.class);

    static {
        Security.setProperty("crypto.policy", "unlimited");
    }

    @SuppressWarnings("deprecation")
    public static SslContext forServer(Node node, ClientAuth clientAuth) {
        return new JdkSslContext(node.getSslContext(), false, clientAuth);
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

    protected final EventLoopGroup bossGroup;

    protected final EventLoopGroup                  clientGroup;
    protected final EventExecutorGroup              inboundExecutor;
    protected final Set<CommonClientCommunications> openOutbound = Collections.newSetFromMap(new ConcurrentHashMap<>());
    protected final EventExecutorGroup              outboundExecutor;
    protected final RPCPlugin                       stats;
    protected final EventLoopGroup                  workerGroup;
    private volatile SslContext                     clientSslContext;
    private final boolean                           closeOnReply;
    private final AtomicBoolean                     running      = new AtomicBoolean();
    private volatile MtlsServer                     server;

    public CommonNettyCommunications(RPCPlugin stats, EventLoopGroup clientGroup, EventLoopGroup bossGroup,
            EventLoopGroup workerGroup, EventExecutorGroup inboundExecutor, EventExecutorGroup outboundExecutor,
            boolean closeOnReply) {
        this.stats = stats;
        this.clientGroup = clientGroup;
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.inboundExecutor = inboundExecutor;
        this.outboundExecutor = outboundExecutor;
        this.closeOnReply = closeOnReply;
    }

    public CommonNettyCommunications(String label) {
        this(label, 10, 10, 10, 10, 10, false);
    }

    public CommonNettyCommunications(String label, int clientThreads, int bossThreads, int workerThreads,
            int inboundExecutorThreads, int outboundExecutorThreads, boolean closeOnReply) {
        this(label, null, clientThreads, bossThreads, workerThreads, inboundExecutorThreads, outboundExecutorThreads,
                closeOnReply);
    }

    public CommonNettyCommunications(String label, RPCPlugin stats, int clientThreads, int bossThreads,
            int workerThreads, int inboundExecutorThreads, int outboundExecutorThreads, boolean closeOnReply) {
        this(stats, newClientGroup(label, clientThreads), newBossGroup(label, bossThreads),
                newWorkerGroup(label, workerThreads), new DefaultEventExecutorGroup(inboundExecutorThreads),
                new DefaultEventExecutorGroup(outboundExecutorThreads), closeOnReply);
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

    public SslContext forClient() {
        final SslContext current = clientSslContext;
        return current;
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

    @SuppressWarnings("deprecation")
    protected void initialize(Node node) {
        clientSslContext = new JdkSslContext(node.getSslContext(), true, ClientAuth.OPTIONAL);
    }

    protected abstract Function<X509Certificate, Responder> provider();

    protected abstract SslContext sslCtx();

    private MtlsServer newServer() {
        return new MtlsServer(closeOnReply, endpoint(), sslCtx(), provider(), defaultBuiilder(), bossGroup, workerGroup,
                inboundExecutor);

    }
}
