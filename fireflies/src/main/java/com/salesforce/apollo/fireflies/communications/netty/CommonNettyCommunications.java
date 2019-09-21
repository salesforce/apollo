/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.apollo.fireflies.communications.netty;

import java.security.Security;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.avro.ipc.RPCPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.comm.netty4.MtlsServer;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.fireflies.communications.CommonClientCommunications;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.ssl.SslContextBuilder;

abstract public class CommonNettyCommunications {
    public static List<String> CIPHERS = new ArrayList<>();
    protected static final Logger log = LoggerFactory.getLogger(FirefliesNettyCommunications.class);

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

    protected final EventLoopGroup eventGroup = new NioEventLoopGroup(2);
    protected final Set<CommonClientCommunications> openOutbound = Collections.newSetFromMap(new ConcurrentHashMap<>());
    protected final RPCPlugin stats;
    private final AtomicBoolean running = new AtomicBoolean();

    private volatile MtlsServer server;

    public CommonNettyCommunications() {
        this(null);
    }

    public CommonNettyCommunications(RPCPlugin stats) {
        this.stats = stats;
    }

    public void close() {
        if (!running.compareAndSet(true, false)) { return; }
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

    abstract protected MtlsServer newServer();

}
