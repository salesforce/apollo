/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche.communications.netty;

import java.net.InetSocketAddress;
import java.security.cert.X509Certificate;
import java.util.function.Function;

import org.apache.avro.ipc.RPCPlugin;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.specific.SpecificResponder;

import com.salesforce.apollo.avalanche.Avalanche;
import com.salesforce.apollo.avalanche.Avalanche.Service;
import com.salesforce.apollo.avalanche.communications.avro.AvalancheClientCommunications;
import com.salesforce.apollo.avalanche.communications.avro.AvalancheCommunications;
import com.salesforce.apollo.avalanche.communications.avro.AvalancheServerCommunications;
import com.salesforce.apollo.avro.Apollo;
import com.salesforce.apollo.comm.netty4.NettyTlsTransceiver;
import com.salesforce.apollo.fireflies.Member;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.fireflies.communications.netty.CommonNettyCommunications;

import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.EventExecutorGroup;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class AvalancheNettyCommunications extends CommonNettyCommunications implements AvalancheCommunications {
    private static final boolean CLOSE_ON_REPLY = true;
    private volatile Avalanche   avalanche;

    public AvalancheNettyCommunications(RPCPlugin stats, EventLoopGroup clientGroup, EventLoopGroup bossGroup,
            EventLoopGroup workerGroup, EventExecutorGroup inboundExecutor, EventExecutorGroup outboundExecutor) {
        super(stats, clientGroup, bossGroup, workerGroup, inboundExecutor, outboundExecutor, CLOSE_ON_REPLY);
    }

    public AvalancheNettyCommunications(String label, int clientThreads, int bossThreads, int workerThreads,
            int inboundExecutorThreads, int outboundExecutorThreads) {
        super(label, clientThreads, bossThreads, workerThreads, inboundExecutorThreads, outboundExecutorThreads,
                CLOSE_ON_REPLY);
    }

    public AvalancheNettyCommunications(String label, RPCPlugin stats, int clientThreads, int bossThreads,
            int workerThreads, int inboundExecutorThreads, int outboundExecutorThreads) {
        super(label, stats, clientThreads, bossThreads, workerThreads, inboundExecutorThreads, outboundExecutorThreads,
                CLOSE_ON_REPLY);
    }

    @Override
    public AvalancheClientCommunications connectToNode(Member to, Node from) {
        try {
            AvalancheClientCommunications thisOutbound[] = new AvalancheClientCommunications[1];
            AvalancheClientCommunications outbound = new AvalancheClientCommunications(
                    new NettyTlsTransceiver(to.getAvalancheEndpoint(), forClient(), clientGroup, inboundExecutor) {

                        @Override
                        public void close() {
                            openOutbound.remove(thisOutbound[0]);
                            super.close();
                        }

                    }, to);
            thisOutbound[0] = outbound;
            openOutbound.add(outbound);
            if (stats != null) {
                outbound.add(stats);
            }
            return outbound;
        } catch (Throwable e) {
            if (log.isDebugEnabled()) {
                log.error("Error connecting to {}", to, e);
            } else {
                log.error("Error connecting to {}: {}", to, e.toString());
            }
            return null;
        }
    }

    @Override
    public void initialize(Avalanche avalanche) {
        this.avalanche = avalanche;
        initialize(avalanche.getNode());
    }

    @Override
    protected ClientAuth clientAuth() {
        return ClientAuth.NONE;
    }

    @Override
    protected InetSocketAddress endpoint() {
        return avalanche.getNode().getAvalancheEndpoint();
    }

    protected Function<X509Certificate, Responder> provider() {
        return certificate -> {
            Service service = avalanche.getService();
            SpecificResponder responder = new SpecificResponder(Apollo.class,
                    new AvalancheServerCommunications(service));
            return responder;
        };
    }

    @Override
    protected SslContext sslCtx() {
        return forServer(avalanche.getNode(), ClientAuth.NONE);
    }

}
