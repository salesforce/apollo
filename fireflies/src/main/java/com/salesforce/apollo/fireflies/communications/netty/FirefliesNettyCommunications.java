/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.communications.netty;

import java.net.InetSocketAddress;
import java.security.cert.X509Certificate;
import java.util.function.Function;

import org.apache.avro.ipc.RPCPlugin;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.specific.SpecificResponder;

import com.salesforce.apollo.avro.Apollo;
import com.salesforce.apollo.comm.netty4.NettyTlsTransceiver;
import com.salesforce.apollo.fireflies.CertWithKey;
import com.salesforce.apollo.fireflies.FirefliesParameters;
import com.salesforce.apollo.fireflies.Member;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.fireflies.View;
import com.salesforce.apollo.fireflies.communications.FfClientCommunications;
import com.salesforce.apollo.fireflies.communications.FfServerCommunications;
import com.salesforce.apollo.fireflies.communications.FirefliesCommunications;

import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContextBuilder;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class FirefliesNettyCommunications extends CommonNettyCommunications implements FirefliesCommunications {
    public static String label() {
        return "FF";
    }

    volatile View view; 

    public FirefliesNettyCommunications(RPCPlugin stats, EventLoopGroup clientGroup, EventLoopGroup bossGroup,
            EventLoopGroup workerGroup) {
        super(stats, clientGroup, bossGroup, workerGroup);
    }

    @Override
    public FfClientCommunications connectTo(Member to, Node from) {
        try {
            FfClientCommunications thisOutbound[] = new FfClientCommunications[1];
            FfClientCommunications outbound = new FfClientCommunications(
                    new NettyTlsTransceiver(to.getFirefliesEndpoint(), forClient(from).build(), clientGroup) {

                        @Override
                        public void close() {
                            openOutbound.remove(thisOutbound[0]);
                            super.close();
                        }

                    }, to);
            thisOutbound[0] = outbound;
            openOutbound.add(outbound);
            return outbound;
        } catch (Throwable e) {
            log.debug("Error connecting to {}", to, e);
            return null;
        }
    }

    @Override
    public void initialize(View view) {
        this.view = view;
    }

    @Override
    public Node newNode(CertWithKey identity, FirefliesParameters parameters) {
        return new Node(identity, parameters);
    }

    @Override
    public Node newNode(CertWithKey identity, FirefliesParameters parameters, InetSocketAddress[] boundPorts) {
        return new Node(identity, parameters, boundPorts);
    }

    @Override
    protected ClientAuth clientAuth() {
        return ClientAuth.REQUIRE;
    }

    @Override
    protected InetSocketAddress endpoint() {
        return view.getNode().getFirefliesEndpoint();
    }
 
    protected Function<X509Certificate, Responder> provider() {
        return certificate -> {
            SpecificResponder responder = new SpecificResponder(Apollo.class,
                    new FfServerCommunications(view.getService(), certificate));
            if (stats != null) {
                responder.addRPCPlugin(stats);
            }
            return responder;
        };
    }

    @Override
    protected SslContextBuilder sslCtxBuilder() {
        return forServer(view.getNode());
    }
}
