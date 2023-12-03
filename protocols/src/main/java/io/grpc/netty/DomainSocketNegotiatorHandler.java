/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package io.grpc.netty;

import com.salesforce.apollo.comm.grpc.DomainSockets;
import io.grpc.Attributes;
import io.grpc.ChannelLogger;
import io.grpc.Grpc;
import io.grpc.Grpc.TransportAttr;
import io.grpc.SecurityLevel;
import io.grpc.internal.GrpcAttributes;
import io.grpc.netty.InternalProtocolNegotiator.ProtocolNegotiator;
import io.grpc.netty.ProtocolNegotiators.GrpcNegotiationHandler;
import io.grpc.netty.ProtocolNegotiators.ProtocolNegotiationHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.unix.PeerCredentials;
import io.netty.util.AsciiString;

/**
 * @author hal.hildebrand
 */

public class DomainSocketNegotiatorHandler extends ProtocolNegotiationHandler {
    @TransportAttr
    public static final Attributes.Key<PeerCredentials> TRANSPORT_ATTR_PEER_CREDENTIALS = Attributes.Key.create(
    "com.salesforce.apollo.TRANSPORT_ATTR_PEER_CREDENTIAL");
    private final       DomainSockets                   domainSockets;
    boolean protocolNegotiationEventReceived;

    DomainSocketNegotiatorHandler(ChannelHandler next, ChannelLogger negotiationLogger, DomainSockets domainSockets) {
        super(next, negotiationLogger);
        this.domainSockets = domainSockets;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        if (protocolNegotiationEventReceived) {
            replaceOnActive(ctx);
            fireProtocolNegotiationEvent(ctx);
        }
        // Still propagate channelActive to the new handler.
        super.channelActive(ctx);
    }

    @Override
    protected void protocolNegotiationEventTriggered(ChannelHandlerContext ctx) {
        protocolNegotiationEventReceived = true;
        if (ctx.channel().isActive()) {
            replaceOnActive(ctx);
            fireProtocolNegotiationEvent(ctx);
        }
    }

    private void replaceOnActive(ChannelHandlerContext ctx) {
        ProtocolNegotiationEvent existingPne = getProtocolNegotiationEvent();
        PeerCredentials credentials = domainSockets.getPeerCredentials(ctx.channel());
        Attributes attrs = existingPne.getAttributes()
                                      .toBuilder()
                                      .set(GrpcAttributes.ATTR_SECURITY_LEVEL, SecurityLevel.PRIVACY_AND_INTEGRITY)
                                      .set(TRANSPORT_ATTR_PEER_CREDENTIALS, credentials)
                                      .set(Grpc.TRANSPORT_ATTR_LOCAL_ADDR, ctx.channel().localAddress())
                                      .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, ctx.channel().remoteAddress())
                                      .build();
        replaceProtocolNegotiationEvent(existingPne.withAttributes(attrs));
    }

    public static final class DomainSocketNegotiator implements ProtocolNegotiator {

        private final DomainSockets domainSockets;

        public DomainSocketNegotiator(DomainSockets domainSockets) {
            this.domainSockets = domainSockets;
        }

        @Override
        public void close() {
        }

        @Override
        public ChannelHandler newHandler(GrpcHttp2ConnectionHandler grpcHandler) {
            ChannelHandler grpcNegotiationHandler = new GrpcNegotiationHandler(grpcHandler);
            return new DomainSocketNegotiatorHandler(grpcNegotiationHandler, grpcHandler.getNegotiationLogger(),
                                                     domainSockets);
        }

        @Override
        public AsciiString scheme() {
            return AsciiString.of("domain");
        }
    }
}
