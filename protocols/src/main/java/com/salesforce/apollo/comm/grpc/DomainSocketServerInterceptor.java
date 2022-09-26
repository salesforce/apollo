/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm.grpc;

import static io.grpc.netty.DomainSocketNegotiatorHandler.TRANSPORT_ATTR_PEER_CREDENTIALS;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueDomainSocketChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerDomainSocketChannel;
import io.netty.channel.unix.PeerCredentials;
import io.netty.channel.unix.ServerDomainSocketChannel;

/**
 * @author hal.hildebrand
 *
 */
public class DomainSocketServerInterceptor implements ServerInterceptor {

    public static final Context.Key<PeerCredentials> PEER_CREDENTIALS_CONTEXT_KEY = Context.key("com.salesforce.apollo.PEER_CREDENTIALS");

    public static Class<? extends Channel> getChannelType() {
        return KQueue.isAvailable() ? KQueueDomainSocketChannel.class : EpollDomainSocketChannel.class;
    }

    public static EventLoopGroup getEventLoopGroup() {
        return KQueue.isAvailable() ? new KQueueEventLoopGroup() : new EpollEventLoopGroup();
    }

    public static Class<? extends ServerDomainSocketChannel> getServerDomainSocketChannelClass() {
        return KQueue.isAvailable() ? KQueueServerDomainSocketChannel.class : EpollServerDomainSocketChannel.class;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                                                                 final Metadata requestHeaders,
                                                                 ServerCallHandler<ReqT, RespT> next) {
        var principal = call.getAttributes().get(TRANSPORT_ATTR_PEER_CREDENTIALS);
        if (principal == null) {
            call.close(Status.INTERNAL.withCause(new NullPointerException("Principal is missing"))
                                      .withDescription("Principal is missing"),
                       null);
            return new ServerCall.Listener<ReqT>() {
            };
        }
        Context ctx = Context.current().withValue(PEER_CREDENTIALS_CONTEXT_KEY, principal);
        return Contexts.interceptCall(ctx, call, requestHeaders, next);
    }

}
