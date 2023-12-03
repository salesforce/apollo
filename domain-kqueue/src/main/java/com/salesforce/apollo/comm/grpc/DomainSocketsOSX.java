/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm.grpc;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.EventLoopTaskQueueFactory;
import io.netty.channel.SelectStrategyFactory;
import io.netty.channel.kqueue.KQueueDomainSocketChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerDomainSocketChannel;
import io.netty.channel.unix.PeerCredentials;
import io.netty.channel.unix.ServerDomainSocketChannel;
import io.netty.util.concurrent.EventExecutorChooserFactory;
import io.netty.util.concurrent.RejectedExecutionHandler;

import java.io.IOException;
import java.util.concurrent.Executor;

/**
 * @author hal.hildebrand
 */
public class DomainSocketsOSX implements DomainSockets {
    static {
        try {
            final var sock = new KQueueServerDomainSocketChannel();
            sock.close();
        } catch (Throwable t) {
            //            t.printStackTrace();
        }
    }

    @Override
    public Class<? extends Channel> getChannelType() {
        return KQueueDomainSocketChannel.class;
    }

    @Override
    public EventLoopGroup getEventLoopGroup() {
        return new KQueueEventLoopGroup();
    }

    @Override
    public EventLoopGroup getEventLoopGroup(int threads) {
        return new KQueueEventLoopGroup(threads);
    }

    @Override
    public EventLoopGroup getEventLoopGroup(int threads, Executor executor) {
        return new KQueueEventLoopGroup(threads, executor);
    }

    @Override
    public EventLoopGroup getEventLoopGroup(int threads, Executor executor, EventExecutorChooserFactory chooserFactory,
                                            SelectStrategyFactory selectStrategyFactory) {
        return new KQueueEventLoopGroup(threads, executor, chooserFactory, selectStrategyFactory);
    }

    @Override
    public EventLoopGroup getEventLoopGroup(int threads, Executor executor, EventExecutorChooserFactory chooserFactory,
                                            SelectStrategyFactory selectStrategyFactory,
                                            RejectedExecutionHandler rejectedExecutionHandler) {
        return new KQueueEventLoopGroup(threads, executor, chooserFactory, selectStrategyFactory,
                                        rejectedExecutionHandler);
    }

    @Override
    public EventLoopGroup getEventLoopGroup(int threads, Executor executor, EventExecutorChooserFactory chooserFactory,
                                            SelectStrategyFactory selectStrategyFactory,
                                            RejectedExecutionHandler rejectedExecutionHandler,
                                            EventLoopTaskQueueFactory queueFactory) {
        return new KQueueEventLoopGroup(threads, executor, chooserFactory, selectStrategyFactory,
                                        rejectedExecutionHandler, queueFactory);
    }

    @Override
    public EventLoopGroup getEventLoopGroup(int threads, Executor executor,
                                            SelectStrategyFactory selectStrategyFactory) {
        return new KQueueEventLoopGroup(threads, executor, selectStrategyFactory);
    }

    @Override
    public PeerCredentials getPeerCredentials(Channel channel) {
        if (channel instanceof KQueueDomainSocketChannel ep) {
            try {
                return ep.peerCredentials();
            } catch (IOException e) {
                throw new IllegalStateException("Cannot get peer credentials for: " + channel, e);
            }
        } else {
            throw new IllegalStateException("Cannot get peer credentials (unsupported) for: " + channel);
        }
    }

    @Override
    public Class<? extends ServerDomainSocketChannel> getServerDomainSocketChannelClass() {
        return KQueueServerDomainSocketChannel.class;
    }
}
