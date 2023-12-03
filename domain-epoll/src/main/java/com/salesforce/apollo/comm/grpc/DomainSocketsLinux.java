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
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.unix.PeerCredentials;
import io.netty.channel.unix.ServerDomainSocketChannel;
import io.netty.util.concurrent.EventExecutorChooserFactory;
import io.netty.util.concurrent.RejectedExecutionHandler;

import java.io.IOException;
import java.util.concurrent.Executor;

/**
 * @author hal.hildebrand
 */
public class DomainSocketsLinux implements DomainSockets {

    public Class<? extends Channel> getChannelType() {
        return EpollDomainSocketChannel.class;
    }

    public EventLoopGroup getEventLoopGroup() {
        return new EpollEventLoopGroup();
    }

    public EventLoopGroup getEventLoopGroup(int threads) {
        return new EpollEventLoopGroup(threads);
    }

    public EventLoopGroup getEventLoopGroup(int threads, Executor executor) {
        return new EpollEventLoopGroup(threads, executor);
    }

    public EventLoopGroup getEventLoopGroup(int threads, Executor executor, EventExecutorChooserFactory chooserFactory,
                                            SelectStrategyFactory selectStrategyFactory) {
        return new EpollEventLoopGroup(threads, executor, chooserFactory, selectStrategyFactory);
    }

    public EventLoopGroup getEventLoopGroup(int threads, Executor executor, EventExecutorChooserFactory chooserFactory,
                                            SelectStrategyFactory selectStrategyFactory,
                                            RejectedExecutionHandler rejectedExecutionHandler) {
        return new EpollEventLoopGroup(threads, executor, chooserFactory, selectStrategyFactory,
                                       rejectedExecutionHandler);
    }

    public EventLoopGroup getEventLoopGroup(int threads, Executor executor, EventExecutorChooserFactory chooserFactory,
                                            SelectStrategyFactory selectStrategyFactory,
                                            RejectedExecutionHandler rejectedExecutionHandler,
                                            EventLoopTaskQueueFactory queueFactory) {
        return new EpollEventLoopGroup(threads, executor, chooserFactory, selectStrategyFactory,
                                       rejectedExecutionHandler, queueFactory);
    }

    public EventLoopGroup getEventLoopGroup(int threads, Executor executor,
                                            SelectStrategyFactory selectStrategyFactory) {
        return new EpollEventLoopGroup(threads, executor, selectStrategyFactory);
    }

    public PeerCredentials getPeerCredentials(Channel channel) {
        if (channel instanceof EpollDomainSocketChannel ep) {
            try {
                return ep.peerCredentials();
            } catch (IOException e) {
                throw new IllegalStateException("Cannot get peer credentials for: " + channel, e);
            }
        } else {
            throw new IllegalStateException("Cannot get peer credentials (unsupported) for: " + channel);
        }
    }

    public Class<? extends ServerDomainSocketChannel> getServerDomainSocketChannelClass() {
        return EpollServerDomainSocketChannel.class;
    }
}
