package com.salesforce.apollo.comm.grpc;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.EventLoopTaskQueueFactory;
import io.netty.channel.SelectStrategyFactory;
import io.netty.channel.unix.PeerCredentials;
import io.netty.channel.unix.ServerDomainSocketChannel;
import io.netty.util.concurrent.EventExecutorChooserFactory;
import io.netty.util.concurrent.RejectedExecutionHandler;

import java.util.concurrent.Executor;

public interface DomainSockets {
    Class<? extends Channel> getChannelType();

    EventLoopGroup getEventLoopGroup();

    EventLoopGroup getEventLoopGroup(int threads);

    EventLoopGroup getEventLoopGroup(int threads, Executor executor);

    EventLoopGroup getEventLoopGroup(int threads, Executor executor, EventExecutorChooserFactory chooserFactory,
                                     SelectStrategyFactory selectStrategyFactory);

    EventLoopGroup getEventLoopGroup(int threads, Executor executor, EventExecutorChooserFactory chooserFactory,
                                     SelectStrategyFactory selectStrategyFactory,
                                     RejectedExecutionHandler rejectedExecutionHandler);

    EventLoopGroup getEventLoopGroup(int threads, Executor executor, EventExecutorChooserFactory chooserFactory,
                                     SelectStrategyFactory selectStrategyFactory,
                                     RejectedExecutionHandler rejectedExecutionHandler,
                                     EventLoopTaskQueueFactory queueFactory);

    EventLoopGroup getEventLoopGroup(int threads, Executor executor, SelectStrategyFactory selectStrategyFactory);

    PeerCredentials getPeerCredentials(Channel channel);

    Class<? extends ServerDomainSocketChannel> getServerDomainSocketChannelClass();
}
