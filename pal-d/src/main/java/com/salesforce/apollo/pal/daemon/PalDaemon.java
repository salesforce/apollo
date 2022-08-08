/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.pal.daemon;

import java.io.IOException;
import java.nio.file.Path;

import com.salesforce.apollo.pal.daemon.grpc.PalDaemonService;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerDomainSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;

/**
 * @author hal.hildebrand
 *
 */
public class PalDaemon {

    private final Server server;

    public PalDaemon(Path socketPath) {
        var group = KQueue.isAvailable() ? new KQueueEventLoopGroup() : new EpollEventLoopGroup();
        server = NettyServerBuilder.forAddress(new DomainSocketAddress(socketPath.toFile().getAbsolutePath()))
                                   .channelType(KQueue.isAvailable() ? KQueueServerDomainSocketChannel.class
                                                                     : EpollServerDomainSocketChannel.class)
                                   .workerEventLoopGroup(group)
                                   .bossEventLoopGroup(group)
                                   .addService(new PalDaemonService(this))
                                   .build();
    }

    public void start() throws IOException {
        server.start();
    }
}
