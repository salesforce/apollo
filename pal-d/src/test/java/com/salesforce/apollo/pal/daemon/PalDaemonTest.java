/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.pal.daemon;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.salesfoce.apollo.pal.proto.PalGrpc;
import com.salesfoce.apollo.pal.proto.PalGrpc.PalBlockingStub;
import com.salesfoce.apollo.pal.proto.PalSecrets;

import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueDomainSocketChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.unix.DomainSocketAddress;

/**
 * @author hal.hildebrand
 *
 */
public class PalDaemonTest {

    @Test
    public void smokin() throws Exception {
        Path socketPath = Path.of("target").resolve("smokin.socket");
        Files.deleteIfExists(socketPath);

        var target = socketPath.toFile().getPath();
        var server = new PalDaemon(socketPath, s -> null);
        server.start();

        ManagedChannel channel = NettyChannelBuilder.forAddress(new DomainSocketAddress(target))
                                                    .eventLoopGroup(KQueue.isAvailable() ? new KQueueEventLoopGroup()
                                                                                         : new EpollEventLoopGroup())
                                                    .channelType(KQueue.isAvailable() ? KQueueDomainSocketChannel.class
                                                                                      : EpollDomainSocketChannel.class)
                                                    .keepAliveTime(1, TimeUnit.MILLISECONDS)
                                                    .usePlaintext()
                                                    .build();
        assertFalse(channel.isShutdown());
        PalBlockingStub stub = PalGrpc.newBlockingStub(channel);

        var result = stub.decrypt(PalSecrets.getDefaultInstance());
        assertNotNull(result);
    }
}
