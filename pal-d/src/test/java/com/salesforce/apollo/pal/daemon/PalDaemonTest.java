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

import com.google.protobuf.Any;
import com.salesfoce.apollo.pal.proto.PalDGrpc;
import com.salesfoce.apollo.pal.proto.PalDGrpc.PalDBlockingStub;

import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
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
        var server = new PalDaemon(socketPath);
        server.start();

        ManagedChannel channel = NettyChannelBuilder.forAddress(new DomainSocketAddress(target))
                                                    .eventLoopGroup(new KQueueEventLoopGroup())
                                                    .channelType(KQueueDomainSocketChannel.class)
                                                    .keepAliveTime(1, TimeUnit.MILLISECONDS)
                                                    .usePlaintext()
                                                    .build();
        assertFalse(channel.isShutdown());
        PalDBlockingStub stub = PalDGrpc.newBlockingStub(channel);

        var result = stub.ping(Any.newBuilder().build());
        assertNotNull(result);
    }
}
