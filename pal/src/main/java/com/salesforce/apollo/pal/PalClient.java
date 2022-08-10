/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.pal;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import com.salesfoce.apollo.pal.proto.Decrypted;
import com.salesfoce.apollo.pal.proto.Encrypted;
import com.salesfoce.apollo.pal.proto.PalGrpc;
import com.salesfoce.apollo.pal.proto.PalGrpc.PalBlockingStub;

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
public class PalClient {

    private final Path socketPath;

    public PalClient(Path socketPath) {
        this.socketPath = socketPath;
    }

    public Decrypted decrypt(Encrypted encrypted) {
        ManagedChannel channel = NettyChannelBuilder.forAddress(new DomainSocketAddress(socketPath.toFile()))
                                                    .eventLoopGroup(KQueue.isAvailable() ? new KQueueEventLoopGroup()
                                                                                         : new EpollEventLoopGroup())
                                                    .channelType(KQueue.isAvailable() ? KQueueDomainSocketChannel.class
                                                                                      : EpollDomainSocketChannel.class)
                                                    .keepAliveTime(1000, TimeUnit.SECONDS)
                                                    .usePlaintext()
                                                    .build();
        PalBlockingStub stub = PalGrpc.newBlockingStub(channel);
        try {
            return stub.decrypt(encrypted);
        } finally {
            channel.shutdown();
        }
    }
}
