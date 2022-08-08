/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.pal.daemon;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.salesfoce.apollo.pal.proto.PalGrpc.PalImplBase;
import com.salesfoce.apollo.pal.proto.PalSecrets;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
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

    public class PalDaemonService extends PalImplBase {

        @Override
        public void decrypt(PalSecrets request, StreamObserver<PalSecrets> responseObserver) {
            PalDaemon.this.decrypt(request).whenComplete((s, t) -> {
                if (t != null) {
                    responseObserver.onError(t);
                } else {
                    responseObserver.onNext(s);
                    responseObserver.onCompleted();
                }
            });
        }

    }

    private final Function<String, Set<String>> labelsRetriever;
    private final Server                        server;

    public PalDaemon(Path socketPath, Function<String, Set<String>> labelsRetriever) {

        var group = KQueue.isAvailable() ? new KQueueEventLoopGroup() : new EpollEventLoopGroup();
        server = NettyServerBuilder.forAddress(new DomainSocketAddress(socketPath.toFile().getAbsolutePath()))
                                   .channelType(KQueue.isAvailable() ? KQueueServerDomainSocketChannel.class
                                                                     : EpollServerDomainSocketChannel.class)
                                   .workerEventLoopGroup(group)
                                   .bossEventLoopGroup(group)
                                   .addService(new PalDaemonService())
                                   .build();
        this.labelsRetriever = labelsRetriever;
    }

    public void start() throws IOException {
        server.start();
    }

    private CompletableFuture<PalSecrets> decrypt(PalSecrets secrets) {
        var fs = new CompletableFuture<PalSecrets>();
        fs.complete(PalSecrets.getDefaultInstance());
        return fs;
    }
}
