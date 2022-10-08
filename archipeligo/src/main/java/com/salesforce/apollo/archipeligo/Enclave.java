/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipeligo;

import static com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor.getEventLoopGroup;
import static com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor.getServerDomainSocketChannelClass;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor;
import com.salesforce.apollo.crypto.Digest;

import io.grpc.Server;
import io.grpc.netty.DomainSocketNegotiatorHandler.DomainSocketNegotiator;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.unix.DomainSocketAddress;

/**
 * @author hal.hildebrand
 *
 */
public class Enclave {

    private final Map<String, RoutableService<Digest, ?>> services = new ConcurrentHashMap<>();
    private final Path                                    socketPath;
    private final Server                                  terminal;

    public Enclave(Path socketPath) {
        this.socketPath = socketPath;
        terminal = NettyServerBuilder.forAddress(new DomainSocketAddress(socketPath.toFile()))
                                     .protocolNegotiator(new DomainSocketNegotiator())
                                     .channelType(getServerDomainSocketChannelClass())
                                     .workerEventLoopGroup(getEventLoopGroup())
                                     .bossEventLoopGroup(getEventLoopGroup())
                                     .intercept(new DomainSocketServerInterceptor())
                                     .build();
    }

    public void start() throws IOException {
        terminal.start();
    }
}
