/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipeligo;

import com.google.common.primitives.Ints;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.salesforce.apollo.archipelago.Demultiplexer;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.test.proto.ByteMessage;
import com.salesforce.apollo.test.proto.PeerCreds;
import com.salesforce.apollo.test.proto.TestItGrpc;
import com.salesforce.apollo.test.proto.TestItGrpc.TestItImplBase;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.DomainSocketNegotiatorHandler.DomainSocketNegotiator;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.unix.DomainSocketAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.salesforce.apollo.archipelago.RouterImpl.clientInterceptor;
import static com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor.IMPL;
import static com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor.PEER_CREDENTIALS_CONTEXT_KEY;
import static com.salesforce.apollo.cryptography.QualifiedBase64.qb64;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author hal.hildebrand
 */
public class DemultiplexerTest {

    private static final Class<? extends io.netty.channel.Channel> channelType    = IMPL.getChannelType();
    private static final Executor                                  executor       = Executors.newCachedThreadPool(
    Thread.ofVirtual().factory());
    private final        EventLoopGroup                            eventLoopGroup = IMPL.getEventLoopGroup();
    private final        List<ManagedChannel>                      opened         = new ArrayList<>();
    private              Server                                    serverA;
    private              Server                                    serverB;
    private              Demultiplexer                             terminus;

    @AfterEach
    public void after() throws InterruptedException {
        if (terminus != null) {
            terminus.close(Duration.ofSeconds(1));
        }
        if (serverA != null) {
            serverA.shutdownNow();
            serverA.awaitTermination();
        }
        if (serverB != null) {
            serverB.shutdownNow();
            serverB.awaitTermination();
        }
        opened.forEach(mc -> mc.shutdown());
        opened.clear();
    }

    @Test
    public void smokin() throws Exception {
        final var name = UUID.randomUUID().toString();
        var routes = new HashMap<String, DomainSocketAddress>();
        Function<String, ManagedChannel> dmux = d -> handler(routes.get(d));

        terminus = new Demultiplexer(InProcessServerBuilder.forName(name), Router.METADATA_CONTEXT_KEY, dmux);
        terminus.start();

        var ctxA = DigestAlgorithm.DEFAULT.getOrigin();
        routes.put(qb64(ctxA), serverA());

        var ctxB = DigestAlgorithm.DEFAULT.getLast();
        routes.put(qb64(ctxB), serverB());

        var channel = InProcessChannelBuilder.forName(name).intercept(clientInterceptor(ctxA)).build();
        opened.add(channel);
        var clientA = TestItGrpc.newBlockingStub(channel);
        var resultA = clientA.ping(Any.getDefaultInstance());
        assertNotNull(resultA);
        var creds = resultA.unpack(PeerCreds.class);
        assertNotNull(creds);

        channel = InProcessChannelBuilder.forName(name).intercept(clientInterceptor(ctxB)).build();
        opened.add(channel);
        var clientB = TestItGrpc.newBlockingStub(channel);
        var resultB = clientB.ping(Any.getDefaultInstance());
        assertNotNull(resultB);
        var msg = resultB.unpack(ByteMessage.class);
        assertEquals("Hello Server", msg.getContents().toStringUtf8());
    }

    private ManagedChannel handler(DomainSocketAddress address) {
        return NettyChannelBuilder.forAddress(address)
                                  .executor(executor)
                                  .eventLoopGroup(eventLoopGroup)
                                  .channelType(channelType)
                                  .keepAliveTime(1, TimeUnit.SECONDS)
                                  .usePlaintext()
                                  .build();
    }

    private DomainSocketAddress serverA() throws IOException {
        Path socketPathA = Path.of("target").resolve(UUID.randomUUID().toString());
        Files.deleteIfExists(socketPathA);
        assertFalse(Files.exists(socketPathA));

        final var address = new DomainSocketAddress(socketPathA.toFile());
        serverA = NettyServerBuilder.forAddress(address)
                                    .protocolNegotiator(new DomainSocketNegotiator(IMPL))
                                    .channelType(IMPL.getServerDomainSocketChannelClass())
                                    .workerEventLoopGroup(IMPL.getEventLoopGroup())
                                    .bossEventLoopGroup(IMPL.getEventLoopGroup())
                                    .addService(new ServerA())
                                    .intercept(new DomainSocketServerInterceptor())
                                    .build();
        serverA.start();
        return address;
    }

    private DomainSocketAddress serverB() throws IOException {
        Path socketPathA = Path.of("target").resolve(UUID.randomUUID().toString());
        Files.deleteIfExists(socketPathA);
        assertFalse(Files.exists(socketPathA));

        final var address = new DomainSocketAddress(socketPathA.toFile());
        serverB = NettyServerBuilder.forAddress(address)
                                    .protocolNegotiator(new DomainSocketNegotiator(IMPL))
                                    .channelType(IMPL.getServerDomainSocketChannelClass())
                                    .workerEventLoopGroup(IMPL.getEventLoopGroup())
                                    .bossEventLoopGroup(IMPL.getEventLoopGroup())
                                    .addService(new ServerB())
                                    .intercept(new DomainSocketServerInterceptor())
                                    .build();
        serverB.start();
        return address;
    }

    public static class ServerA extends TestItImplBase {
        @Override
        public void ping(Any request, StreamObserver<Any> responseObserver) {
            final var credentials = PEER_CREDENTIALS_CONTEXT_KEY.get();
            if (credentials == null) {
                responseObserver.onError(
                new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("No credentials available")));
                return;
            }
            responseObserver.onNext(Any.pack(PeerCreds.newBuilder()
                                                      .setPid(credentials.pid())
                                                      .setUid(credentials.uid())
                                                      .addAllGids(Ints.asList(credentials.gids()))
                                                      .build()));
            responseObserver.onCompleted();
        }
    }

    public static class ServerB extends TestItImplBase {
        @Override
        public void ping(Any request, StreamObserver<Any> responseObserver) {
            final var credentials = PEER_CREDENTIALS_CONTEXT_KEY.get();
            if (credentials == null) {
                responseObserver.onError(
                new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("No credentials available")));
                return;
            }
            responseObserver.onNext(
            Any.pack(ByteMessage.newBuilder().setContents(ByteString.copyFromUtf8("Hello Server")).build()));
            responseObserver.onCompleted();
        }
    }
}
