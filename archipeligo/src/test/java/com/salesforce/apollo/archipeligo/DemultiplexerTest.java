/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipeligo;

import static com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor.PEER_CREDENTIALS_CONTEXT_KEY;
import static com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor.getChannelType;
import static com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor.getEventLoopGroup;
import static com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor.getServerDomainSocketChannelClass;
import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.google.common.primitives.Ints;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.test.proto.ByteMessage;
import com.salesfoce.apollo.test.proto.PeerCreds;
import com.salesfoce.apollo.test.proto.TestItGrpc;
import com.salesfoce.apollo.test.proto.TestItGrpc.TestItImplBase;
import com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.DomainSocketNegotiatorHandler.DomainSocketNegotiator;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.channel.unix.DomainSocketAddress;

/**
 * @author hal.hildebrand
 *
 */
public class DemultiplexerTest {

    public static class ServerA extends TestItImplBase {
        @Override
        public void ping(Any request, StreamObserver<Any> responseObserver) {
            final var credentials = PEER_CREDENTIALS_CONTEXT_KEY.get();
            if (credentials == null) {
                responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("No credentials available")));
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
                responseObserver.onError(new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("No credentials available")));
                return;
            }
            responseObserver.onNext(Any.pack(ByteMessage.newBuilder()
                                                        .setContents(ByteString.copyFromUtf8("Hello Server"))
                                                        .build()));
            responseObserver.onCompleted();
        }
    }

    public static ClientInterceptor clientInterceptor(Digest ctx) {
        return new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                                       CallOptions callOptions, Channel next) {
                ClientCall<ReqT, RespT> newCall = next.newCall(method, callOptions);
                return new SimpleForwardingClientCall<ReqT, RespT>(newCall) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        headers.put(Router.METADATA_CONTEXT_KEY, qb64(ctx));
                        super.start(responseListener, headers);
                    }
                };
            }
        };
    }

    private Server        serverA;
    private Server        serverB;
    private Demultiplexer terminus;

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
    }

    @Test
    public void smokin() throws Exception {
        final var name = UUID.randomUUID().toString();
        var routes = new HashMap<String, DomainSocketAddress>();
        Function<String, Channel> dmux = d -> handler(routes.get(d));

        terminus = new Demultiplexer(InProcessServerBuilder.forName(name), Router.METADATA_CONTEXT_KEY, dmux);
        terminus.start();

        var ctxA = DigestAlgorithm.DEFAULT.getOrigin();
        routes.put(qb64(ctxA), serverA());

        var clientA = TestItGrpc.newBlockingStub(InProcessChannelBuilder.forName(name)
                                                                        .intercept(clientInterceptor(ctxA))
                                                                        .build());

        var ctxB = DigestAlgorithm.DEFAULT.getLast();
        routes.put(qb64(ctxB), serverB());

        var clientB = TestItGrpc.newBlockingStub(InProcessChannelBuilder.forName(name)
                                                                        .intercept(clientInterceptor(ctxB))
                                                                        .build());

        var resultA = clientA.ping(Any.newBuilder().build());
        assertNotNull(resultA);
        var creds = resultA.unpack(PeerCreds.class);
        assertNotNull(creds);

        var resultB = clientB.ping(Any.newBuilder().build());
        assertNotNull(resultB);
        var msg = resultB.unpack(ByteMessage.class);
        assertEquals("Hello Server", msg.getContents().toStringUtf8());
    }

    private Channel handler(DomainSocketAddress address) {
        return NettyChannelBuilder.forAddress(address)
                                  .eventLoopGroup(getEventLoopGroup())
                                  .channelType(getChannelType())
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
                                    .protocolNegotiator(new DomainSocketNegotiator())
                                    .channelType(getServerDomainSocketChannelClass())
                                    .workerEventLoopGroup(getEventLoopGroup())
                                    .bossEventLoopGroup(getEventLoopGroup())
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
                                    .protocolNegotiator(new DomainSocketNegotiator())
                                    .channelType(getServerDomainSocketChannelClass())
                                    .workerEventLoopGroup(getEventLoopGroup())
                                    .bossEventLoopGroup(getEventLoopGroup())
                                    .addService(new ServerB())
                                    .intercept(new DomainSocketServerInterceptor())
                                    .build();
        serverB.start();
        return address;
    }
}
