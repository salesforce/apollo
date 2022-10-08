/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipeligo;

import static com.salesforce.apollo.archipeligo.Terminus.CONTEXT_METADATA_KEY;
import static com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor.PEER_CREDENTIALS_CONTEXT_KEY;
import static com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor.getEventLoopGroup;
import static com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor.getServerDomainSocketChannelClass;
import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.google.common.primitives.Ints;
import com.google.protobuf.Any;
import com.salesfoce.apollo.test.proto.PeerCreds;
import com.salesfoce.apollo.test.proto.TestItGrpc;
import com.salesfoce.apollo.test.proto.TestItGrpc.TestItImplBase;
import com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor;
import com.salesforce.apollo.crypto.DigestAlgorithm;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.DomainSocketNegotiatorHandler.DomainSocketNegotiator;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.channel.unix.DomainSocketAddress;

/**
 * @author hal.hildebrand
 *
 */
public class TerminusTest {

    public static class TestServer extends TestItImplBase {

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

    @Test
    public void smokin() throws Exception {
        Path socketPath = Path.of("target").resolve("smokin.socket");
        Files.deleteIfExists(socketPath);
        assertFalse(Files.exists(socketPath));

        final var address = new DomainSocketAddress(socketPath.toFile());
        var server = NettyServerBuilder.forAddress(address)
                                       .protocolNegotiator(new DomainSocketNegotiator())
                                       .channelType(getServerDomainSocketChannelClass())
                                       .workerEventLoopGroup(getEventLoopGroup())
                                       .bossEventLoopGroup(getEventLoopGroup())
                                       .addService(new TestServer())
                                       .intercept(new DomainSocketServerInterceptor())
                                       .build();
        server.start();
        var ctx = DigestAlgorithm.DEFAULT.getOrigin();

        final var name = UUID.randomUUID().toString();
        ServerBuilder<?> builder = InProcessServerBuilder.forName(name);
        var terminus = new Terminus(builder);
        terminus.start();
        terminus.register(ctx, address);

        ClientInterceptor clientInterceptor = new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                                       CallOptions callOptions, Channel next) {
                ClientCall<ReqT, RespT> newCall = next.newCall(method, callOptions);
                return new SimpleForwardingClientCall<ReqT, RespT>(newCall) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        headers.put(CONTEXT_METADATA_KEY, qb64(ctx));
                        super.start(responseListener, headers);
                    }
                };
            }
        };

        var stub = TestItGrpc.newBlockingStub(InProcessChannelBuilder.forName(name)
                                                                     .intercept(clientInterceptor)
                                                                     .build());

        var result = stub.ping(Any.newBuilder().build());
        assertNotNull(result);
        var creds = result.unpack(PeerCreds.class);
        assertNotNull(creds);

        System.out.println("Success:\n" + creds);
    }
}
