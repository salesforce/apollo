/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipeligo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.test.proto.ByteMessage;
import com.salesfoce.apollo.test.proto.TestItGrpc;
import com.salesfoce.apollo.test.proto.TestItGrpc.TestItBlockingStub;
import com.salesfoce.apollo.test.proto.TestItGrpc.TestItImplBase;
import com.salesforce.apollo.archipeligo.ServerConnectionCache.ReleasableManagedChannel;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.utils.Utils;

import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class RouterTest {
    public static class Server extends TestItImplBase {
        private final RoutableService<TestIt> router;

        public Server(RoutableService<TestIt> router) {
            this.router = router;
        }

        @Override
        public void ping(Any request, StreamObserver<Any> responseObserver) {
            router.evaluate(responseObserver, t -> t.ping(request, responseObserver));
        }
    }

    public class ServerA implements TestIt {
        @Override
        public void ping(Any request, StreamObserver<Any> responseObserver) {
            responseObserver.onNext(Any.pack(ByteMessage.newBuilder()
                                                        .setContents(ByteString.copyFromUtf8("Hello Server A"))
                                                        .build()));
            responseObserver.onCompleted();
        }
    }

    public class ServerB implements TestIt {
        @Override
        public void ping(Any request, StreamObserver<Any> responseObserver) {
            responseObserver.onNext(Any.pack(ByteMessage.newBuilder()
                                                        .setContents(ByteString.copyFromUtf8("Hello Server B"))
                                                        .build()));
            responseObserver.onCompleted();
        }
    }

    public static interface TestIt {
        void ping(Any request, StreamObserver<Any> responseObserver);
    }

    public static class TestItClient implements TestItService {
        private final TestItBlockingStub               client;
        private final ReleasableManagedChannel<Member> connection;

        public TestItClient(ReleasableManagedChannel<Member> c) {
            this.connection = c;
            client = TestItGrpc.newBlockingStub(c);
        }

        @Override
        public void close() throws IOException {
            connection.release();
        }

        @Override
        public Member getMember() {
            return connection.getTo();
        }

        @Override
        public Any ping(Any request) {
            return client.ping(request);
        }
    }

    public static interface TestItService extends Link<Member> {
        Any ping(Any request);
    }

    @Test
    public void smokin() throws Exception {
        var local = new TestItService() {

            @Override
            public void close() throws IOException {
            }

            @Override
            public Member getMember() {
                return null;
            }

            @Override
            public Any ping(Any request) {
                return null;
            }
        };
        var serverMember = new SigningMemberImpl(Utils.getMember(0));
        final var name = UUID.randomUUID().toString();

        var serverBuilder = InProcessServerBuilder.forName(name);
        var cacheBuilder = ServerConnectionCache.newBuilder()
                                                .setFactory(to -> InProcessChannelBuilder.forName(name).build());
        var router = new Router<Member>(serverBuilder, cacheBuilder, null);
        final var ctxA = DigestAlgorithm.DEFAULT.getOrigin().prefix(0x666);
        Router<Member>.CommonCommunications<TestItService, TestIt> commsA = router.create(serverMember, ctxA,
                                                                                          new ServerA(), "A",
                                                                                          r -> new Server(r),
                                                                                          c -> new TestItClient(c),
                                                                                          local);

        final var ctxB = DigestAlgorithm.DEFAULT.getLast().prefix(0x666);
        Router<Member>.CommonCommunications<TestItService, TestIt> commsB = router.create(serverMember, ctxB,
                                                                                          new ServerB(), "A",
                                                                                          r -> new Server(r),
                                                                                          c -> new TestItClient(c),
                                                                                          local);

        router.start();

        var clientA = commsA.connect(new SigningMemberImpl(Utils.getMember(1)));

        var resultA = clientA.ping(Any.newBuilder().build());
        assertNotNull(resultA);
        var msg = resultA.unpack(ByteMessage.class);
        assertEquals("Hello Server A", msg.getContents().toStringUtf8());

        var clientB = commsB.connect(new SigningMemberImpl(Utils.getMember(2)));
        var resultB = clientB.ping(Any.newBuilder().build());
        assertNotNull(resultB);
        msg = resultB.unpack(ByteMessage.class);
        assertEquals("Hello Server B", msg.getContents().toStringUtf8());

        router.close(Duration.ofSeconds(1));
    }
}
