/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipelago;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.test.proto.ByteMessage;
import com.salesforce.apollo.test.proto.TestItGrpc;
import com.salesforce.apollo.test.proto.TestItGrpc.TestItBlockingStub;
import com.salesforce.apollo.test.proto.TestItGrpc.TestItImplBase;
import com.salesforce.apollo.utils.Utils;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.joou.ULong;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author hal.hildebrand
 */
public class RouterTest {
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
        var serverMember = new SigningMemberImpl(Utils.getMember(0), ULong.MIN);
        final var name = UUID.randomUUID().toString();

        var serverBuilder = InProcessServerBuilder.forName(name);
        var cacheBuilder = ServerConnectionCache.newBuilder()
                                                .setFactory(to -> InProcessChannelBuilder.forName(name).build());
        var router = new RouterImpl(serverMember, serverBuilder, cacheBuilder, null);
        final var ctxA = DigestAlgorithm.DEFAULT.getOrigin().prefix(0x666);
        RouterImpl.CommonCommunications<TestItService, TestIt> commsA = router.create(serverMember, ctxA, new ServerA(),
                                                                                      "A", r -> new Server(r),
                                                                                      c -> new TestItClient(c), local);

        final var ctxB = DigestAlgorithm.DEFAULT.getLast().prefix(0x666);
        RouterImpl.CommonCommunications<TestItService, TestIt> commsB = router.create(serverMember, ctxB, new ServerB(),
                                                                                      "A", r -> new Server(r),
                                                                                      c -> new TestItClient(c), local);

        router.start();

        var clientA = commsA.connect(new SigningMemberImpl(Utils.getMember(1), ULong.MIN));

        var resultA = clientA.ping(Any.getDefaultInstance());
        assertNotNull(resultA);
        var msg = resultA.unpack(ByteMessage.class);
        assertEquals("Hello Server A", msg.getContents().toStringUtf8());

        var clientB = commsB.connect(new SigningMemberImpl(Utils.getMember(2), ULong.MIN));
        var resultB = clientB.ping(Any.getDefaultInstance());
        assertNotNull(resultB);
        msg = resultB.unpack(ByteMessage.class);
        assertEquals("Hello Server B", msg.getContents().toStringUtf8());

        router.close(Duration.ofSeconds(1));
    }

    public interface TestIt {
        void ping(Any request, StreamObserver<Any> responseObserver);
    }

    public interface TestItService extends Link {
        Any ping(Any request);
    }

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

    public static class TestItClient implements TestItService {
        private final TestItBlockingStub   client;
        private final ManagedServerChannel connection;

        public TestItClient(ManagedServerChannel c) {
            this.connection = c;
            client = TestItGrpc.newBlockingStub(c);
        }

        @Override
        public void close() throws IOException {
            connection.release();
        }

        @Override
        public Member getMember() {
            return connection.getMember();
        }

        @Override
        public Any ping(Any request) {
            return client.ping(request);
        }
    }

    public class ServerA implements TestIt {
        @Override
        public void ping(Any request, StreamObserver<Any> responseObserver) {
            responseObserver.onNext(
            Any.pack(ByteMessage.newBuilder().setContents(ByteString.copyFromUtf8("Hello Server A")).build()));
            responseObserver.onCompleted();
        }
    }

    public class ServerB implements TestIt {
        @Override
        public void ping(Any request, StreamObserver<Any> responseObserver) {
            responseObserver.onNext(
            Any.pack(ByteMessage.newBuilder().setContents(ByteString.copyFromUtf8("Hello Server B")).build()));
            responseObserver.onCompleted();
        }
    }
}
