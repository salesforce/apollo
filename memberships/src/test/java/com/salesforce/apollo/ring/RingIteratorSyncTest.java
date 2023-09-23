package com.salesforce.apollo.ring;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.test.proto.ByteMessage;
import com.salesfoce.apollo.test.proto.TestItGrpc;
import com.salesforce.apollo.archipelago.*;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.utils.Utils;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author hal.hildebrand
 **/
public class RingIteratorSyncTest {
    @Test
    public void smokin() throws Exception {
        var serverMember1 = new SigningMemberImpl(Utils.getMember(0));
        var serverMember2 = new SigningMemberImpl(Utils.getMember(1));
        var pinged1 = new AtomicBoolean();
        var pinged2 = new AtomicBoolean();

        var local1 = new TestItService() {

            @Override
            public void close() throws IOException {
            }

            @Override
            public Member getMember() {
                return serverMember1;
            }

            @Override
            public Any ping(Any request) {
                pinged1.set(true);
                return Any.getDefaultInstance();
            }
        };
        var local2 = new TestItService() {

            @Override
            public void close() throws IOException {
            }

            @Override
            public Member getMember() {
                return serverMember2;
            }

            @Override
            public Any ping(Any request) {
                pinged2.set(true);
                return Any.getDefaultInstance();
            }
        };
        final var name = UUID.randomUUID().toString();
        Context<Member> context = Context.newBuilder().build();

        var serverBuilder = InProcessServerBuilder.forName(name);
        var cacheBuilder = ServerConnectionCache.newBuilder()
                .setFactory(to -> InProcessChannelBuilder.forName(name).build());
        var router = new RouterImpl(serverMember1, serverBuilder, cacheBuilder, null);
        final var ctxA = DigestAlgorithm.DEFAULT.getOrigin().prefix(0x666);
        RouterImpl.CommonCommunications<TestItService, TestIt> commsA = router.create(serverMember1, ctxA, new ServerA(local1),
                "A", r -> new Server(r),
                c -> new TestItClient(c), local1);

        final var ctxB = DigestAlgorithm.DEFAULT.getLast().prefix(0x666);
        RouterImpl.CommonCommunications<TestItService, TestIt> commsB = router.create(serverMember2, ctxB, new ServerB(local2),
                "A", r -> new Server(r),
                c -> new TestItClient(c), local1);

        router.start();
        var frequency = Duration.ofMillis(100);
        var scheduler = Executors.newSingleThreadScheduledExecutor();
        var exec = Executors.newVirtualThreadPerTaskExecutor();
        var sync = new RingIteratorSync<Member, TestItService>(frequency, context, serverMember1, scheduler, commsA, exec);
        var countdown = new CountDownLatch(2);
        sync.iterate(ctxA,
                (link, round) ->
                        link.ping(Any.getDefaultInstance()),
                (round, result, link) -> {
                    countdown.countDown();
                    return true;
                });
        countdown.await(1, TimeUnit.SECONDS);
        assertTrue(pinged1.get());
        assertFalse(pinged2.get());
    }

    public static interface TestIt {
        void ping(Any request, StreamObserver<Any> responseObserver);
    }

    public static interface TestItService extends Link {
        Any ping(Any request);
    }

    public static class Server extends TestItGrpc.TestItImplBase {
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
        private final TestItGrpc.TestItBlockingStub client;
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
        private final TestItService local;

        public ServerA(TestItService local) {
            this.local = local;
        }

        @Override
        public void ping(Any request, StreamObserver<Any> responseObserver) {
            local.ping(request);
            responseObserver.onNext(Any.pack(ByteMessage.newBuilder()
                    .setContents(ByteString.copyFromUtf8("Hello Server A"))
                    .build()));
            responseObserver.onCompleted();
        }
    }

    public class ServerB implements TestIt {
        private final TestItService local;

        public ServerB(TestItService local) {
            this.local = local;
        }

        @Override
        public void ping(Any request, StreamObserver<Any> responseObserver) {
            local.ping(request);
            responseObserver.onNext(Any.pack(ByteMessage.newBuilder()
                    .setContents(ByteString.copyFromUtf8("Hello Server B"))
                    .build()));
            responseObserver.onCompleted();
        }
    }
}
