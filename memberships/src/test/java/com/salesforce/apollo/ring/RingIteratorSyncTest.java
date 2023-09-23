package com.salesforce.apollo.ring;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.test.proto.ByteMessage;
import com.salesfoce.apollo.test.proto.TestItGrpc;
import com.salesforce.apollo.archipelago.*;
import com.salesforce.apollo.archipeligo.RouterTest;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.utils.Utils;
import io.grpc.BindableService;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

/**
 * @author hal.hildebrand
 **/
public class RingIteratorSyncTest {
    public static class Server extends TestItGrpc.TestItImplBase {
        private final RoutableService<RouterTest.TestIt> router;

        public Server(RoutableService<RouterTest.TestIt> router) {
            this.router = router;
        }

        @Override
        public void ping(Any request, StreamObserver<Any> responseObserver) {
            router.evaluate(responseObserver, t -> t.ping(request, responseObserver));
        }
    }

    public class ServerA implements RouterTest.TestIt {
        @Override
        public void ping(Any request, StreamObserver<Any> responseObserver) {
            responseObserver.onNext(Any.pack(ByteMessage.newBuilder()
                    .setContents(ByteString.copyFromUtf8("Hello Server A"))
                    .build()));
            responseObserver.onCompleted();
        }
    }

    public class ServerB implements RouterTest.TestIt {
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

    public static class TestItClient implements RouterTest.TestItService {
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

    public static interface TestItService extends Link {
        Any ping(Any request);
    }

    @Test
    public void smokin() throws Exception {
        var local = new RouterTest.TestItService() {

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
        Context<Member> context = Context.newBuilder().build();

        var serverBuilder = InProcessServerBuilder.forName(name);
        var cacheBuilder = ServerConnectionCache.newBuilder()
                .setFactory(to -> InProcessChannelBuilder.forName(name).build());
        var router = new RouterImpl(serverMember, serverBuilder, cacheBuilder, null);
        final var ctxA = DigestAlgorithm.DEFAULT.getOrigin().prefix(0x666);
        RouterImpl.CommonCommunications<RouterTest.TestItService, RouterTest.TestIt> commsA = router.create(serverMember, ctxA, new  ServerA(),
                "A", r -> new RouterTest.Server(r),
                c -> new RouterTest.TestItClient(c), local);

        final var ctxB = DigestAlgorithm.DEFAULT.getLast().prefix(0x666);
        RouterImpl.CommonCommunications<RouterTest.TestItService, RouterTest.TestIt> commsB = router.create(serverMember, ctxB, new  ServerB(),
                "A", r -> new RouterTest.Server(r),
                c -> new RouterTest.TestItClient(c), local);

        router.start();
        var frequency = Duration.ofMillis(100);
        var scheduler = Executors.newSingleThreadScheduledExecutor();
        var exec = Executors.newVirtualThreadPerTaskExecutor();
        var sync = new RingIteratorSync<Member, RouterTest.TestItService>(frequency, context, serverMember, scheduler, commsA, exec);

        sync.iterate(Digest.NONE, (link, round) -> null, (round, result, link) -> false);
    }
}
