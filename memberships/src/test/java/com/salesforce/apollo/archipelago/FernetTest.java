package com.salesforce.apollo.archipelago;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.macasaet.fernet.Key;
import com.macasaet.fernet.Token;
import com.salesforce.apollo.archipelago.client.FernetCallCredentials;
import com.salesforce.apollo.archipelago.server.FernetServerInterceptor;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.test.proto.ByteMessage;
import com.salesforce.apollo.test.proto.TestItGrpc;
import com.salesforce.apollo.utils.Utils;
import io.grpc.CallCredentials;
import io.grpc.stub.StreamObserver;
import org.joou.ULong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test Token Auth via Fernet Tokens
 *
 * @author hal.hildebrand
 **/
public class FernetTest {
    private final TestItService local = new TestItService() {

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
    private       Token         token;

    @BeforeEach
    public void before() {
        final SecureRandom deterministicRandom = new SecureRandom() {
            private static final long serialVersionUID = 3075400891983079965L;

            public void nextBytes(final byte[] bytes) {
                for (int i = bytes.length; --i >= 0; bytes[i] = 1)
                    ;
            }
        };
        final Key key = Key.generateKey(deterministicRandom);

        token = Token.generate(deterministicRandom, key, "Hello, world!");
    }

    @Test
    public void smokin() throws Exception {
        final var credentials = FernetCallCredentials.blocking(() -> token);
        final var memberA = new SigningMemberImpl(Utils.getMember(0), ULong.MIN);
        final var memberB = new SigningMemberImpl(Utils.getMember(1), ULong.MIN);
        final var ctxA = DigestAlgorithm.DEFAULT.getOrigin().prefix(0x666);
        final var prefix = UUID.randomUUID().toString();

        RouterSupplier serverA = new LocalServer(prefix, memberA);
        var routerA = serverA.router(ServerConnectionCache.newBuilder(), () -> RouterImpl.defaultServerLimit(), null,
                                     Collections.singletonList(new FernetServerInterceptor()));

        RouterImpl.CommonCommunications<TestItService, TestIt> commsA = routerA.create(memberA, ctxA, new ServerA(),
                                                                                       "A", r -> new Server(r),
                                                                                       c -> new TestItClient(c,
                                                                                                             credentials),
                                                                                       local);

        RouterSupplier serverB = new LocalServer(prefix, memberB);
        var routerB = serverB.router(ServerConnectionCache.newBuilder(), () -> RouterImpl.defaultServerLimit(), null,
                                     Collections.singletonList(new FernetServerInterceptor()));

        RouterImpl.CommonCommunications<TestItService, TestIt> commsA_B = routerB.create(memberB, ctxA, new ServerB(),
                                                                                         "B", r -> new Server(r),
                                                                                         c -> new TestItClient(c,
                                                                                                               credentials),
                                                                                         local);

        routerA.start();
        routerB.start();

        var clientA = commsA.connect(memberB);

        var resultA = clientA.ping(Any.getDefaultInstance());
        assertNotNull(resultA);
        assertEquals("Hello Server B", resultA.unpack(ByteMessage.class).getContents().toStringUtf8());

        var clientB = commsA_B.connect(memberA);
        var resultB = clientB.ping(Any.getDefaultInstance());
        assertNotNull(resultB);
        assertEquals("Hello Server A", resultB.unpack(ByteMessage.class).getContents().toStringUtf8());

        routerA.close(Duration.ofSeconds(1));
        routerB.close(Duration.ofSeconds(1));
    }

    public interface TestIt {
        void ping(Any request, StreamObserver<Any> responseObserver);
    }

    public interface TestItService extends Link {
        Any ping(Any request);
    }

    public static class Server extends TestItGrpc.TestItImplBase {
        private final RoutableService<TestIt> router;

        public Server(RoutableService<TestIt> router) {
            this.router = router;
        }

        @Override
        public void ping(Any request, StreamObserver<Any> responseObserver) {
            router.evaluate(responseObserver, (t, token) -> {
                assertNotNull(token);
                t.ping(request, responseObserver);
            });
        }
    }

    public static class TestItClient implements TestItService {
        private final TestItGrpc.TestItBlockingStub client;
        private final ManagedServerChannel          connection;
        private final CallCredentials               credentials;

        public TestItClient(ManagedServerChannel c, CallCredentials credentials) {
            this.connection = c;
            this.credentials = credentials;
            client = TestItGrpc.newBlockingStub(c).withCallCredentials(credentials);
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
