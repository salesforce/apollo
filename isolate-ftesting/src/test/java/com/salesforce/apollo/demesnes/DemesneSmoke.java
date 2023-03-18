/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.demesnes;

import static com.salesforce.apollo.comm.grpc.DomainSockets.getChannelType;
import static com.salesforce.apollo.comm.grpc.DomainSockets.getEventLoopGroup;
import static com.salesforce.apollo.comm.grpc.DomainSockets.getServerDomainSocketChannelClass;
import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.demesne.proto.DemesneParameters;
import com.salesfoce.apollo.test.proto.ByteMessage;
import com.salesfoce.apollo.test.proto.TestItGrpc;
import com.salesfoce.apollo.test.proto.TestItGrpc.TestItBlockingStub;
import com.salesfoce.apollo.test.proto.TestItGrpc.TestItImplBase;
import com.salesforce.apollo.archipelago.Enclave;
import com.salesforce.apollo.archipelago.Link;
import com.salesforce.apollo.archipelago.ManagedServerChannel;
import com.salesforce.apollo.archipelago.Portal;
import com.salesforce.apollo.archipelago.RoutableService;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.ServerConnectionCache;
import com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.model.demesnes.DemesneImpl;
import com.salesforce.apollo.stereotomy.Stereotomy;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.jks.JksKeyStore;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.services.grpc.kerl.KERLServer;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLAdapter;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLService;
import com.salesforce.apollo.utils.Utils;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.netty.DomainSocketNegotiatorHandler.DomainSocketNegotiator;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.channel.unix.ServerDomainSocketChannel;

/**
 * @author hal.hildebrand
 *
 */
public class DemesneSmoke {

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

    public static interface TestItService extends Link {
        Any ping(Any request);
    }

    private final static Class<? extends io.netty.channel.Channel> clientChannelType = getChannelType();

    private static final Class<? extends ServerDomainSocketChannel> serverChannelType = getServerDomainSocketChannelClass();

    public static ClientInterceptor clientInterceptor(Digest ctx) {
        return new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                                       CallOptions callOptions, Channel next) {
                ClientCall<ReqT, RespT> newCall = next.newCall(method, callOptions);
                return new SimpleForwardingClientCall<ReqT, RespT>(newCall) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        headers.put(Router.METADATA_CLIENT_ID_KEY, qb64(ctx));
                        super.start(responseListener, headers);
                    }
                };
            }
        };
    }

    public static void main(String[] argv) throws Exception {
        var t = new DemesneSmoke();
        t.before();
        t.portal();
        t.after();
        t.before();
        t.smokin();
        t.after();
        System.exit(0);
    }

    private EventLoopGroup      eventLoopGroup;
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

    public void after() throws Exception {
        if (eventLoopGroup != null) {
            var fs = eventLoopGroup.shutdownGracefully(0, 10, TimeUnit.SECONDS);
            fs.get();
            var success = eventLoopGroup.awaitTermination(10, TimeUnit.SECONDS);
            System.out.println("Shutdown: " + success);
            eventLoopGroup = null;
        }
    }

    public void before() {
        eventLoopGroup = getEventLoopGroup();
    }

    public void portal() throws Exception {
        final var ctxA = DigestAlgorithm.DEFAULT.getOrigin().prefix(0x666);
        final var ctxB = DigestAlgorithm.DEFAULT.getLast().prefix(0x666);
        var serverMember1 = new SigningMemberImpl(Utils.getMember(0));
        var serverMember2 = new SigningMemberImpl(Utils.getMember(1));
        final var bridge = new DomainSocketAddress(Path.of("target").resolve(UUID.randomUUID().toString()).toFile());

        final var exec = Executors.newVirtualThreadPerTaskExecutor();

        final var portalEndpoint = new DomainSocketAddress(Path.of("target")
                                                               .resolve(UUID.randomUUID().toString())
                                                               .toFile());
        final var portal = new Portal<>(NettyServerBuilder.forAddress(portalEndpoint)
                                                          .protocolNegotiator(new DomainSocketNegotiator())
                                                          .channelType(getServerDomainSocketChannelClass())
                                                          .workerEventLoopGroup(eventLoopGroup)
                                                          .bossEventLoopGroup(eventLoopGroup)
                                                          .intercept(new DomainSocketServerInterceptor()),
                                        s -> handler(portalEndpoint), bridge, exec, Duration.ofMillis(1));

        final var endpoint1 = new DomainSocketAddress(Path.of("target").resolve(UUID.randomUUID().toString()).toFile());
        var enclave1 = new Enclave(serverMember1, endpoint1, exec, bridge, Duration.ofMillis(1), d -> {
            portal.register(qb64(d), endpoint1);
        });
        var router1 = enclave1.router(exec);
        Router.CommonCommunications<TestItService, TestIt> commsA = router1.create(serverMember1, ctxA, new ServerA(),
                                                                                   "A", r -> new Server(r),
                                                                                   c -> new TestItClient(c), local);

        final var endpoint2 = new DomainSocketAddress(Path.of("target").resolve(UUID.randomUUID().toString()).toFile());
        var enclave2 = new Enclave(serverMember2, endpoint2, exec, bridge, Duration.ofMillis(1), d -> {
            portal.register(qb64(d), endpoint2);
        });
        var router2 = enclave2.router(exec);
        Router.CommonCommunications<TestItService, TestIt> commsB = router2.create(serverMember2, ctxB, new ServerB(),
                                                                                   "A", r -> new Server(r),
                                                                                   c -> new TestItClient(c), local);

        portal.start();
        router1.start();
        router2.start();

        var clientA = commsA.connect(serverMember2);

        var resultA = clientA.ping(Any.getDefaultInstance());
        resultA.unpack(ByteMessage.class);

        var clientB = commsB.connect(serverMember1);
        var resultB = clientB.ping(Any.getDefaultInstance());
        resultB.unpack(ByteMessage.class);

        portal.close(Duration.ofSeconds(10));
        router1.close(Duration.ofSeconds(10));
        router2.close(Duration.ofSeconds(10));
        exec.shutdownNow();
        exec.awaitTermination(10, TimeUnit.SECONDS);
    }

    public void smokin() throws Exception {
        var commDirectory = Path.of("target").resolve(UUID.randomUUID().toString());
        Files.createDirectories(commDirectory);
        final var ksPassword = new char[] { 'f', 'o', 'o' };
        final var ks = KeyStore.getInstance("JKS");
        ks.load(null, ksPassword);
        final var keystore = new JksKeyStore(ks, () -> ksPassword);
        final var kerl = new MemKERL(DigestAlgorithm.DEFAULT);
        Stereotomy controller = new StereotomyImpl(keystore, kerl, SecureRandom.getInstanceStrong());
        var identifier = controller.newIdentifier().get();
        var baos = new ByteArrayOutputStream();
        ks.store(baos, ksPassword);
        ProtoKERLService protoService = new ProtoKERLAdapter(kerl);
        Member serverMember = new ControlledIdentifierMember(identifier);
        var kerlEndpoint = UUID.randomUUID().toString();
        final var portalEndpoint = new DomainSocketAddress(commDirectory.resolve(kerlEndpoint).toFile());
        var serverBuilder = NettyServerBuilder.forAddress(portalEndpoint)
                                              .protocolNegotiator(new DomainSocketNegotiator())
                                              .channelType(serverChannelType)
                                              .workerEventLoopGroup(eventLoopGroup)
                                              .bossEventLoopGroup(eventLoopGroup)
                                              .intercept(new DomainSocketServerInterceptor());

        var cacheBuilder = ServerConnectionCache.newBuilder().setFactory(to -> handler(portalEndpoint));
        var router = new Router(serverMember, serverBuilder, cacheBuilder, null);
        router.start();
        Digest context = DigestAlgorithm.DEFAULT.getOrigin();
        @SuppressWarnings("unused")
        var comms = router.create(serverMember, context, protoService, protoService.getClass().getCanonicalName(),
                                  r -> new KERLServer(r, null), null, null);

        var parameters = DemesneParameters.newBuilder()
                                          .setMember(identifier.getIdentifier().toIdent())
                                          .setKeyStore(ByteString.copyFrom(baos.toByteArray()))
                                          .setCommDirectory(commDirectory.toString())
                                          .build();
        var demesne = new DemesneImpl(parameters, ksPassword);
        demesne.start();
        Thread.sleep(Duration.ofSeconds(2));
        demesne.stop();
        router.close(Duration.ofSeconds(10));
    }

    private ManagedChannel handler(DomainSocketAddress address) {
        return NettyChannelBuilder.forAddress(address)
                                  .eventLoopGroup(eventLoopGroup)
                                  .channelType(clientChannelType)
                                  .keepAliveTime(1, TimeUnit.SECONDS)
                                  .usePlaintext()
                                  .build();
    }
}