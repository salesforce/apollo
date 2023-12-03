/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model.demesnes;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.cryptography.proto.Digeste;
import com.salesfoce.apollo.demesne.proto.DemesneParameters;
import com.salesfoce.apollo.demesne.proto.SubContext;
import com.salesfoce.apollo.test.proto.ByteMessage;
import com.salesfoce.apollo.test.proto.TestItGrpc;
import com.salesfoce.apollo.test.proto.TestItGrpc.TestItBlockingStub;
import com.salesfoce.apollo.test.proto.TestItGrpc.TestItImplBase;
import com.salesforce.apollo.archipelago.*;
import com.salesforce.apollo.archipelago.RouterImpl.CommonCommunications;
import com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.model.demesnes.comm.DemesneKERLServer;
import com.salesforce.apollo.model.demesnes.comm.OuterContextServer;
import com.salesforce.apollo.model.demesnes.comm.OuterContextService;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.Stereotomy;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.event.Seal;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification.Builder;
import com.salesforce.apollo.stereotomy.identifier.spec.InteractionSpecification;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLAdapter;
import com.salesforce.apollo.utils.Utils;
import io.grpc.*;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.netty.DomainSocketNegotiatorHandler.DomainSocketNegotiator;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.channel.unix.ServerDomainSocketChannel;
import org.joou.ULong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor.IMPL;
import static com.salesforce.apollo.cryptography.QualifiedBase64.qb64;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author hal.hildebrand
 */
public class DemesneTest {
    private final static Class<? extends io.netty.channel.Channel>  clientChannelType = IMPL.getChannelType();
    private static final Class<? extends ServerDomainSocketChannel> serverChannelType = IMPL.getServerDomainSocketChannelClass();
    private final static Executor                                   executor          = Executors.newVirtualThreadPerTaskExecutor();
    private final        TestItService                              local             = new TestItService() {

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
    private              EventLoopGroup                             eventLoopGroup;

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

    @AfterEach
    public void after() throws Exception {
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully();
            eventLoopGroup.awaitTermination(1, TimeUnit.SECONDS);
            eventLoopGroup = null;
        }
    }

    @BeforeEach
    public void before() {
        eventLoopGroup = IMPL.getEventLoopGroup();
    }

    @Test
    public void portal() throws Exception {
        final var ctxA = DigestAlgorithm.DEFAULT.getOrigin().prefix(0x666);
        final var ctxB = DigestAlgorithm.DEFAULT.getLast().prefix(0x666);
        var serverMember1 = new SigningMemberImpl(Utils.getMember(0), ULong.MIN);
        var serverMember2 = new SigningMemberImpl(Utils.getMember(1), ULong.MIN);

        final var bridge = new DomainSocketAddress(Path.of("target").resolve(UUID.randomUUID().toString()).toFile());

        final var portalEndpoint = new DomainSocketAddress(
        Path.of("target").resolve(UUID.randomUUID().toString()).toFile());
        final var routes = new HashMap<String, DomainSocketAddress>();
        final var portal = new Portal<>(serverMember1.getId(), NettyServerBuilder.forAddress(portalEndpoint)
                                                                                 .protocolNegotiator(
                                                                                 new DomainSocketNegotiator(IMPL))
                                                                                 .channelType(
                                                                                 IMPL.getServerDomainSocketChannelClass())
                                                                                 .workerEventLoopGroup(
                                                                                 IMPL.getEventLoopGroup())
                                                                                 .bossEventLoopGroup(
                                                                                 IMPL.getEventLoopGroup())
                                                                                 .intercept(
                                                                                 new DomainSocketServerInterceptor()),
                                        s -> handler(portalEndpoint), bridge, Duration.ofMillis(1), s -> routes.get(s));

        final var endpoint1 = new DomainSocketAddress(Path.of("target").resolve(UUID.randomUUID().toString()).toFile());
        var enclave1 = new Enclave(serverMember1, endpoint1, bridge, d -> routes.put(qb64(d), endpoint1));
        var router1 = enclave1.router();
        CommonCommunications<TestItService, TestIt> commsA = router1.create(serverMember1, ctxA, new ServerA(), "A",
                                                                            r -> new Server(r),
                                                                            c -> new TestItClient(c), local);

        final var endpoint2 = new DomainSocketAddress(Path.of("target").resolve(UUID.randomUUID().toString()).toFile());
        var enclave2 = new Enclave(serverMember2, endpoint2, bridge, d -> routes.put(qb64(d), endpoint2));
        var router2 = enclave2.router();
        CommonCommunications<TestItService, TestIt> commsB = router2.create(serverMember2, ctxB, new ServerB(), "B",
                                                                            r -> new Server(r),
                                                                            c -> new TestItClient(c), local);

        portal.start();
        router1.start();
        router2.start();

        var clientA = commsA.connect(serverMember2);

        var resultA = clientA.ping(Any.getDefaultInstance());
        assertNotNull(resultA);
        var msg = resultA.unpack(ByteMessage.class);
        assertEquals("Hello Server A", msg.getContents().toStringUtf8());

        var clientB = commsB.connect(serverMember1);
        var resultB = clientB.ping(Any.getDefaultInstance());
        assertNotNull(resultB);
        msg = resultB.unpack(ByteMessage.class);
        assertEquals("Hello Server B", msg.getContents().toStringUtf8());

        portal.close(Duration.ofSeconds(1));
        router1.close(Duration.ofSeconds(1));
        router2.close(Duration.ofSeconds(1));
    }

    @Test
    public void smokin() throws Exception {
        Digest context = DigestAlgorithm.DEFAULT.getOrigin();
        final var commDirectory = Path.of("target").resolve(UUID.randomUUID().toString());
        Files.createDirectories(commDirectory);
        final var kerl = new MemKERL(DigestAlgorithm.DEFAULT);
        Stereotomy controller = new StereotomyImpl(new MemKeyStore(), kerl, SecureRandom.getInstanceStrong());
        ControlledIdentifier<SelfAddressingIdentifier> identifier = controller.newIdentifier();
        Member serverMember = new ControlledIdentifierMember(identifier);
        final var portalAddress = UUID.randomUUID().toString();
        final var portalEndpoint = new DomainSocketAddress(commDirectory.resolve(portalAddress).toFile());
        final var router = new RouterImpl(serverMember, NettyServerBuilder.forAddress(portalEndpoint)
                                                                          .protocolNegotiator(
                                                                          new DomainSocketNegotiator(IMPL))
                                                                          .channelType(serverChannelType)
                                                                          .workerEventLoopGroup(eventLoopGroup)
                                                                          .bossEventLoopGroup(eventLoopGroup)
                                                                          .intercept(
                                                                          new DomainSocketServerInterceptor()),
                                          ServerConnectionCache.newBuilder().setFactory(to -> handler(portalEndpoint)),
                                          null);
        router.start();

        final var registered = new TreeSet<Digest>();
        final var deregistered = new TreeSet<Digest>();

        final OuterContextService service = new OuterContextService() {
            @Override
            public void deregister(Digeste context) {
                deregistered.remove(Digest.from(context));
            }

            @Override
            public void register(SubContext context) {
                registered.add(Digest.from(context.getContext()));
            }
        };

        final var parentAddress = UUID.randomUUID().toString();
        final var parentEndpoint = new DomainSocketAddress(commDirectory.resolve(parentAddress).toFile());
        final var kerlServer = new DemesneKERLServer(new ProtoKERLAdapter(kerl), null);
        final var outerService = new OuterContextServer(service, null);
        final var outerContextService = NettyServerBuilder.forAddress(parentEndpoint)
                                                          .protocolNegotiator(new DomainSocketNegotiator(IMPL))
                                                          .channelType(IMPL.getServerDomainSocketChannelClass())
                                                          .addService(kerlServer)
                                                          .addService(outerService)
                                                          .workerEventLoopGroup(IMPL.getEventLoopGroup())
                                                          .bossEventLoopGroup(IMPL.getEventLoopGroup())
                                                          .intercept(new DomainSocketServerInterceptor())
                                                          .build();
        outerContextService.start();

        final var parameters = DemesneParameters.newBuilder()
                                                .setContext(context.toDigeste())
                                                .setPortal(portalAddress)
                                                .setParent(parentAddress)
                                                .setCommDirectory(commDirectory.toString())
                                                .setMaxTransfer(100)
                                                .setFalsePositiveRate(.125)
                                                .build();
        final var demesne = new DemesneImpl(parameters);
        Builder<SelfAddressingIdentifier> specification = IdentifierSpecification.newBuilder();
        final var incp = demesne.inception(identifier.getIdentifier().toIdent(), specification);

        final var seal = Seal.EventSeal.construct(incp.getIdentifier(), incp.hash(controller.digestAlgorithm()),
                                                  incp.getSequenceNumber().longValue());

        final var builder = InteractionSpecification.newBuilder().addAllSeals(Collections.singletonList(seal));

        // Commit
        EventCoordinates coords = identifier.seal(builder);
        demesne.commit(coords.toEventCoords());
        demesne.start();
        Thread.sleep(Duration.ofSeconds(2));
        demesne.stop();
        assertEquals(1, registered.size());
        assertTrue(registered.contains(context));
        assertEquals(0, deregistered.size());
        assertNotNull(demesne.getId());
        var stored = kerl.getKeyEvent(incp.getCoordinates());
        assertNotNull(stored);
        var attached = kerl.getAttachment(incp.getCoordinates());
        assertNotNull(attached);
        assertEquals(1, attached.seals().size());
        final var extracted = attached.seals().get(0);
        assertTrue(extracted instanceof Seal.DigestSeal);
        //        assertEquals(1, attached.endorsements().size());
    }

    private ManagedChannel handler(DomainSocketAddress address) {
        return NettyChannelBuilder.forAddress(address)
                                  .executor(executor)
                                  .eventLoopGroup(eventLoopGroup)
                                  .channelType(clientChannelType)
                                  .keepAliveTime(1, TimeUnit.SECONDS)
                                  .usePlaintext()
                                  .build();
    }

    public static interface TestIt {
        void ping(Any request, StreamObserver<Any> responseObserver);
    }

    public static interface TestItService extends Link {
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
