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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Collections;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.demesne.proto.DemesneParameters;
import com.salesfoce.apollo.demesne.proto.SubContext;
import com.salesfoce.apollo.test.proto.ByteMessage;
import com.salesfoce.apollo.test.proto.TestItGrpc;
import com.salesfoce.apollo.test.proto.TestItGrpc.TestItBlockingStub;
import com.salesfoce.apollo.test.proto.TestItGrpc.TestItImplBase;
import com.salesfoce.apollo.utils.proto.Digeste;
import com.salesforce.apollo.archipelago.Link;
import com.salesforce.apollo.archipelago.ManagedServerChannel;
import com.salesforce.apollo.archipelago.RoutableService;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.RouterImpl;
import com.salesforce.apollo.archipelago.ServerConnectionCache;
import com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.model.demesnes.DemesneImpl;
import com.salesforce.apollo.model.demesnes.comm.DemesneKERLServer;
import com.salesforce.apollo.model.demesnes.comm.OuterContextServer;
import com.salesforce.apollo.model.demesnes.comm.OuterContextService;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
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
        t.smokin();
        t.after();
        System.exit(0);
    }

    private EventLoopGroup eventLoopGroup;

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

    public void smokin() throws Exception {
        Digest context = DigestAlgorithm.DEFAULT.getOrigin();
        final var commDirectory = Path.of("target").resolve(UUID.randomUUID().toString());
        Files.createDirectories(commDirectory);
        final var kerl = new MemKERL(DigestAlgorithm.DEFAULT);
        Stereotomy controller = new StereotomyImpl(new MemKeyStore(), kerl, SecureRandom.getInstanceStrong());
        ControlledIdentifier<SelfAddressingIdentifier> identifier = controller.newIdentifier().get();
        Member serverMember = new ControlledIdentifierMember(identifier);
        final var portalAddress = UUID.randomUUID().toString();
        final var portalEndpoint = new DomainSocketAddress(commDirectory.resolve(portalAddress).toFile());
        final var router = new RouterImpl(serverMember,
                                          NettyServerBuilder.forAddress(portalEndpoint)
                                                            .protocolNegotiator(new DomainSocketNegotiator())
                                                            .channelType(serverChannelType)
                                                            .workerEventLoopGroup(eventLoopGroup)
                                                            .bossEventLoopGroup(eventLoopGroup)
                                                            .intercept(new DomainSocketServerInterceptor()),
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
                                                          .protocolNegotiator(new DomainSocketNegotiator())
                                                          .channelType(getServerDomainSocketChannelClass())
                                                          .addService(kerlServer)
                                                          .addService(outerService)
                                                          .workerEventLoopGroup(getEventLoopGroup())
                                                          .bossEventLoopGroup(getEventLoopGroup())
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
        identifier.seal(builder)
                  .thenAccept(coords -> demesne.commit(coords.toEventCoords()))
                  .thenAccept(v -> demesne.start())
                  .get();
        Thread.sleep(Duration.ofSeconds(2));
        demesne.stop();
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
