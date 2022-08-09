/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.pal.daemon;

import static io.grpc.netty.DomainSocketNegotiatorHandler.TRANSPORT_ATTR_PEER_CREDENTIALS;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.salesfoce.apollo.pal.proto.Decrypted;
import com.salesfoce.apollo.pal.proto.Encrypted;
import com.salesfoce.apollo.pal.proto.PalGrpc.PalImplBase;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.DomainSocketNegotiatorHandler.DomainSocketNegotiator;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerDomainSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.channel.unix.PeerCredentials;

/**
 * @author hal.hildebrand
 *
 */
public class PalDaemon {

    class PalDaemonService extends PalImplBase {

        @Override
        public void decrypt(Encrypted request, StreamObserver<Decrypted> responseObserver) {
            PalDaemon.this.decrypt(request).whenComplete((s, t) -> {
                if (t != null) {
                    responseObserver.onError(t);
                } else {
                    responseObserver.onNext(s);
                    responseObserver.onCompleted();
                }
            });
        }

    }

    public static final Metadata.Key<String>                                     PRINCIPAL_METADATA_KEY = Metadata.Key.of("Principal",
                                                                                                                          Metadata.ASCII_STRING_MARSHALLER);
    private static final Context.Key<PeerCredentials>                            CLIENT_ID_CONTEXT_KEY  = Context.key("domain.principal");
    private final Map<String, Function<Encrypted, CompletableFuture<Decrypted>>> decrypters;
    private final Function<PeerCredentials, CompletableFuture<Set<String>>>      labelsRetriever;
    private final ThreadLocal<PeerCredentials>                                   peerCredentials        = new ThreadLocal<>();
    private final Server                                                         server;

    public PalDaemon(Path socketPath, Function<PeerCredentials, CompletableFuture<Set<String>>> labelsRetriever,
                     Map<String, Function<Encrypted, CompletableFuture<Decrypted>>> decrypters) {
        var group = KQueue.isAvailable() ? new KQueueEventLoopGroup() : new EpollEventLoopGroup();
        server = NettyServerBuilder.forAddress(new DomainSocketAddress(socketPath.toFile().getAbsolutePath()))
                                   .protocolNegotiator(new DomainSocketNegotiator())
                                   .channelType(KQueue.isAvailable() ? KQueueServerDomainSocketChannel.class
                                                                     : EpollServerDomainSocketChannel.class)
                                   .workerEventLoopGroup(group)
                                   .bossEventLoopGroup(group)
                                   .addService(new PalDaemonService())
                                   .intercept(interceptor())
                                   .build();
        this.labelsRetriever = labelsRetriever;
        this.decrypters = decrypters;
    }

    public void start() throws IOException {
        server.start();
    }

    private CompletableFuture<Decrypted> decrypt(Encrypted secrets) {
        var fs = new CompletableFuture<Decrypted>();
        if (true) {
            fs.complete(Decrypted.getDefaultInstance());
            return fs;
        }
        final var credentials = getCredentials();
        if (credentials == null) {
            fs.completeExceptionally(new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("No credentials available")));
        }
        labelsRetriever.apply(credentials).thenAccept(validLabels -> {
            secrets.getSecretsMap().forEach((environment, secret) -> {
                for (var label : secret.getLabelsList()) {
                    if (!validLabels.contains(label)) {
                        fs.completeExceptionally(new StatusRuntimeException(Status.PERMISSION_DENIED.withDescription("No permission for label")));
                        break;
                    }
                }
            });
        });

        return fs;
    }

    private PeerCredentials getCredentials() {
        return peerCredentials.get();
    }

    private ServerInterceptor interceptor() {
        return new ServerInterceptor() {

            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                                                                         final Metadata requestHeaders,
                                                                         ServerCallHandler<ReqT, RespT> next) {

                call.getAttributes().get(null);
                var principal = call.getAttributes().get(TRANSPORT_ATTR_PEER_CREDENTIALS);
                if (principal == null) {
                    call.close(Status.INTERNAL.withCause(new NullPointerException("Principal is missing"))
                                              .withDescription("Principal is missing"),
                               null);
                    return new ServerCall.Listener<ReqT>() {
                    };
                }
                peerCredentials.set(principal);
                return Contexts.interceptCall(Context.current(), call, requestHeaders, next);
            }

        };
    }
}
