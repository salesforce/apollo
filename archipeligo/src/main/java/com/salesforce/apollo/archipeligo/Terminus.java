/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipeligo;

import static com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor.getChannelType;
import static com.salesforce.apollo.comm.grpc.DomainSocketServerInterceptor.getEventLoopGroup;
import static com.salesforce.apollo.crypto.QualifiedBase64.digest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;
import com.salesforce.apollo.crypto.Digest;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Contexts;
import io.grpc.HandlerRegistry;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerMethodDefinition;
import io.grpc.Status;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.unix.DomainSocketAddress;

/**
 * @author hal.hildebrand
 *
 */
public class Terminus {
    private static class ByteMarshaller implements MethodDescriptor.Marshaller<byte[]> {
        @Override
        public byte[] parse(InputStream stream) {
            try {
                return ByteStreams.toByteArray(stream);
            } catch (IOException ex) {
                throw new RuntimeException();
            }
        }

        @Override
        public InputStream stream(byte[] value) {
            return new ByteArrayInputStream(value);
        }
    }

    private static class CallProxy<ReqT, RespT> {
        private class RequestProxy extends ServerCall.Listener<ReqT> {
            private final ClientCall<ReqT, ?> clientCall;
            private final Lock                lock = new ReentrantLock();
            private boolean                   needToRequest;

            public RequestProxy(ClientCall<ReqT, ?> clientCall) {
                this.clientCall = clientCall;
            }

            @Override
            public void onCancel() {
                clientCall.cancel("Server cancelled", null);
            }

            @Override
            public void onHalfClose() {
                clientCall.halfClose();
            }

            @Override
            public void onMessage(ReqT message) {
                clientCall.sendMessage(message);
                lock.lock();
                try {
                    if (clientCall.isReady()) {
                        clientCallListener.serverCall.request(1);
                    } else {
                        needToRequest = true;
                    }
                } finally {
                    lock.unlock();
                }
            }

            @Override
            public void onReady() {
                clientCallListener.onServerReady();
            }

            void onClientReady() {
                lock.lock();
                try {
                    if (needToRequest) {
                        clientCallListener.serverCall.request(1);
                        needToRequest = false;
                    }
                } finally {
                    lock.unlock();
                }
            }
        }

        private class ResponseProxy extends ClientCall.Listener<RespT> {
            private final Lock                 lock = new ReentrantLock();
            private boolean                    needToRequest;
            private final ServerCall<?, RespT> serverCall;

            public ResponseProxy(ServerCall<?, RespT> serverCall) {
                this.serverCall = serverCall;
            }

            @Override
            public void onClose(Status status, Metadata trailers) {
                serverCall.close(status, trailers);
            }

            @Override
            public void onHeaders(Metadata headers) {
                serverCall.sendHeaders(headers);
            }

            @Override
            public void onMessage(RespT message) {
                serverCall.sendMessage(message);
                lock.lock();
                try {
                    if (serverCall.isReady()) {
                        serverCallListener.clientCall.request(1);
                    } else {
                        needToRequest = true;
                    }
                } finally {
                    lock.unlock();
                }
            }

            @Override
            public void onReady() {
                serverCallListener.onClientReady();
            }

            void onServerReady() {
                lock.lock();
                try {
                    if (needToRequest) {
                        serverCallListener.clientCall.request(1);
                        needToRequest = false;
                    }
                } finally {
                    lock.unlock();
                }
            }
        }

        final ResponseProxy clientCallListener;
        final RequestProxy  serverCallListener;

        public CallProxy(ServerCall<ReqT, RespT> serverCall, ClientCall<ReqT, RespT> clientCall) {
            serverCallListener = new RequestProxy(clientCall);
            clientCallListener = new ResponseProxy(serverCall);
        }
    }

    private class GrpcProxy<ReqT, RespT> implements ServerCallHandler<ReqT, RespT> {
        @Override
        public ServerCall.Listener<ReqT> startCall(ServerCall<ReqT, RespT> serverCall, Metadata headers) {
            ClientCall<ReqT, RespT> clientCall = getChannel().newCall(serverCall.getMethodDescriptor(),
                                                                      CallOptions.DEFAULT);
            CallProxy<ReqT, RespT> proxy = new CallProxy<>(serverCall, clientCall);
            clientCall.start(proxy.clientCallListener, headers);
            serverCall.request(1);
            clientCall.request(1);
            return proxy.serverCallListener;
        }

        private Channel getChannel() {
            var address = enclaves.get(Terminus.CLIENT_CONTEXT_KEY.get());
            if (address == null) {
                return null;
            }
            return handler(address);
        }

        private Channel handler(DomainSocketAddress address) {
            return NettyChannelBuilder.forAddress(address)
                                      .eventLoopGroup(getEventLoopGroup())
                                      .channelType(getChannelType())
                                      .keepAliveTime(1, TimeUnit.MILLISECONDS)
                                      .usePlaintext()
                                      .build();
        }

    }

    public static final Metadata.Key<String>         CONTEXT_METADATA_KEY = Metadata.Key.of("from.Context",
                                                                                            Metadata.ASCII_STRING_MARSHALLER);
    private static final io.grpc.Context.Key<Digest> CLIENT_CONTEXT_KEY   = io.grpc.Context.key("from.Context");
    private static final Logger                      log                  = LoggerFactory.getLogger(Terminus.class);
    @SuppressWarnings("unused")
    private static final Metadata.Key<String>        TARGET_METADATA_KEY  = Metadata.Key.of("to.Endpoint",
                                                                                            Metadata.ASCII_STRING_MARSHALLER);

    private final Map<Digest, DomainSocketAddress> enclaves = new ConcurrentSkipListMap<>();
    private final Server                           server;

    /**
     *
     */
    public Terminus(ServerBuilder<?> builder) {
        final var interceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                                                                         final Metadata requestHeaders,
                                                                         ServerCallHandler<ReqT, RespT> next) {
                String id = requestHeaders.get(CONTEXT_METADATA_KEY);
                if (id == null) {
                    log.error("No context id in call headers: {}", requestHeaders.keys());
                    throw new IllegalStateException("No context ID in call");
                }

                return Contexts.interceptCall(io.grpc.Context.current().withValue(CLIENT_CONTEXT_KEY, digest(id)), call,
                                              requestHeaders, next);
            }
        };
        final var registry = new HandlerRegistry() {
            private final MethodDescriptor.Marshaller<byte[]> byteMarshaller = new ByteMarshaller();

            @Override
            public ServerMethodDefinition<?, ?> lookupMethod(String methodName, String authority) {
                MethodDescriptor<byte[], byte[]> methodDescriptor = MethodDescriptor.newBuilder(byteMarshaller,
                                                                                                byteMarshaller)
                                                                                    .setFullMethodName(methodName)
                                                                                    .setType(MethodDescriptor.MethodType.UNKNOWN)
                                                                                    .build();

                return ServerMethodDefinition.create(methodDescriptor, new GrpcProxy<>());
            }
        };
        this.server = builder.intercept(interceptor).fallbackHandlerRegistry(registry).build();
    }

    public void register(Digest ctx, DomainSocketAddress address) {
        enclaves.put(ctx, address);
    }

    public void start() throws IOException {
        server.start();
    }
}
