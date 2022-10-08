/*
 * Copyright 2017, gRPC Authors All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.salesforce.apollo.archipeligo;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.common.io.ByteStreams;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.HandlerRegistry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerMethodDefinition;
import io.grpc.Status;

/** A grpc-level proxy. */
public class GrpcProxy<ReqT, RespT> implements ServerCallHandler<ReqT, RespT> {
    public static class Registry extends HandlerRegistry {
        private final MethodDescriptor.Marshaller<byte[]> byteMarshaller = new ByteMarshaller();
        private final ServerCallHandler<byte[], byte[]>   handler;

        public Registry(ServerCallHandler<byte[], byte[]> handler) {
            this.handler = handler;
        }

        @Override
        public ServerMethodDefinition<?, ?> lookupMethod(String methodName, String authority) {
            MethodDescriptor<byte[], byte[]> methodDescriptor = MethodDescriptor.newBuilder(byteMarshaller,
                                                                                            byteMarshaller)
                                                                                .setFullMethodName(methodName)
                                                                                .setType(MethodDescriptor.MethodType.UNKNOWN)
                                                                                .build();
            return ServerMethodDefinition.create(methodDescriptor, handler);
        }
    }

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
            // Hold 'this' lock when accessing
            private boolean needToRequest;

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
                synchronized (this) {
                    if (clientCall.isReady()) {
                        clientCallListener.serverCall.request(1);
                    } else {
                        needToRequest = true;
                    }
                }
            }

            @Override
            public void onReady() {
                clientCallListener.onServerReady();
            }

            synchronized void onClientReady() {
                if (needToRequest) {
                    clientCallListener.serverCall.request(1);
                    needToRequest = false;
                }
            }
        }

        private class ResponseProxy extends ClientCall.Listener<RespT> {
            // Hold 'this' lock when accessing
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
                synchronized (this) {
                    if (serverCall.isReady()) {
                        serverCallListener.clientCall.request(1);
                    } else {
                        needToRequest = true;
                    }
                }
            }

            @Override
            public void onReady() {
                serverCallListener.onClientReady();
            }

            synchronized void onServerReady() {
                if (needToRequest) {
                    serverCallListener.clientCall.request(1);
                    needToRequest = false;
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

    private static final Logger logger = Logger.getLogger(GrpcProxy.class.getName());

    public static void main(String[] args) throws IOException, InterruptedException {
        String target = "localhost:8980";
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        logger.info("Proxy will connect to " + target);
        GrpcProxy<byte[], byte[]> proxy = new GrpcProxy<>(channel);
        int port = 8981;
        Server server = ServerBuilder.forPort(port).fallbackHandlerRegistry(new Registry(proxy)).build().start();
        logger.info("Proxy started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                server.shutdown();
                try {
                    server.awaitTermination(10, TimeUnit.SECONDS);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
                if (!server.isTerminated()) {
                    server.shutdownNow();
                }
                channel.shutdownNow();
            }
        });
        server.awaitTermination();
        if (!channel.awaitTermination(1, TimeUnit.SECONDS)) {
            System.out.println("Channel didn't shut down promptly");
        }
    }

    private final Channel channel;

    public GrpcProxy(Channel channel) {
        this.channel = channel;
    }

    @Override
    public ServerCall.Listener<ReqT> startCall(ServerCall<ReqT, RespT> serverCall, Metadata headers) {
        ClientCall<ReqT, RespT> clientCall = channel.newCall(serverCall.getMethodDescriptor(), CallOptions.DEFAULT);
        CallProxy<ReqT, RespT> proxy = new CallProxy<>(serverCall, clientCall);
        clientCall.start(proxy.clientCallListener, headers);
        serverCall.request(1);
        clientCall.request(1);
        return proxy.serverCallListener;
    }
}
