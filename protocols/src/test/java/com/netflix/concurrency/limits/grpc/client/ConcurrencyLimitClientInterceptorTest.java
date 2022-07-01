package com.netflix.concurrency.limits.grpc.client;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.grpc.StringMarshaller;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCalls;

public class ConcurrencyLimitClientInterceptorTest {
    private static final MethodDescriptor<String, String> METHOD_DESCRIPTOR = MethodDescriptor.<String, String>newBuilder()
                                                                                              .setType(MethodType.UNARY)
                                                                                              .setFullMethodName("service/method")
                                                                                              .setRequestMarshaller(StringMarshaller.INSTANCE)
                                                                                              .setResponseMarshaller(StringMarshaller.INSTANCE)
                                                                                              .build();

//    @Test
    public void simulation() throws IOException {
        Semaphore sem = new Semaphore(20, true);
        Server server = NettyServerBuilder.forPort(0)
                                          .addService(ServerServiceDefinition.builder("service")
                                                                             .addMethod(METHOD_DESCRIPTOR,
                                                                                        ServerCalls.asyncUnaryCall((req,
                                                                                                                    observer) -> {
                                                                                            try {
                                                                                                sem.acquire();
                                                                                                TimeUnit.MILLISECONDS.sleep(100);
                                                                                            } catch (InterruptedException e) {
                                                                                            } finally {
                                                                                                sem.release();
                                                                                            }

                                                                                            observer.onNext("response");
                                                                                            observer.onCompleted();
                                                                                        }))
                                                                             .build())
                                          .build()
                                          .start();

        Limiter<GrpcClientRequestContext> limiter = new GrpcClientLimiterBuilder().blockOnLimit(true).build();

        Channel channel = NettyChannelBuilder.forTarget("localhost:" + server.getPort())
                                             .usePlaintext()
                                             .intercept(new ConcurrencyLimitClientInterceptor(limiter))
                                             .build();

        AtomicLong counter = new AtomicLong();
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            System.out.println(" " + counter.getAndSet(0) + " : " + limiter.toString());
        }, 1, 1, TimeUnit.SECONDS);

        for (int i = 0; i < 10000000; i++) {
            counter.incrementAndGet();
            ClientCalls.futureUnaryCall(channel.newCall(METHOD_DESCRIPTOR, CallOptions.DEFAULT), "request");
        }
    }
}
