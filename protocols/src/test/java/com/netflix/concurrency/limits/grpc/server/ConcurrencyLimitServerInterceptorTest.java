package com.netflix.concurrency.limits.grpc.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.grpc.StringMarshaller;
import com.netflix.concurrency.limits.grpc.mockito.OptionalResultCaptor;
import com.netflix.concurrency.limits.limiter.SimpleLimiter;

import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCalls;

public class ConcurrencyLimitServerInterceptorTest {

    private static final MethodDescriptor<String, String> METHOD_DESCRIPTOR = MethodDescriptor.<String, String>newBuilder()
                                                                                              .setType(MethodType.UNARY)
                                                                                              .setFullMethodName("service/method")
                                                                                              .setRequestMarshaller(StringMarshaller.INSTANCE)
                                                                                              .setResponseMarshaller(StringMarshaller.INSTANCE)
                                                                                              .build();

    Limiter<GrpcServerRequestContext>      limiter;
    OptionalResultCaptor<Limiter.Listener> listener;

    private ManagedChannel channel;
    private Server         server;

    @AfterEach
    public void afterEachTest() {
        if (server != null) {
            server.shutdown();
            server = null;
        }
        if (channel != null) {
            channel.shutdown();
            channel = null;
        }
    }

    @BeforeEach
    public void beforeEachTest() {
        limiter = Mockito.spy(SimpleLimiter.newBuilder().named("foo").build());

        listener = OptionalResultCaptor.forClass(Limiter.Listener.class);

        Mockito.doAnswer(listener).when(limiter).acquire(Mockito.any());
    }

    @Test
    public void releaseOnCancellation() {
        // Setup server
        startServer((req, observer) -> {
            Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
            observer.onNext("delayed_response");
            observer.onCompleted();
        });

        ListenableFuture<String> future = ClientCalls.futureUnaryCall(channel.newCall(METHOD_DESCRIPTOR,
                                                                                      CallOptions.DEFAULT),
                                                                      "foo");
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        future.cancel(true);

        // Verify
        Mockito.verify(limiter, Mockito.times(1)).acquire(Mockito.isA(GrpcServerRequestContext.class));
        Mockito.verify(listener.getResult().get(), Mockito.times(0)).onIgnore();

        Mockito.verify(listener.getResult().get(), Mockito.timeout(2000).times(1)).onSuccess();

        verifyCounts(0, 0, 1, 0);
    }

    @Test
    public void releaseOnDeadlineExceeded() {
        // Setup server
        startServer((req, observer) -> {
            Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
            observer.onNext("delayed_response");
            observer.onCompleted();
        });

        try {
            ClientCalls.blockingUnaryCall(channel.newCall(METHOD_DESCRIPTOR,
                                                          CallOptions.DEFAULT.withDeadlineAfter(1, TimeUnit.SECONDS)),
                                          "foo");
        } catch (StatusRuntimeException e) {
            assertEquals(Status.Code.DEADLINE_EXCEEDED, e.getStatus().getCode());
        }
        // Verify
        Mockito.verify(limiter, Mockito.times(1)).acquire(Mockito.isA(GrpcServerRequestContext.class));
        Mockito.verify(listener.getResult().get(), Mockito.times(0)).onIgnore();

        Mockito.verify(listener.getResult().get(), Mockito.timeout(2000).times(1)).onSuccess();

        verifyCounts(0, 0, 1, 0);
    }

    @Test
    public void releaseOnError() {
        // Setup server
        startServer((req, observer) -> {
            observer.onError(Status.INVALID_ARGUMENT.asRuntimeException());
        });

        try {
            ClientCalls.blockingUnaryCall(channel, METHOD_DESCRIPTOR, CallOptions.DEFAULT, "foo");
            fail("Should have failed with INVALID_ARGUMENT error");
        } catch (StatusRuntimeException e) {
            assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
        }
        // Verify
        Mockito.verify(limiter, Mockito.times(1)).acquire(Mockito.isA(GrpcServerRequestContext.class));

        verifyCounts(0, 0, 1, 0);
    }

    @Test
    public void releaseOnSuccess() {
        // Setup server
        startServer((req, observer) -> {
            observer.onNext("response");
            observer.onCompleted();
        });

        ClientCalls.blockingUnaryCall(channel, METHOD_DESCRIPTOR, CallOptions.DEFAULT, "foo");
        Mockito.verify(limiter, Mockito.times(1)).acquire(Mockito.isA(GrpcServerRequestContext.class));
        Mockito.verify(listener.getResult().get(), Mockito.timeout(1000).times(1)).onSuccess();

        verifyCounts(0, 0, 1, 0);
    }

    @Test
    public void releaseOnUncaughtException() throws Exception {
        Thread.sleep(500);
        // Setup server
        startServer((req, observer) -> {
            throw new RuntimeException("failure");
        });
        try {
            ClientCalls.blockingUnaryCall(channel, METHOD_DESCRIPTOR, CallOptions.DEFAULT, "foo");
            fail("Should have failed with UNKNOWN error");
        } catch (StatusRuntimeException e) {
            assertEquals(Status.Code.UNKNOWN, e.getStatus().getCode());
        }
        Thread.sleep(500);
        var builder = new StringBuilder().append('\n')
                                         .append('\n')
                                         .append("******************************************")
                                         .append('\n')
                                         .append("*** 2 stack traces above were expected ***")
                                         .append('\n')
                                         .append("******************************************")
                                         .append('\n')
                                         .append('\n');
        LoggerFactory.getLogger(getClass()).warn(builder.toString());
        // Verify
        Mockito.verify(limiter, Mockito.times(1)).acquire(Mockito.isA(GrpcServerRequestContext.class));
        Mockito.verify(listener.getResult().get(), Mockito.timeout(1000).times(1)).onIgnore();

        verifyCounts(0, 1, 0, 0);
    }

    public void verifyCounts(int dropped, int ignored, int success, int rejected) {
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
        }
//        assertEquals(dropped,
//                     registry.counter("unit.test.limiter.call", "id", testName.getMethodName(), "status", "dropped")
//                             .count());
//        assertEquals(ignored,
//                     registry.counter("unit.test.limiter.call", "id", testName.getMethodName(), "status", "ignored")
//                             .count());
//        assertEquals(success,
//                     registry.counter("unit.test.limiter.call", "id", testName.getMethodName(), "status", "success")
//                             .count());
//        assertEquals(rejected,
//                     registry.counter("unit.test.limiter.call", "id", testName.getMethodName(), "status", "rejected")
//                             .count());
    }

    private void startServer(ServerCalls.UnaryMethod<String, String> method) {
        try {
            server = NettyServerBuilder.forPort(0)
                                       .addService(ServerInterceptors.intercept(ServerServiceDefinition.builder("service")
                                                                                                       .addMethod(METHOD_DESCRIPTOR,
                                                                                                                  ServerCalls.asyncUnaryCall(method))
                                                                                                       .build(),
                                                                                ConcurrencyLimitServerInterceptor.newBuilder(limiter)
                                                                                                                 .build()))
                                       .build()
                                       .start();

            channel = NettyChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext().build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
