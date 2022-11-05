package com.netflix.concurrency.limits.grpc.server.example;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;

import com.google.common.util.concurrent.Uninterruptibles;
import com.salesforce.apollo.utils.Utils;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.Metadata;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;

public class Driver {
    public static class Builder {
        private String               id       = "";
        private Consumer<Long>       latencyAccumulator;
        private int                  port;
        private long                 runtimeSeconds;
        private List<Driver.Segment> segments = new ArrayList<>();

        public Builder add(String name, Supplier<Long> delaySupplier, long duration, TimeUnit units) {
            segments.add(new Segment() {
                @Override
                public long duration() {
                    return units.toNanos(duration);
                }

                @Override
                public String name() {
                    return name;
                }

                @Override
                public long nextDelay() {
                    return delaySupplier.get();
                }
            });
            return this;
        }

        public Driver build() {
            return new Driver(this);
        }

        public Builder exponential(double mean, long duration, TimeUnit units) {
            final ExponentialDistribution distribution = new ExponentialDistribution(mean);
            return add("exponential(" + mean + ")", () -> (long) distribution.sample(), duration, units);
        }

        public Builder exponentialRps(double rps, long duration, TimeUnit units) {
            return exponential(1000.0 / rps, duration, units);
        }

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder latencyAccumulator(Consumer<Long> consumer) {
            this.latencyAccumulator = consumer;
            return this;
        }

        public Builder normal(double mean, double sd, long duration, TimeUnit units) {
            final NormalDistribution distribution = new NormalDistribution(mean, sd);
            return add("normal(" + mean + ")", () -> (long) distribution.sample(), duration, units);
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder runtime(long duration, TimeUnit units) {
            this.runtimeSeconds = units.toNanos(duration);
            return this;
        }

        public Builder slience(long duration, TimeUnit units) {
            return add("slience()", () -> units.toMillis(duration), duration, units);
        }

        public Builder uniform(double lower, double upper, long duration, TimeUnit units) {
            final UniformRealDistribution distribution = new UniformRealDistribution(lower, upper);
            return add("uniform(" + lower + "," + upper + ")", () -> (long) distribution.sample(), duration, units);
        }
    }

    private interface Segment {
        long duration();

        String name();

        long nextDelay();
    }

    public static final Metadata.Key<String> ID_HEADER = Metadata.Key.of("id", Metadata.ASCII_STRING_MARSHALLER);

    static public Builder newBuilder() {
        return new Builder();
    }

    private final Channel        channel;
    private final AtomicInteger  dropCounter    = new AtomicInteger(0);
    private final Consumer<Long> latencyAccumulator;
    private final long           runtime;
    private final List<Segment>  segments;
    private final AtomicInteger  successCounter = new AtomicInteger(0);

    public Driver(Builder builder) {
        this.segments = builder.segments;
        this.runtime = builder.runtimeSeconds;
        this.latencyAccumulator = builder.latencyAccumulator;

        Metadata metadata = new Metadata();
        metadata.put(ID_HEADER, builder.id);

        this.channel = ClientInterceptors.intercept(NettyChannelBuilder.forTarget("localhost:" + builder.port)
                                                                       .usePlaintext()
                                                                       .build(),
                                                    MetadataUtils.newAttachHeadersInterceptor(metadata));
    }

    public int getAndResetDropCount() {
        return dropCounter.getAndSet(0);
    }

    public int getAndResetSuccessCount() {
        return successCounter.getAndSet(0);
    }

    public void run() {
        long endTime = System.nanoTime() + this.runtime;
        while (true) {
            for (Driver.Segment segment : segments) {
                long segmentEndTime = System.nanoTime() + segment.duration();
                while (true) {
                    long currentTime = System.nanoTime();
                    if (currentTime > endTime) {
                        return;
                    }

                    if (currentTime > segmentEndTime) {
                        break;
                    }

                    long startTime = System.nanoTime();
                    Uninterruptibles.sleepUninterruptibly(Math.max(0, segment.nextDelay()), TimeUnit.MILLISECONDS);
                    ClientCalls.asyncUnaryCall(channel.newCall(TestServer.METHOD_DESCRIPTOR,
                                                               CallOptions.DEFAULT.withWaitForReady()),
                                               "request", new StreamObserver<String>() {
                                                   @Override
                                                   public void onCompleted() {
                                                       latencyAccumulator.accept(System.nanoTime() - startTime);
                                                       successCounter.incrementAndGet();
                                                   }

                                                   @Override
                                                   public void onError(Throwable t) {
                                                       dropCounter.incrementAndGet();
                                                   }

                                                   @Override
                                                   public void onNext(String value) {
                                                   }
                                               });
                }
            }
        }
    }

    public CompletableFuture<Void> runAsync() {
        return CompletableFuture.runAsync(this::run, Executors.newSingleThreadExecutor(Utils.virtualThreadFactory()));
    }
}
