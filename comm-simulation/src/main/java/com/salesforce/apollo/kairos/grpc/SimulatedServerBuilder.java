/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.kairos.grpc;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.InputStream;
import java.net.SocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.salesforce.apollo.kairos.Simulation;

import io.grpc.BinaryLog;
import io.grpc.BindableService;
import io.grpc.CompressorRegistry;
import io.grpc.Deadline.Ticker;
import io.grpc.DecompressorRegistry;
import io.grpc.HandlerRegistry;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallExecutorSupplier;
import io.grpc.ServerInterceptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServerStreamTracer.Factory;
import io.grpc.ServerTransportFilter;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.internal.AbstractServerImplBuilder;

/**
 * @author hal.hildebrand
 *
 */
public class SimulatedServerBuilder extends AbstractServerImplBuilder<SimulatedServerBuilder> {
    private final InProcessServerBuilder concrete;
    private Simulation                   simulation;

    private SimulatedServerBuilder(SocketAddress listenAddress) {
        concrete = InProcessServerBuilder.forAddress(listenAddress);
    }

    @Override
    public SimulatedServerBuilder addService(BindableService bindableService) {
        concrete.addService(bindableService);
        return this;
    }

    @Override
    public SimulatedServerBuilder addService(ServerServiceDefinition service) {
        concrete.addService(service);
        return this;
    }

    @Override
    public SimulatedServerBuilder addStreamTracerFactory(Factory factory) {
        concrete.addStreamTracerFactory(factory);
        return this;
    }

    @Override
    public SimulatedServerBuilder addTransportFilter(ServerTransportFilter filter) {
        concrete.addTransportFilter(filter);
        return this;
    }

    @Override
    public Server build() {
        checkNotNull(simulation, "simulation");
        concrete.executor(simulation.getScheduler());
        concrete.scheduledExecutorService(simulation.getScheduler());
        concrete.callExecutor(new ServerCallExecutorSupplier() {
            @Override
            public <ReqT, RespT> Executor getExecutor(ServerCall<ReqT, RespT> call, Metadata metadata) {
                return simulation.getScheduler();
            }
        });
        return concrete.build();
    }

    @Override
    public SimulatedServerBuilder callExecutor(ServerCallExecutorSupplier executorSupplier) {
        return this;
    }

    @Override
    public SimulatedServerBuilder compressorRegistry(CompressorRegistry registry) {
        concrete.compressorRegistry(registry);
        return this;
    }

    public SimulatedServerBuilder deadlineTicker(Ticker ticker) {
        concrete.deadlineTicker(ticker);
        return this;
    }

    @Override
    public SimulatedServerBuilder decompressorRegistry(DecompressorRegistry registry) {
        concrete.decompressorRegistry(registry);
        return this;
    }

    @Override
    public SimulatedServerBuilder directExecutor() {
        return this;
    }

    @Override
    public SimulatedServerBuilder executor(Executor executor) {
        return this;
    }

    @Override
    public SimulatedServerBuilder fallbackHandlerRegistry(HandlerRegistry fallbackRegistry) {
        concrete.fallbackHandlerRegistry(fallbackRegistry);
        return this;
    }

    @Override
    public SimulatedServerBuilder handshakeTimeout(long timeout, TimeUnit unit) {
        concrete.handshakeTimeout(timeout, unit);
        return this;
    }

    @Override
    public SimulatedServerBuilder intercept(ServerInterceptor interceptor) {
        concrete.intercept(interceptor);
        return this;
    }

    @Override
    public SimulatedServerBuilder maxInboundMessageSize(int bytes) {
        concrete.maxInboundMessageSize(bytes);
        return this;
    }

    @Override
    public SimulatedServerBuilder maxInboundMetadataSize(int bytes) {
        concrete.maxInboundMetadataSize(bytes);
        return this;
    }

    public SimulatedServerBuilder scheduledExecutorService(ScheduledExecutorService scheduledExecutorService) {
        concrete.scheduledExecutorService(scheduledExecutorService);
        return this;
    }

    @Override
    public SimulatedServerBuilder setBinaryLog(BinaryLog binaryLog) {
        concrete.setBinaryLog(binaryLog);
        return this;
    }

    public SimulatedServerBuilder simulation(Simulation simulation) {
        this.simulation = simulation;
        return this;
    }

    @Override
    public SimulatedServerBuilder useTransportSecurity(File certChain, File privateKey) {
        concrete.useTransportSecurity(certChain, privateKey);
        return this;
    }

    @Override
    public SimulatedServerBuilder useTransportSecurity(InputStream certChain, InputStream privateKey) {
        concrete.useTransportSecurity(certChain, privateKey);
        return this;
    }

    @Override
    protected SimulatedServerBuilder delegate() {
        return this;
    }

}
