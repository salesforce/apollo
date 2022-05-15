/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.kairos.grpc;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.errorprone.annotations.DoNotCall;
import com.salesforce.apollo.kairos.Simulation;

import io.grpc.BinaryLog;
import io.grpc.ClientInterceptor;
import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import io.grpc.ManagedChannel;
import io.grpc.NameResolver.Factory;
import io.grpc.ProxyDetector;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessSocketAddress;
import io.grpc.internal.AbstractManagedChannelImplBuilder;

/**
 * A simulated GRPC server builder
 * 
 * @author hal.hildebrand
 *
 */
public class SimulatedManagedChannelBuilder extends AbstractManagedChannelImplBuilder<SimulatedManagedChannelBuilder> {
    /**
     * Create a channel builder that will connect to the server referenced by the
     * given address.
     *
     * @param address the address of the server to connect to
     * @return a new builder
     */
    public static SimulatedManagedChannelBuilder forAddress(SocketAddress address) {
        return new SimulatedManagedChannelBuilder(checkNotNull(address, "address"), null);
    }

    /**
     * Always fails. Call {@link #forName} instead.
     */
    @DoNotCall("Unsupported. Use forName() instead")
    public static SimulatedManagedChannelBuilder forAddress(String name, int port) {
        throw new UnsupportedOperationException("call forName() instead");
    }

    /**
     * Create a channel builder that will connect to the server with the given name.
     *
     * @param name the identity of the server to connect to
     * @return a new builder
     */
    public static SimulatedManagedChannelBuilder forName(String name) {
        return forAddress(new InProcessSocketAddress(checkNotNull(name, "name")));
    }

    /**
     * Create a channel builder that will connect to the server referenced by the
     * given target URI. Only intended for use with a custom name resolver.
     *
     * @param target the identity of the server to connect to
     * @return a new builder
     */
    public static SimulatedManagedChannelBuilder forTarget(String target) {
        return new SimulatedManagedChannelBuilder(null, checkNotNull(target, "target"));
    }

    private final InProcessChannelBuilder concrete;
    private Simulation                    simulation;

    public SimulatedManagedChannelBuilder simulation(Simulation simulation) {
        checkNotNull(simulation, "simulation");
        this.simulation = simulation;
        return this;
    }

    private SimulatedManagedChannelBuilder(@Nullable SocketAddress directAddress, @Nullable String target) {
        concrete = directAddress == null ? InProcessChannelBuilder.forAddress(directAddress)
                                         : InProcessChannelBuilder.forTarget(target);
    }

    @Override
    public ManagedChannel build() {
        checkNotNull(simulation, "simulation");
        concrete.executor(simulation.getScheduler());
        concrete.scheduledExecutorService(simulation.getScheduler());
        concrete.offloadExecutor(simulation.getScheduler());
        return super.build();
    }

    @Override
    public SimulatedManagedChannelBuilder compressorRegistry(CompressorRegistry registry) {
        concrete.compressorRegistry(registry);
        return this;
    }

    @Override
    public SimulatedManagedChannelBuilder decompressorRegistry(DecompressorRegistry registry) {
        concrete.decompressorRegistry(registry);
        return this;
    }

    @Override
    public SimulatedManagedChannelBuilder defaultLoadBalancingPolicy(String policy) {
        concrete.defaultLoadBalancingPolicy(policy);
        return this;
    }

    @Override
    public SimulatedManagedChannelBuilder defaultServiceConfig(Map<String, ?> serviceConfig) {
        concrete.defaultServiceConfig(serviceConfig);
        return this;
    }

    @Override
    public SimulatedManagedChannelBuilder directExecutor() {
        return this;
    }

    @Override
    public SimulatedManagedChannelBuilder disableRetry() {
        concrete.disableRetry();
        return this;
    }

    @Override
    public SimulatedManagedChannelBuilder disableServiceConfigLookUp() {
        concrete.disableServiceConfigLookUp();
        return this;
    }

    @Override
    public SimulatedManagedChannelBuilder enableFullStreamDecompression() {
        concrete.enableFullStreamDecompression();
        return this;
    }

    @Override
    public SimulatedManagedChannelBuilder enableRetry() {
        concrete.enableRetry();
        return this;
    }

    @Override
    public SimulatedManagedChannelBuilder executor(Executor executor) {
        return this;
    }

    @Override
    public SimulatedManagedChannelBuilder idleTimeout(long value, TimeUnit unit) {
        concrete.idleTimeout(value, unit);
        return this;
    }

    @Override
    public SimulatedManagedChannelBuilder intercept(ClientInterceptor... interceptors) {
        concrete.intercept(interceptors);
        return this;
    }

    @Override
    public SimulatedManagedChannelBuilder intercept(List<ClientInterceptor> interceptors) {
        concrete.intercept(interceptors);
        return this;
    }

    @Override
    public SimulatedManagedChannelBuilder keepAliveTime(long keepAliveTime, TimeUnit timeUnit) {
        concrete.keepAliveTime(keepAliveTime, timeUnit);
        return this;
    }

    @Override
    public SimulatedManagedChannelBuilder keepAliveTimeout(long keepAliveTimeout, TimeUnit timeUnit) {
        concrete.keepAliveTimeout(keepAliveTimeout, timeUnit);
        return this;
    }

    @Override
    public SimulatedManagedChannelBuilder keepAliveWithoutCalls(boolean enable) {
        concrete.keepAliveWithoutCalls(enable);
        return this;
    }

    @Override
    public SimulatedManagedChannelBuilder maxHedgedAttempts(int maxHedgedAttempts) {
        concrete.maxHedgedAttempts(maxHedgedAttempts);
        return this;
    }

    @Override
    public SimulatedManagedChannelBuilder maxInboundMessageSize(int max) {
        concrete.maxInboundMessageSize(max);
        return this;
    }

    @Override
    public SimulatedManagedChannelBuilder maxInboundMetadataSize(int bytes) {
        concrete.maxInboundMetadataSize(bytes);
        return this;
    }

    @Override
    public SimulatedManagedChannelBuilder maxRetryAttempts(int maxRetryAttempts) {
        concrete.maxRetryAttempts(maxRetryAttempts);
        return this;
    }

    @Override
    public SimulatedManagedChannelBuilder maxTraceEvents(int maxTraceEvents) {
        concrete.maxTraceEvents(maxTraceEvents);
        return this;
    }

    @SuppressWarnings("deprecation")
    @Override
    public SimulatedManagedChannelBuilder nameResolverFactory(Factory resolverFactory) {
        concrete.nameResolverFactory(resolverFactory);
        return this;
    }

    @Override
    public SimulatedManagedChannelBuilder offloadExecutor(Executor executor) {
        return this;
    }

    @Override
    public SimulatedManagedChannelBuilder overrideAuthority(String authority) {
        concrete.overrideAuthority(authority);
        return this;
    }

    @Override
    public SimulatedManagedChannelBuilder perRpcBufferLimit(long bytes) {
        concrete.perRpcBufferLimit(bytes);
        return this;
    }

    public SimulatedManagedChannelBuilder propagateCauseWithStatus(boolean enable) {
        concrete.propagateCauseWithStatus(enable);
        return this;
    }

    @Override
    public SimulatedManagedChannelBuilder proxyDetector(ProxyDetector proxyDetector) {
        concrete.proxyDetector(proxyDetector);
        return this;
    }

    @Override
    public SimulatedManagedChannelBuilder retryBufferSize(long bytes) {
        concrete.retryBufferSize(bytes);
        return this;
    }

    public SimulatedManagedChannelBuilder scheduledExecutorService(ScheduledExecutorService scheduledExecutorService) {
        concrete.scheduledExecutorService(scheduledExecutorService);
        return this;
    }

    @Override
    public SimulatedManagedChannelBuilder setBinaryLog(BinaryLog binaryLog) {
        concrete.setBinaryLog(binaryLog);
        return this;
    }

    @Override
    public SimulatedManagedChannelBuilder usePlaintext() {
        concrete.usePlaintext();
        return this;
    }

    @Override
    public SimulatedManagedChannelBuilder userAgent(String userAgent) {
        concrete.userAgent(userAgent);
        return this;
    }

    @Override
    public SimulatedManagedChannelBuilder useTransportSecurity() {
        concrete.useTransportSecurity();
        return this;
    }

    @Override
    protected SimulatedManagedChannelBuilder delegate() {
        return this;
    }

}
