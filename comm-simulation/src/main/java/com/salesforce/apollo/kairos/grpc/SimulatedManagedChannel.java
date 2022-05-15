/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.kairos.grpc;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.MoreObjects;
import com.salesforce.apollo.kairos.KairosFuture;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;

/**
 * @author hal.hildebrand
 *
 */
abstract public class SimulatedManagedChannel extends ManagedChannel {
    protected final ManagedChannel concrete;

    public SimulatedManagedChannel(ManagedChannel concrete) {
        this.concrete = concrete;
    }

    @Override
    public String authority() {
        return concrete.authority();
    }

    @Override
    public final boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        try {
            return scheduleAwaitTermination().get();
        } catch (ExecutionException e) {
            throw new IllegalStateException("Error in simulation", e.getCause());
        }
    }

    @Override
    public boolean isShutdown() {
        return concrete.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return concrete.isTerminated();
    }

    @SuppressWarnings("unchecked")
    @Override
    public final <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(MethodDescriptor<RequestT, ResponseT> methodDescriptor,
                                                                               CallOptions callOptions) {
        try {
            return (ClientCall<RequestT, ResponseT>) scheduleNewCall().get();
        } catch (ExecutionException e) {
            throw new IllegalStateException("Error in simulation", e.getCause());
        }
    }

    @Override
    public final ManagedChannel shutdown() {
        try {
            return scheduleShutdown().get();
        } catch (ExecutionException e) {
            throw new IllegalStateException("Error in simulation", e.getCause());
        }
    }

    @Override
    public final ManagedChannel shutdownNow() {
        try {
            return scheduleShutdownNow().get();
        } catch (ExecutionException e) {
            throw new IllegalStateException("Error in simulation", e.getCause());
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("concrete", concrete).toString();
    }

    protected abstract KairosFuture<Boolean> scheduleAwaitTermination();

    protected abstract KairosFuture<ClientCall<?, ?>> scheduleNewCall();

    protected abstract KairosFuture<ManagedChannel> scheduleShutdown();

    protected abstract KairosFuture<ManagedChannel> scheduleShutdownNow();
}
