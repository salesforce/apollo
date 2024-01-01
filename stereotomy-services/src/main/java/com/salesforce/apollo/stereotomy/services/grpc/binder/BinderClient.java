/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc.binder;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.codahale.metrics.Timer.Context;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.salesforce.apollo.stereotomy.event.proto.Ident;
import com.salesforce.apollo.stereotomy.services.grpc.proto.BinderGrpc;
import com.salesforce.apollo.stereotomy.services.grpc.proto.BinderGrpc.BinderFutureStub;
import com.salesforce.apollo.archipelago.ManagedServerChannel;
import com.salesforce.apollo.archipelago.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;

/**
 * @author hal.hildebrand
 */
public class BinderClient implements BinderService {

    private final ManagedServerChannel channel;
    private final BinderFutureStub     client;
    private final StereotomyMetrics    metrics;
    public BinderClient(ManagedServerChannel channel, StereotomyMetrics metrics) {
        this.channel = channel;
        this.client = BinderGrpc.newFutureStub(channel).withCompression("gzip");
        this.metrics = metrics;
    }

    public static CreateClientCommunications<BinderService> getCreate(StereotomyMetrics metrics) {
        return (c) -> {
            return new BinderClient(c, metrics);
        };

    }

    @Override
    public CompletableFuture<Boolean> bind(com.salesforce.apollo.stereotomy.event.proto.Binding binding) {
        Context timer = metrics == null ? null : metrics.bindClient().time();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(binding.getSerializedSize());
            metrics.outboundBindRequest().mark(binding.getSerializedSize());
        }
        CompletableFuture<Boolean> f = new CompletableFuture<>();
        ListenableFuture<Empty> result = client.bind(binding);
        result.addListener(() -> {
            try {
                result.get();
            } catch (InterruptedException e) {
                f.completeExceptionally(e);
                return;
            } catch (ExecutionException e) {
                f.completeExceptionally(e.getCause());
                return;
            }
            if (timer != null) {
                timer.stop();
            }
            f.complete(true);
        }, r -> r.run());
        return f;
    }

    @Override
    public void close() {
        channel.release();
    }

    @Override
    public Member getMember() {
        return channel.getMember();
    }

    @Override
    public CompletableFuture<Boolean> unbind(Ident identifier) {
        Context timer = metrics == null ? null : metrics.unbindClient().time();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(identifier.getSerializedSize());
            metrics.outboundUnbindRequest().mark(identifier.getSerializedSize());
        }
        CompletableFuture<Boolean> f = new CompletableFuture<>();
        ListenableFuture<Empty> result = client.unbind(identifier);
        result.addListener(() -> {
            try {
                result.get();
            } catch (InterruptedException e) {
                f.completeExceptionally(e);
                return;
            } catch (ExecutionException e) {
                f.completeExceptionally(e.getCause());
                return;
            }
            if (timer != null) {
                timer.stop();
            }
            f.complete(true);
        }, r -> r.run());
        return f;
    }

}
