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
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.BindContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.BinderGrpc;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.BinderGrpc.BinderFutureStub;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.IdentifierContext;
import com.salesfoce.apollo.utils.proto.Digeste;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;

/**
 * @author hal.hildebrand
 *
 */
public class BinderClient implements BinderService {

    public static CreateClientCommunications<BinderService> getCreate(Digest context, StereotomyMetrics metrics) {
        return (t, f, c) -> {
            return new BinderClient(context, c, t, metrics);
        };

    }

    private final ManagedServerChannel channel;
    private final BinderFutureStub        client;
    private final Member                  member;
    private final StereotomyMetrics       metrics;
    private final Digeste                 context;

    public BinderClient(Digest context, ManagedServerChannel channel, Member member, StereotomyMetrics metrics) {
        this.context = context.toDigeste();
        this.member = member;
        this.channel = channel;
        this.client = BinderGrpc.newFutureStub(channel.channel).withCompression("gzip");
        this.metrics = metrics;
    }

    @Override
    public void close() {
        channel.release();
    }

    @Override
    public Member getMember() {
        return member;
    }

    @Override
    public CompletableFuture<Boolean> bind(com.salesfoce.apollo.stereotomy.event.proto.Binding binding) {
        Context timer = metrics == null ? null : metrics.bindClient().time();
        var request = BindContext.newBuilder().setContext(context).setBinding(binding).build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundBindRequest().mark(request.getSerializedSize());
        }
        CompletableFuture<Boolean> f = new CompletableFuture<>();
        ListenableFuture<Empty> result = client.bind(request);
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
    public CompletableFuture<Boolean> unbind(Ident identifier) {
        Context timer = metrics == null ? null : metrics.unbindClient().time();
        var request = IdentifierContext.newBuilder().setIdentifier(identifier).setContext(context).build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundUnbindRequest().mark(request.getSerializedSize());
        }
        CompletableFuture<Boolean> f = new CompletableFuture<>();
        ListenableFuture<Empty> result = client.unbind(request);
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
