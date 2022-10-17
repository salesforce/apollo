/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc.validation;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.codahale.metrics.Timer.Context;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.BoolValue;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KeyEventContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.ValidatorGrpc;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.ValidatorGrpc.ValidatorFutureStub;
import com.salesforce.apollo.archipelago.ManagedServerChannel;
import com.salesforce.apollo.archipelago.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;
import com.salesforce.apollo.stereotomy.services.proto.ProtoEventValidation;

/**
 * @author hal.hildebrand
 *
 */
public class EventValidationClient implements EventValidationService {

    public static CreateClientCommunications<EventValidationService> getCreate(StereotomyMetrics metrics) {
        return (c) -> {
            return new EventValidationClient(c, metrics);
        };

    }

    public static EventValidationService getLocalLoopback(ProtoEventValidation service, Member member) {
        return new EventValidationService() {

            @Override
            public void close() throws IOException {
            }

            @Override
            public Member getMember() {
                return member;
            }

            @Override
            public CompletableFuture<Boolean> validate(KeyEvent_ event) {
                return service.validate(event);
            }
        };
    }

    private final ManagedServerChannel channel;
    private final ValidatorFutureStub  client;
    private final StereotomyMetrics    metrics;

    public EventValidationClient(ManagedServerChannel channel, StereotomyMetrics metrics) {
        this.channel = channel;
        this.client = ValidatorGrpc.newFutureStub(channel).withCompression("gzip");
        this.metrics = metrics;
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
    public CompletableFuture<Boolean> validate(KeyEvent_ event) {
        Context timer = metrics == null ? null : metrics.validatorClient().time();
        var request = KeyEventContext.newBuilder().setKeyEvent(event).build();
        if (metrics != null) {
            metrics.outboundBandwidth().mark(request.getSerializedSize());
            metrics.outboundValidatorRequest().mark(request.getSerializedSize());
        }
        CompletableFuture<Boolean> f = new CompletableFuture<>();
        ListenableFuture<BoolValue> result = client.validate(request);
        result.addListener(() -> {
            boolean success;
            try {
                success = result.get().getValue();
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
            f.complete(success);
        }, r -> r.run());
        return f;
    }

}
