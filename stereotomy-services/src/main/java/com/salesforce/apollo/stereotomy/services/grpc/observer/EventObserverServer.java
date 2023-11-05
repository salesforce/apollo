/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc.observer;

import com.codahale.metrics.Timer.Context;
import com.google.protobuf.Empty;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.AttachmentsContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.EventObserverGrpc.EventObserverImplBase;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KERLContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KeyEventsContext;
import com.salesforce.apollo.archipelago.RoutableService;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;
import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 */
public class EventObserverServer extends EventObserverImplBase {

    private final ClientIdentity identity;
    private final StereotomyMetrics metrics;
    private final RoutableService<EventObserver> routing;

    public EventObserverServer(RoutableService<EventObserver> router, ClientIdentity identity,
                               StereotomyMetrics metrics) {
        this.metrics = metrics;
        this.routing = router;
        this.identity = identity;
    }

    @Override
    public void publish(KERLContext request, StreamObserver<Empty> responseObserver) {
        Context timer = metrics != null ? metrics.publishKERLService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundPublishKERLRequest().mark(request.getSerializedSize());
        }
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;

        }
        routing.evaluate(responseObserver, s -> {
            s.publish(request.getKerl(), request.getValidationsList(), from);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        });
    }

    @Override
    public void publishAttachments(AttachmentsContext request, StreamObserver<Empty> responseObserver) {
        Context timer = metrics != null ? metrics.publishAttachmentsService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundPublishAttachmentsRequest().mark(request.getSerializedSize());
        }
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;

        }
        routing.evaluate(responseObserver, s -> {
            s.publishAttachments(request.getAttachmentsList(), from);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        });
    }

    @Override
    public void publishEvents(KeyEventsContext request, StreamObserver<Empty> responseObserver) {
        Context timer = metrics != null ? metrics.publishEventsService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundPublishEventsRequest().mark(request.getSerializedSize());
        }
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;

        }
        routing.evaluate(responseObserver, s -> {
            s.publishEvents(request.getKeyEventList(), request.getValidationsList(), from);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        });
    }
}
