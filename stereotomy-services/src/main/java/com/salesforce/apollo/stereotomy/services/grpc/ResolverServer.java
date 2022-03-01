/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc;

import java.util.Optional;

import com.codahale.metrics.Timer.Context;
import com.salesfoce.apollo.stereotomy.event.proto.Binding;
import com.salesfoce.apollo.stereotomy.event.proto.KERL;
import com.salesfoce.apollo.stereotomy.event.proto.KeyState;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.EventContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.IdentifierContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.ResolverGrpc.ResolverImplBase;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.stereotomy.services.ProtoResolverService;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class ResolverServer extends ResolverImplBase {
    private ClientIdentity                              identity;
    private final StereotomyMetrics                     metrics;
    private final RoutableService<ProtoResolverService> routing;

    public ResolverServer(ClientIdentity identity, StereotomyMetrics metrics,
                          RoutableService<ProtoResolverService> router) {
        this.metrics = metrics;
        this.identity = identity;
        this.routing = router;
    }

    @Override
    public void kerl(IdentifierContext request, StreamObserver<KERL> responseObserver) {
        Context timer = metrics != null ? metrics.kerlService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundKerlRequest().mark(request.getSerializedSize());
        }
        routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            Digest from = identity.getFrom();
            if (from == null) {
                responseObserver.onError(new IllegalStateException("Member has been removed"));
                return;
            }
            Optional<KERL> response = s.kerl(request.getIdentifier());
            if (response.isEmpty()) {
                if (timer != null) {
                    timer.stop();
                }
                responseObserver.onNext(KERL.getDefaultInstance());
                responseObserver.onCompleted();
            }

            if (timer != null) {
                timer.stop();
                metrics.outboundBandwidth().mark(response.get().getSerializedSize());
                metrics.outboundKerlResponse().mark(response.get().getSerializedSize());
            }
            responseObserver.onNext(response.get());
            responseObserver.onCompleted();
        });
    }

    @Override
    public void lookup(IdentifierContext request, StreamObserver<Binding> responseObserver) {
        Context timer = metrics != null ? metrics.lookupService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundLookupRequest().mark(request.getSerializedSize());
        }
        routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            Digest from = identity.getFrom();
            if (from == null) {
                responseObserver.onError(new IllegalStateException("Member has been removed"));
                return;
            }
            Optional<Binding> response = s.lookup(request.getIdentifier());
            if (response.isEmpty()) {
                if (timer != null) {
                    timer.stop();
                }
                responseObserver.onNext(Binding.getDefaultInstance());
                responseObserver.onCompleted();
            }

            if (timer != null) {
                timer.stop();
                metrics.outboundBandwidth().mark(response.get().getSerializedSize());
                metrics.outboundLookupResponse().mark(response.get().getSerializedSize());
            }
            responseObserver.onNext(response.get());
            responseObserver.onCompleted();
        });
    }

    @Override
    public void resolve(IdentifierContext request, StreamObserver<KeyState> responseObserver) {
        Context timer = metrics != null ? metrics.resolveService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundResolveRequest().mark(request.getSerializedSize());
        }
        routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            Digest from = identity.getFrom();
            if (from == null) {
                responseObserver.onError(new IllegalStateException("Member has been removed"));
                return;
            }
            Optional<KeyState> response = s.resolve(request.getIdentifier());
            if (response.isEmpty()) {
                if (timer != null) {
                    timer.stop();
                }
                responseObserver.onNext(KeyState.getDefaultInstance());
                responseObserver.onCompleted();
            }

            if (timer != null) {
                timer.stop();
                metrics.outboundBandwidth().mark(response.get().getSerializedSize());
                metrics.outboundResolveResponse().mark(response.get().getSerializedSize());
            }
            responseObserver.onNext(response.get());
            responseObserver.onCompleted();
        });

    }

    @Override
    public void resolveCoords(EventContext request, StreamObserver<KeyState> responseObserver) {
        Context timer = metrics != null ? metrics.resolveCoordsService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundResolveCoodsRequest().mark(request.getSerializedSize());
        }
        routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            Digest from = identity.getFrom();
            if (from == null) {
                responseObserver.onError(new IllegalStateException("Member has been removed"));
                return;
            }
            Optional<KeyState> response = s.resolve(request.getCoordinates());
            if (response.isEmpty()) {
                if (timer != null) {
                    timer.stop();
                }
                responseObserver.onNext(KeyState.getDefaultInstance());
                responseObserver.onCompleted();
            }

            if (timer != null) {
                timer.stop();
                metrics.outboundBandwidth().mark(response.get().getSerializedSize());
                metrics.outboundResolveCoordsResponse().mark(response.get().getSerializedSize());
            }
            responseObserver.onNext(response.get());
            responseObserver.onCompleted();
        });
    }

}
