/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc.resolver;

import java.util.Optional;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer.Context;
import com.salesfoce.apollo.stereotomy.event.proto.Binding;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.IdentifierContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.ResolverGrpc.ResolverImplBase;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;
import com.salesforce.apollo.stereotomy.services.proto.ProtoResolver;
import com.salesforce.apollo.utils.Utils;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class ResolverServer extends ResolverImplBase {
    private final static Logger log = LoggerFactory.getLogger(ResolverServer.class);

    private final StereotomyMetrics              metrics;
    private final RoutableService<ProtoResolver> routing;
    private final Executor                       exec;

    public ResolverServer(RoutableService<ProtoResolver> router, Executor exec, StereotomyMetrics metrics) {
        this.metrics = metrics;
        this.routing = router;
        this.exec = exec;
    }

    @Override
    public void lookup(IdentifierContext request, StreamObserver<Binding> responseObserver) {
        Context timer = metrics != null ? metrics.lookupService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundLookupRequest().mark(request.getSerializedSize());
        }
        exec.execute(Utils.wrapped(() -> routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            Optional<Binding> response = s.lookup(request.getIdentifier());
            if (response.isEmpty()) {
                if (timer != null) {
                    timer.stop();
                }
                responseObserver.onNext(Binding.getDefaultInstance());
                responseObserver.onCompleted();
                return;
            }

            if (timer != null) {
                timer.stop();
                metrics.outboundBandwidth().mark(response.get().getSerializedSize());
                metrics.outboundLookupResponse().mark(response.get().getSerializedSize());
            }
            responseObserver.onNext(response.get());
            responseObserver.onCompleted();
        }), log));
    }
}
