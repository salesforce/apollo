/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.comm.entrance;

import com.codahale.metrics.Timer.Context;
import com.salesforce.apollo.fireflies.proto.EntranceGrpc.EntranceImplBase;
import com.salesforce.apollo.fireflies.proto.Gateway;
import com.salesforce.apollo.fireflies.proto.Join;
import com.salesforce.apollo.fireflies.proto.Redirect;
import com.salesforce.apollo.fireflies.proto.Registration;
import com.salesforce.apollo.archipelago.RoutableService;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.fireflies.FireflyMetrics;
import com.salesforce.apollo.fireflies.View.Service;
import com.salesforce.apollo.protocols.ClientIdentity;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 */
public class EntranceServer extends EntranceImplBase {

    private final FireflyMetrics           metrics;
    private final RoutableService<Service> router;
    private       ClientIdentity           identity;

    public EntranceServer(ClientIdentity identity, RoutableService<Service> r, FireflyMetrics metrics) {
        this.metrics = metrics;
        this.identity = identity;
        this.router = r;
    }

    @Override
    public void join(Join request, StreamObserver<Gateway> responseObserver) {
        Context timer = metrics == null ? null : metrics.inboundJoinDuration().time();
        if (metrics != null) {
            var serializedSize = request.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundJoin().update(serializedSize);
        }
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;
        }
        router.evaluate(responseObserver, s -> {
            // async handling
            s.join(request, from, responseObserver, timer);
        });
    }

    @Override
    public void seed(Registration request, StreamObserver<Redirect> responseObserver) {
        Context timer = metrics == null ? null : metrics.inboundSeedDuration().time();
        if (metrics != null) {
            var serializedSize = request.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundSeed().update(serializedSize);
        }
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;
        }
        router.evaluate(responseObserver, s -> {
            var r = s.seed(request, from);
            responseObserver.onNext(r);
            responseObserver.onCompleted();
            if (timer != null) {
                var serializedSize = r.getSerializedSize();
                metrics.outboundBandwidth().mark(serializedSize);
                metrics.outboundRedirect().update(serializedSize);
                timer.stop();
            }
        });
    }
}
