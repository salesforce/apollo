/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.communications;

import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer.Context;
import com.salesfoce.apollo.fireflies.proto.Credentials;
import com.salesfoce.apollo.fireflies.proto.EntranceGrpc.EntranceImplBase;
import com.salesfoce.apollo.fireflies.proto.Gateway;
import com.salesfoce.apollo.fireflies.proto.Redirect;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.fireflies.FireflyMetrics;
import com.salesforce.apollo.fireflies.View.Service;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.utils.Utils;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class EntranceServer extends EntranceImplBase {
    private final static Logger log = LoggerFactory.getLogger(FfServer.class);

    private final Executor exec;

    private ClientIdentity                 identity;
    private final FireflyMetrics           metrics;
    private final RoutableService<Service> router;

    public EntranceServer(Service system, ClientIdentity identity, RoutableService<Service> router, Executor exec,
                          FireflyMetrics metrics) {
        this.metrics = metrics;
        this.identity = identity;
        this.router = router;
        this.exec = exec;
    }

    @Override
    public void join(Credentials request, StreamObserver<Gateway> responseObserver) {
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
        router.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            // async handling
            s.join(request, from, responseObserver, timer);
        });
    }

    @Override
    public void seed(Credentials request, StreamObserver<Redirect> responseObserver) {
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
        exec.execute(Utils.wrapped(() -> router.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            var redirect = s.seed(request, from);
            responseObserver.onNext(redirect);
            responseObserver.onCompleted();
            if (timer != null) {
                var serializedSize = redirect.getSerializedSize();
                metrics.outboundBandwidth().mark(serializedSize);
                metrics.outboundRedirect().update(serializedSize);
                timer.stop();
            }
        }), log));
    }
}
