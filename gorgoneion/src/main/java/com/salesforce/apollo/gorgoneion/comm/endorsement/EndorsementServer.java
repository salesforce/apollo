/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion.comm.endorsement;

import com.google.protobuf.Empty;
import com.salesfoce.apollo.gorgoneion.proto.Credentials;
import com.salesfoce.apollo.gorgoneion.proto.EndorsementGrpc.EndorsementImplBase;
import com.salesfoce.apollo.gorgoneion.proto.MemberSignature;
import com.salesfoce.apollo.gorgoneion.proto.Nonce;
import com.salesfoce.apollo.gorgoneion.proto.Notarization;
import com.salesfoce.apollo.stereotomy.event.proto.Validation_;
import com.salesforce.apollo.archipelago.RoutableService;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.gorgoneion.comm.GorgoneionMetrics;
import com.salesforce.apollo.protocols.ClientIdentity;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class EndorsementServer extends EndorsementImplBase {
    private ClientIdentity                            identity;
    private final GorgoneionMetrics                   metrics;
    private final RoutableService<EndorsementService> router;

    public EndorsementServer(ClientIdentity identity, RoutableService<EndorsementService> r,
                             GorgoneionMetrics metrics) {
        this.metrics = metrics;
        this.identity = identity;
        this.router = r;
    }

    @Override
    public void endorse(Nonce request, StreamObserver<MemberSignature> responseObserver) {
        var timer = metrics == null ? null : metrics.registerDuration().time();
        if (metrics != null) {
            var serializedSize = request.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundEndorse().update(serializedSize);
        }
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;
        }
        router.evaluate(responseObserver, s -> {
            s.endorse(request, from).whenComplete((v, t) -> {
                if (t != null) {
                    responseObserver.onError(t);
                } else {
                    responseObserver.onNext(v);
                    responseObserver.onCompleted();
                }
                if (timer != null) {
                    timer.close();
                }
            });
        });
    }

    @Override
    public void enroll(Notarization request, StreamObserver<Empty> responseObserver) {
        var timer = metrics == null ? null : metrics.enrollDuration().time();
        if (metrics != null) {
            var serializedSize = request.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundEnroll().update(serializedSize);
        }
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;
        }
        router.evaluate(responseObserver, s -> {
            s.enroll(request, from).whenComplete((e, t) -> {
                if (t != null) {
                    responseObserver.onError(t);
                } else {
                    responseObserver.onNext(e);
                    responseObserver.onCompleted();
                }
                if (timer != null) {
                    timer.close();
                }
            });
        });
    }

    @Override
    public void validate(Credentials request, StreamObserver<Validation_> responseObserver) {
        var timer = metrics == null ? null : metrics.registerDuration().time();
        if (metrics != null) {
            var serializedSize = request.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundValidateCredentials().update(serializedSize);
        }
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;
        }
        router.evaluate(responseObserver, s -> {
            s.validate(request, from).whenComplete((v, t) -> {
                if (t != null) {
                    responseObserver.onError(t);
                } else {
                    responseObserver.onNext(v);
                    responseObserver.onCompleted();
                }
                if (timer != null) {
                    timer.close();
                }
            });
        });
    }
}
