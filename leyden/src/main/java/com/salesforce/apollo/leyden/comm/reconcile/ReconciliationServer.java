package com.salesforce.apollo.leyden.comm.reconcile;

import com.codahale.metrics.Timer;
import com.google.protobuf.Empty;
import com.salesforce.apollo.archipelago.RoutableService;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.protocols.ClientIdentity;
import com.salesforce.apollo.thoth.proto.Intervals;
import com.salesforce.apollo.thoth.proto.ReconciliationGrpc;
import com.salesforce.apollo.thoth.proto.Update;
import com.salesforce.apollo.thoth.proto.Updating;
import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 **/
public class ReconciliationServer extends ReconciliationGrpc.ReconciliationImplBase {
    private final RoutableService<ReconciliationService> routing;
    private final ReconciliationMetrics                  metrics;
    private final ClientIdentity                         identity;

    public ReconciliationServer(RoutableService<ReconciliationService> r, ClientIdentity identity,
                                ReconciliationMetrics metrics) {
        this.routing = r;
        this.identity = identity;
        this.metrics = metrics;
    }

    @Override
    public void reconcile(Intervals request, StreamObserver<Update> responseObserver) {
        Timer.Context timer = metrics == null ? null : metrics.inboundReconcileTimer().time();
        if (metrics != null) {
            var serializedSize = request.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundReconcile().update(serializedSize);
        }
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;
        }
        routing.evaluate(responseObserver, s -> {
            try {
                Update response = s.reconcile(request, from);
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                if (metrics != null) {
                    var serializedSize = response.getSerializedSize();
                    metrics.outboundBandwidth().mark(serializedSize);
                    metrics.reconcileReply().update(serializedSize);
                }
            } finally {
                if (timer != null) {
                    timer.stop();
                }
            }
        });
    }

    @Override
    public void update(Updating request, StreamObserver<Empty> responseObserver) {
        Timer.Context timer = metrics == null ? null : metrics.inboundUpdateTimer().time();
        if (metrics != null) {
            var serializedSize = request.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundReconcile().update(serializedSize);
        }
        Digest from = identity.getFrom();
        if (from == null) {
            responseObserver.onError(new IllegalStateException("Member has been removed"));
            return;
        }
        routing.evaluate(responseObserver, s -> {
            try {
                s.update(request, from);
                responseObserver.onNext(Empty.getDefaultInstance());
                responseObserver.onCompleted();
            } finally {
                if (timer != null) {
                    timer.stop();
                }
            }
        });
    }
}
