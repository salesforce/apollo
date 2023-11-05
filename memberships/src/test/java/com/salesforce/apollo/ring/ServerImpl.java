package com.salesforce.apollo.ring;

import com.google.protobuf.Any;
import com.salesfoce.apollo.test.proto.TestItGrpc;
import com.salesforce.apollo.archipelago.RoutableService;
import io.grpc.stub.StreamObserver;

public class ServerImpl extends TestItGrpc.TestItImplBase {
    private final RoutableService<TestIt> router;

    public ServerImpl(RoutableService<TestIt> router) {
        this.router = router;
    }

    @Override
    public void ping(Any request, StreamObserver<Any> responseObserver) {
        router.evaluate(responseObserver, t -> t.ping(request, responseObserver));
    }
}