package com.salesforce.apollo.ring;

import com.google.protobuf.Any;
import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 **/
public interface TestIt {
    void ping(Any request, StreamObserver<Any> responseObserver);
}
