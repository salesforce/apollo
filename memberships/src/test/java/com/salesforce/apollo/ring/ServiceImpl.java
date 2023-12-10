package com.salesforce.apollo.ring;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.salesforce.apollo.test.proto.ByteMessage;
import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 **/
public class ServiceImpl implements TestIt {
    private final TestItService local;
    private final String        response;

    public ServiceImpl(TestItService local, String response) {
        this.local = local;
        this.response = response;
    }

    @Override
    public void ping(Any request, StreamObserver<Any> responseObserver) {
        local.ping(request);
        responseObserver.onNext(
        Any.pack(ByteMessage.newBuilder().setContents(ByteString.copyFromUtf8(response)).build()));
        responseObserver.onCompleted();
    }
}
