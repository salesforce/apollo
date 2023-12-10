package com.salesforce.apollo.thoth.grpc;

import com.google.protobuf.Empty;
import com.salesforce.apollo.stereotomy.event.proto.EventCoords;
import com.salesforce.apollo.stereotomy.event.proto.Ident;
import com.salesforce.apollo.stereotomy.event.proto.InceptionEvent;
import com.salesforce.apollo.stereotomy.event.proto.RotationEvent;
import com.salesforce.apollo.thoth.proto.Thoth_Grpc;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.protobuf.InceptionEventImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.RotationEventImpl;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification;
import com.salesforce.apollo.thoth.Thoth;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hal.hildebrand
 **/
public class ThothServer extends Thoth_Grpc.Thoth_ImplBase {
    private static final Logger                                                    log = LoggerFactory.getLogger(
    ThothServer.class);
    private final        IdentifierSpecification.Builder<SelfAddressingIdentifier> inception;
    private final        RotationSpecification.Builder                             rotation;
    private final        Thoth                                                     thoth;

    public ThothServer(Thoth thoth) {
        this(IdentifierSpecification.newBuilder(), RotationSpecification.newBuilder(), thoth);
    }

    public ThothServer(IdentifierSpecification.Builder<SelfAddressingIdentifier> inception,
                       RotationSpecification.Builder rotation, Thoth thoth) {
        this.inception = inception;
        this.rotation = rotation;
        this.thoth = thoth;
    }

    @Override
    public void commit(EventCoords request, StreamObserver<Empty> responseObserver) {
        var from = EventCoordinates.from(request);
        try {
            thoth.commit(from);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Throwable t) {
            log.info("Error committing delegation event: " + from, t);
            responseObserver.onError(t);
        }
    }

    public Thoth getThoth() {
        return thoth;
    }

    @Override
    public void identifier(Empty request, StreamObserver<Ident> responseObserver) {
        try {
            var ident = thoth.identifier().toIdent();
            responseObserver.onNext(ident);
            responseObserver.onCompleted();
        } catch (Throwable t) {
            log.info("Error getting identifier", t);
            responseObserver.onError(t);
        }
    }

    @Override
    public void inception(Ident request, StreamObserver<InceptionEvent> responseObserver) {
        try {
            var i = Identifier.from(request);
            if (i instanceof SelfAddressingIdentifier sai) {
                var incep = thoth.inception(sai, inception);
                if (incep instanceof InceptionEventImpl incp) {
                    responseObserver.onNext(incp.toInceptionEvent_());
                    responseObserver.onCompleted();
                } else {
                    log.info("Not an inception event impl: {}", incep);
                    responseObserver.onError(new IllegalArgumentException("Not an inception event: " + incep));
                }
            } else {
                log.info("Not a SelfAddressingIdentifier: {}", i);
                responseObserver.onError(new IllegalArgumentException("Not a SelfAddressingIdentifier: " + i));
            }
        } catch (Throwable t) {
            log.info("Error creating inception event", t);
            responseObserver.onError(t);
        }
    }

    @Override
    public void rotate(Empty request, StreamObserver<RotationEvent> responseObserver) {
        try {
            var rot = thoth.rotate(rotation);
            if (rot instanceof RotationEventImpl incp) {
                responseObserver.onNext(incp.toRotationEvent_());
                responseObserver.onCompleted();
            } else {
                log.info("Not a rotation event impl: {}", rot);
                responseObserver.onError(new IllegalArgumentException("Not a rotation event: " + rot));
            }
        } catch (Throwable t) {
            log.info("Error rotating identifier", t);
            responseObserver.onError(t);
        }
    }
}
