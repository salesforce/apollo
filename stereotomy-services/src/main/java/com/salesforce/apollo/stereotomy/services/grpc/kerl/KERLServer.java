/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc.kerl;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.codahale.metrics.Timer.Context;
import com.salesfoce.apollo.stereotomy.event.proto.Attachment;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyState_;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.EventContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.EventDigestContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.IdentifierContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KERLContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KERLServiceGrpc.KERLServiceImplBase;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KeyEventWitAttachmentsContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KeyEventsContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KeyStates;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLService;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class KERLServer extends KERLServiceImplBase {

    private final StereotomyMetrics metrics;

    private final RoutableService<ProtoKERLService> routing;

    public KERLServer(StereotomyMetrics metrics, RoutableService<ProtoKERLService> router) {
        this.metrics = metrics;
        this.routing = router;
    }

    @Override
    public void append(KeyEventsContext request, StreamObserver<KeyStates> responseObserver) {
        Context timer = metrics != null ? metrics.appendEventsService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundAppendEventsRequest().mark(request.getSerializedSize());
        }
        routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            CompletableFuture<List<KeyState_>> result = s.append(request.getKeyEventList());
            if (result == null) {
                responseObserver.onNext(KeyStates.getDefaultInstance());
                responseObserver.onCompleted();
            } else {
                result.whenComplete((ks, t) -> {
                    if (timer != null) {
                        timer.stop();
                    }
                    if (t != null) {
                        responseObserver.onError(t);
                    } else {
                        var states = KeyStates.newBuilder().addAllKeyStates(ks).build();
                        responseObserver.onNext(states);
                        responseObserver.onCompleted();
                        metrics.outboundBandwidth().mark(states.getSerializedSize());
                        metrics.outboundAppendEventsResponse().mark(states.getSerializedSize());
                    }
                });
            }
        });

    }

    @Override
    public void appendKERL(KERLContext request, StreamObserver<KeyStates> responseObserver) {
        Context timer = metrics != null ? metrics.appendKERLService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundAppendKERLRequest().mark(request.getSerializedSize());
        }
        routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            CompletableFuture<List<KeyState_>> result = s.append(request.getKerl());
            if (result == null) {
                responseObserver.onNext(KeyStates.getDefaultInstance());
                responseObserver.onCompleted();
            } else {
                result.whenComplete((b, t) -> {
                    if (timer != null) {
                        timer.stop();
                    }
                    if (t != null) {
                        responseObserver.onError(t);
                    } else {
                        var results = KeyStates.newBuilder().addAllKeyStates(b).build();
                        responseObserver.onNext(results);
                        responseObserver.onCompleted();
                        metrics.outboundBandwidth().mark(request.getSerializedSize());
                        metrics.outboundAppendKERLResponse().mark(request.getSerializedSize());
                    }
                });
            }
        });
    }

    @Override
    public void appendWithAttachments(KeyEventWitAttachmentsContext request,
                                      StreamObserver<KeyStates> responseObserver) {
        Context timer = metrics != null ? metrics.appendWithAttachmentsService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundAppendWithAttachmentsRequest().mark(request.getSerializedSize());
        }
        routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            CompletableFuture<List<KeyState_>> result = s.append(request.getEventsList(), request.getAttachmentsList());
            if (result == null) {
                responseObserver.onNext(KeyStates.getDefaultInstance());
                responseObserver.onCompleted();
            } else {
                result.whenComplete((ks, t) -> {
                    if (timer != null) {
                        timer.stop();
                    }
                    if (t != null) {
                        responseObserver.onError(t);
                    } else {
                        var states = KeyStates.newBuilder().addAllKeyStates(ks).build();
                        responseObserver.onNext(states);
                        responseObserver.onCompleted();
                        metrics.outboundBandwidth().mark(states.getSerializedSize());
                        metrics.outboundAppendWithAttachmentsResponse().mark(states.getSerializedSize());
                    }
                });
            }
        });

    }

    @Override
    public void getAttachment(EventContext request, StreamObserver<Attachment> responseObserver) {
        Context timer = metrics != null ? metrics.getAttachmentService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundGetAttachmentRequest().mark(request.getSerializedSize());
        }
        routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            Optional<Attachment> response = s.getAttachment(request.getCoordinates());
            if (response.isEmpty()) {
                if (timer != null) {
                    timer.stop();
                }
                responseObserver.onNext(Attachment.getDefaultInstance());
                responseObserver.onCompleted();
            }

            if (timer != null) {
                timer.stop();
                metrics.outboundBandwidth().mark(response.get().getSerializedSize());
                metrics.outboundGetAttachmentResponse().mark(response.get().getSerializedSize());
            }
            responseObserver.onNext(response.isEmpty() ? Attachment.getDefaultInstance() : response.get());
            responseObserver.onCompleted();
        });
    }

    @Override
    public void getKERL(IdentifierContext request, StreamObserver<KERL_> responseObserver) {
        Context timer = metrics != null ? metrics.getKERLService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundGetKERLRequest().mark(request.getSerializedSize());
        }
        routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            Optional<KERL_> response = s.getKERL(request.getIdentifier());
            if (response.isEmpty()) {
                if (timer != null) {
                    timer.stop();
                }
                responseObserver.onNext(KERL_.getDefaultInstance());
                responseObserver.onCompleted();
            }
            var kerl = response.isEmpty() ? KERL_.getDefaultInstance() : response.get();
            if (timer != null) {
                timer.stop();
                metrics.outboundBandwidth().mark(kerl.getSerializedSize());
                metrics.outboundGetKERLResponse().mark(kerl.getSerializedSize());
            }
            responseObserver.onNext(kerl);
            responseObserver.onCompleted();
        });
    }

    @Override
    public void getKeyEvent(EventDigestContext request, StreamObserver<KeyEvent_> responseObserver) {
        Context timer = metrics != null ? metrics.getKeyEventService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundGetKeyEventRequest().mark(request.getSerializedSize());
        }
        routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            Optional<KeyEvent_> response = s.getKeyEvent(request.getDigest());
            if (response.isEmpty()) {
                if (timer != null) {
                    timer.stop();
                }
                responseObserver.onNext(KeyEvent_.getDefaultInstance());
                responseObserver.onCompleted();
            }
            var event = response.isEmpty() ? KeyEvent_.getDefaultInstance() : response.get();
            if (timer != null) {
                timer.stop();
                metrics.outboundBandwidth().mark(event.getSerializedSize());
                metrics.outboundGetKeyEventResponse().mark(event.getSerializedSize());
            }
            responseObserver.onNext(event);
            responseObserver.onCompleted();
        });
    }

    public void getKeyEventCoords(EventContext request, StreamObserver<KeyEvent_> responseObserver) {
        Context timer = metrics != null ? metrics.getKeyEventCoordsService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundGetKeyEventCoordsRequest().mark(request.getSerializedSize());
        }
        routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            Optional<KeyEvent_> response = s.getKeyEvent(request.getCoordinates());
            if (response.isEmpty()) {
                if (timer != null) {
                    timer.stop();
                }
                responseObserver.onNext(KeyEvent_.getDefaultInstance());
                responseObserver.onCompleted();
            }
            var event = response.isEmpty() ? KeyEvent_.getDefaultInstance() : response.get();
            if (timer != null) {
                timer.stop();
                metrics.outboundBandwidth().mark(event.getSerializedSize());
                metrics.outboundGetKeyEventCoordsResponse().mark(event.getSerializedSize());
            }
            responseObserver.onNext(event);
            responseObserver.onCompleted();
        });
    }

    @Override
    public void getKeyState(IdentifierContext request, StreamObserver<KeyState_> responseObserver) {
        Context timer = metrics != null ? metrics.getKeyStateService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundGetKeyStateRequest().mark(request.getSerializedSize());
        }
        routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            Optional<KeyState_> response = s.getKeyState(request.getIdentifier());
            if (response.isEmpty()) {
                if (timer != null) {
                    timer.stop();
                }
                responseObserver.onNext(KeyState_.getDefaultInstance());
                responseObserver.onCompleted();
            }
            var state = response.isEmpty() ? KeyState_.getDefaultInstance() : response.get();
            if (timer != null) {
                timer.stop();
                metrics.outboundBandwidth().mark(state.getSerializedSize());
                metrics.outboundGetKeyStateResponse().mark(state.getSerializedSize());
            }
            responseObserver.onNext(state);
            responseObserver.onCompleted();
        });
    }

    @Override
    public void getKeyStateCoords(EventContext request, StreamObserver<KeyState_> responseObserver) {
        Context timer = metrics != null ? metrics.getKeyStateCoordsService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundGetKeyStateCoordsRequest().mark(request.getSerializedSize());
        }
        routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            Optional<KeyState_> response = s.getKeyState(request.getCoordinates());
            if (response.isEmpty()) {
                if (timer != null) {
                    timer.stop();
                }
                responseObserver.onNext(KeyState_.getDefaultInstance());
                responseObserver.onCompleted();
            }
            var state = response.isEmpty() ? KeyState_.getDefaultInstance() : response.get();
            if (timer != null) {
                timer.stop();
                metrics.outboundBandwidth().mark(state.getSerializedSize());
                metrics.outboundGetKeyStateCoordsResponse().mark(state.getSerializedSize());
            }
            responseObserver.onNext(state);
            responseObserver.onCompleted();
        });
    }
}
