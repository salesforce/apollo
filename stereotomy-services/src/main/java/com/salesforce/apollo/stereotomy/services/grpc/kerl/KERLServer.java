/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc.kerl;

import com.codahale.metrics.Timer.Context;
import com.google.protobuf.Empty;
import com.salesforce.apollo.stereotomy.event.proto.*;
import com.salesforce.apollo.stereotomy.services.grpc.proto.*;
import com.salesforce.apollo.stereotomy.services.grpc.proto.KERLServiceGrpc.KERLServiceImplBase;
import com.salesforce.apollo.archipelago.RoutableService;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLService;
import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 */
public class KERLServer extends KERLServiceImplBase {
    private final StereotomyMetrics                 metrics;
    private final RoutableService<ProtoKERLService> routing;

    public KERLServer(RoutableService<ProtoKERLService> router, StereotomyMetrics metrics) {
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
        routing.evaluate(responseObserver, s -> {
            var result = s.append(request.getKeyEventList());
            if (result == null) {
                responseObserver.onNext(KeyStates.getDefaultInstance());
                responseObserver.onCompleted();
            } else {
                if (timer != null) {
                    timer.stop();
                }
                var states = result == null ? KeyStates.getDefaultInstance()
                                            : KeyStates.newBuilder().addAllKeyStates(result).build();
                responseObserver.onNext(states);
                responseObserver.onCompleted();
                if (metrics != null) {
                    final var serializedSize = states.getSerializedSize();
                    metrics.outboundBandwidth().mark(serializedSize);
                    metrics.outboundAppendEventsResponse().mark(serializedSize);
                }
            }
        });
    }

    @Override
    public void appendAttachments(AttachmentsContext request, StreamObserver<Empty> responseObserver) {
        Context timer = metrics != null ? metrics.appendEventsService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundAppendEventsRequest().mark(request.getSerializedSize());
        }
        routing.evaluate(responseObserver, s -> {
            var result = s.appendAttachments(request.getAttachmentsList());
            if (timer != null) {
                timer.stop();
            }
            responseObserver.onNext(result);
            responseObserver.onCompleted();
        });
    }

    @Override
    public void appendKERL(KERLContext request, StreamObserver<KeyStates> responseObserver) {
        Context timer = metrics != null ? metrics.appendKERLService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundAppendKERLRequest().mark(request.getSerializedSize());
        }
        routing.evaluate(responseObserver, s -> {
            var result = s.append(request.getKerl());
            if (timer != null) {
                timer.stop();
            }
            var results =
            result == null ? KeyStates.getDefaultInstance() : KeyStates.newBuilder().addAllKeyStates(result).build();
            responseObserver.onNext(results);
            responseObserver.onCompleted();
            if (metrics != null) {
                final var serializedSize = results.getSerializedSize();
                metrics.outboundBandwidth().mark(serializedSize);
                metrics.outboundAppendKERLResponse().mark(serializedSize);
            }
        });
    }

    @Override
    public void appendValidations(Validations request, StreamObserver<Empty> responseObserver) {
        Context timer = metrics != null ? metrics.appendEventsService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundAppendEventsRequest().mark(request.getSerializedSize());
        }
        routing.evaluate(responseObserver, s -> {
            var result = s.appendValidations(request);
            if (timer != null) {
                timer.stop();
            }
            responseObserver.onNext(result);
            responseObserver.onCompleted();
        });
    }

    @Override
    public void appendWithAttachments(KeyEventWithAttachmentsContext request,
                                      StreamObserver<KeyStates> responseObserver) {
        Context timer = metrics != null ? metrics.appendWithAttachmentsService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundAppendWithAttachmentsRequest().mark(request.getSerializedSize());
        }
        routing.evaluate(responseObserver, s -> {
            var result = s.append(request.getEventsList(), request.getAttachmentsList());
            if (timer != null) {
                timer.stop();
            }
            var states =
            result == null ? KeyStates.getDefaultInstance() : KeyStates.newBuilder().addAllKeyStates(result).build();
            responseObserver.onNext(states);
            responseObserver.onCompleted();
            if (metrics != null) {
                final var serializedSize = states.getSerializedSize();
                metrics.outboundBandwidth().mark(serializedSize);
                metrics.outboundAppendWithAttachmentsResponse().mark(serializedSize);
            }
        });
    }

    @Override
    public void getAttachment(EventCoords request, StreamObserver<Attachment> responseObserver) {
        Context timer = metrics != null ? metrics.getAttachmentService().time() : null;
        if (metrics != null) {
            final var serializedSize = request.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGetAttachmentRequest().mark(serializedSize);
        }
        routing.evaluate(responseObserver, s -> {
            var response = s.getAttachment(request);
            if (response == null) {
                if (timer != null) {
                    timer.stop();
                }
                responseObserver.onNext(Attachment.getDefaultInstance());
                responseObserver.onCompleted();
            } else {
                if (timer != null) {
                    timer.stop();
                }
                Attachment attachment = response == null ? Attachment.getDefaultInstance() : response;
                responseObserver.onNext(attachment);
                responseObserver.onCompleted();
                if (metrics != null) {
                    final var serializedSize = attachment.getSerializedSize();
                    metrics.outboundBandwidth().mark(serializedSize);
                    metrics.outboundGetAttachmentResponse().mark(serializedSize);
                }
            }
        });
    }

    @Override
    public void getKERL(Ident request, StreamObserver<KERL_> responseObserver) {
        Context timer = metrics != null ? metrics.getKERLService().time() : null;
        if (metrics != null) {
            final var serializedSize = request.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGetKERLRequest().mark(serializedSize);
        }
        routing.evaluate(responseObserver, s -> {
            var response = s.getKERL(request);
            if (timer != null) {
                timer.stop();
            }
            var kerl = response == null ? KERL_.getDefaultInstance() : response;
            responseObserver.onNext(kerl);
            responseObserver.onCompleted();
            if (metrics != null) {
                final var serializedSize = kerl.getSerializedSize();
                metrics.outboundBandwidth().mark(serializedSize);
                metrics.outboundGetKERLResponse().mark(serializedSize);
            }
        });
    }

    @Override
    public void getKeyEventCoords(EventCoords request, StreamObserver<KeyEvent_> responseObserver) {
        Context timer = metrics != null ? metrics.getKeyEventCoordsService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundGetKeyEventCoordsRequest().mark(request.getSerializedSize());
        }
        routing.evaluate(responseObserver, s -> {
            var response = s.getKeyEvent(request);
            if (timer != null) {
                timer.stop();
            }
            var event = response == null ? KeyEvent_.getDefaultInstance() : response;
            responseObserver.onNext(event);
            responseObserver.onCompleted();
            if (metrics != null) {
                final var serializedSize = event.getSerializedSize();
                metrics.outboundBandwidth().mark(serializedSize);
                metrics.outboundGetKeyEventCoordsResponse().mark(serializedSize);
            }
        });
    }

    @Override
    public void getKeyState(Ident request, StreamObserver<KeyState_> responseObserver) {
        Context timer = metrics != null ? metrics.getKeyStateService().time() : null;
        if (metrics != null) {
            final var serializedSize = request.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGetKeyStateRequest().mark(serializedSize);
        }
        routing.evaluate(responseObserver, s -> {
            var response = s.getKeyState(request);
            if (timer != null) {
                timer.stop();
            }
            var state = response == null ? KeyState_.getDefaultInstance() : response;
            responseObserver.onNext(state);
            responseObserver.onCompleted();
            if (metrics != null) {
                metrics.outboundBandwidth().mark(state.getSerializedSize());
                metrics.outboundGetKeyStateResponse().mark(state.getSerializedSize());
            }
        });
    }

    @Override
    public void getKeyStateCoords(EventCoords request, StreamObserver<KeyState_> responseObserver) {
        Context timer = metrics != null ? metrics.getKeyStateCoordsService().time() : null;
        if (metrics != null) {
            final var serializedSize = request.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGetKeyStateCoordsRequest().mark(serializedSize);
        }
        routing.evaluate(responseObserver, s -> {
            var response = s.getKeyState(request);
            if (timer != null) {
                timer.stop();
            }
            var state = response == null ? KeyState_.getDefaultInstance() : response;
            responseObserver.onNext(state);
            responseObserver.onCompleted();
            if (metrics != null) {
                final var serializedSize = state.getSerializedSize();
                metrics.outboundBandwidth().mark(serializedSize);
                metrics.outboundGetKeyStateCoordsResponse().mark(serializedSize);
            }
        });
    }

    @Override
    public void getKeyStateWithAttachments(EventCoords request,
                                           StreamObserver<KeyStateWithAttachments_> responseObserver) {
        Context timer = metrics != null ? metrics.getKeyStateService().time() : null;
        if (metrics != null) {
            final var serializedSize = request.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGetKeyStateRequest().mark(serializedSize);
        }
        routing.evaluate(responseObserver, s -> {
            var response = s.getKeyStateWithAttachments(request);
            if (timer != null) {
                timer.stop();
            }
            var state = response == null ? KeyStateWithAttachments_.getDefaultInstance() : response;
            responseObserver.onNext(state);
            responseObserver.onCompleted();
            if (metrics != null) {
                metrics.outboundBandwidth().mark(state.getSerializedSize());
                metrics.outboundGetKeyStateResponse().mark(state.getSerializedSize());
            }
        });
    }

    @Override
    public void getValidations(EventCoords request, StreamObserver<Validations> responseObserver) {
        Context timer = metrics != null ? metrics.getAttachmentService().time() : null;
        if (metrics != null) {
            final var serializedSize = request.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGetAttachmentRequest().mark(serializedSize);
        }
        routing.evaluate(responseObserver, s -> {
            var response = s.getValidations(request);
            if (timer != null) {
                timer.stop();
            }
            var validations = response == null ? Validations.getDefaultInstance() : response;
            responseObserver.onNext(validations);
            responseObserver.onCompleted();
            if (metrics != null) {
                final var serializedSize = validations.getSerializedSize();
                metrics.outboundBandwidth().mark(serializedSize);
                metrics.outboundGetAttachmentResponse().mark(serializedSize);
            }
        });
    }
}
