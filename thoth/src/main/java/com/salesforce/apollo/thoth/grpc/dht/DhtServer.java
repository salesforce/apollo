/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth.grpc.dht;

import com.codahale.metrics.Timer.Context;
import com.google.protobuf.Empty;
import com.salesfoce.apollo.stereotomy.event.proto.*;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.*;
import com.salesfoce.apollo.thoth.proto.KerlDhtGrpc.KerlDhtImplBase;
import com.salesforce.apollo.archipelago.RoutableService;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLService;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 */
public class DhtServer extends KerlDhtImplBase {

    private final StereotomyMetrics metrics;
    private final RoutableService<ProtoKERLService> routing;

    public DhtServer(RoutableService<ProtoKERLService> router, StereotomyMetrics metrics) {
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
            if (timer != null) {
                timer.stop();
            }
            if (result != null) {
                responseObserver.onNext(KeyStates.newBuilder().addAllKeyStates(result).build());
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(new StatusRuntimeException(Status.DATA_LOSS));
            }
        });

    }

    @Override
    public void appendAttachments(AttachmentsContext request, StreamObserver<Empty> responseObserver) {
        Context timer = metrics != null ? metrics.appendWithAttachmentsService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundAppendWithAttachmentsRequest().mark(request.getSerializedSize());
        }
        routing.evaluate(responseObserver, s -> {
            var result = s.appendAttachments(request.getAttachmentsList());
            if (result == null) {
                responseObserver.onError(new StatusRuntimeException(Status.DATA_LOSS));
            } else {
                if (timer != null) {
                    timer.stop();
                }
                if (result != null) {
                    responseObserver.onNext(result);
                    responseObserver.onCompleted();
                } else {
                    responseObserver.onError(new StatusRuntimeException(Status.DATA_LOSS));
                }
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
        routing.evaluate(responseObserver, s -> {
            var result = s.append(request.getKerl());
            if (result == null) {
                responseObserver.onError(new StatusRuntimeException(Status.DATA_LOSS));
            } else {
                if (timer != null) {
                    timer.stop();
                }
                responseObserver.onNext(KeyStates.newBuilder().addAllKeyStates(result).build());
                responseObserver.onCompleted();
            }
        });
    }

    @Override
    public void appendValidations(Validations request, StreamObserver<Empty> responseObserver) {
        Context timer = metrics != null ? metrics.appendWithAttachmentsService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundAppendWithAttachmentsRequest().mark(request.getSerializedSize());
        }
        routing.evaluate(responseObserver, s -> {
            var result = s.appendValidations(request);
            if (result == null) {
                responseObserver.onError(new StatusRuntimeException(Status.DATA_LOSS));
            } else {
                if (timer != null) {
                    timer.stop();
                }
                if (result != null) {
                    responseObserver.onNext(result);
                    responseObserver.onCompleted();
                } else {
                    responseObserver.onError(new StatusRuntimeException(Status.DATA_LOSS));
                }
            }
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
            if (result == null) {
                responseObserver.onError(new StatusRuntimeException(Status.DATA_LOSS));
            } else {
                if (timer != null) {
                    timer.stop();
                }
                if (result != null) {
                    responseObserver.onNext(KeyStates.newBuilder().addAllKeyStates(result).build());
                    responseObserver.onCompleted();
                } else {
                    responseObserver.onError(new StatusRuntimeException(Status.DATA_LOSS));
                }
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
                var attachment = response == null ? Attachment.getDefaultInstance() : response;
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
            if (response == null) {
                if (timer != null) {
                    timer.stop();
                }
                responseObserver.onNext(KERL_.getDefaultInstance());
                responseObserver.onCompleted();
            } else {
                if (timer != null) {
                    timer.stop();
                }
                var kerl = response == null ? KERL_.getDefaultInstance() : response;
                responseObserver.onNext(kerl);
                responseObserver.onCompleted();
                if (metrics == null) {
                    final var serializedSize = kerl.getSerializedSize();
                    metrics.outboundBandwidth().mark(serializedSize);
                    metrics.outboundGetKERLResponse().mark(serializedSize);
                }
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
            if (response == null) {
                if (timer != null) {
                    timer.stop();
                }
                responseObserver.onNext(KeyEvent_.getDefaultInstance());
                responseObserver.onCompleted();
            } else {
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
            if (response == null) {
                if (timer != null) {
                    timer.stop();
                }
                responseObserver.onNext(KeyState_.getDefaultInstance());
                responseObserver.onCompleted();
            } else {
                if (timer != null) {
                    timer.stop();
                }
                var state = response == null ? KeyState_.getDefaultInstance() : response;
                responseObserver.onNext(state);
                responseObserver.onCompleted();
                if (metrics == null) {
                    metrics.outboundBandwidth().mark(state.getSerializedSize());
                    metrics.outboundGetKeyStateResponse().mark(state.getSerializedSize());
                }
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
            if (response == null) {
                if (timer != null) {
                    timer.stop();
                }
                responseObserver.onNext(KeyState_.getDefaultInstance());
                responseObserver.onCompleted();
            }
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
        Context timer = metrics != null ? metrics.getKeyStateCoordsService().time() : null;
        if (metrics != null) {
            final var serializedSize = request.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGetKeyStateCoordsRequest().mark(serializedSize);
        }
        routing.evaluate(responseObserver, s -> {
            var response = s.getKeyStateWithAttachments(request);
            if (response == null) {
                if (timer != null) {
                    timer.stop();
                }
                responseObserver.onNext(KeyStateWithAttachments_.getDefaultInstance());
                responseObserver.onCompleted();
            }
            if (timer != null) {
                timer.stop();
            }
            var state = response == null ? KeyStateWithAttachments_.getDefaultInstance() : response;
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
    public void getKeyStateWithEndorsementsAndValidations(EventCoords request,
                                                          StreamObserver<KeyStateWithEndorsementsAndValidations_> responseObserver) {
        Context timer = metrics != null ? metrics.getKeyStateCoordsService().time() : null;
        if (metrics != null) {
            final var serializedSize = request.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGetKeyStateCoordsRequest().mark(serializedSize);
        }
        routing.evaluate(responseObserver, s -> {
            var response = s.getKeyStateWithEndorsementsAndValidations(request);
            if (response == null) {
                if (timer != null) {
                    timer.stop();
                }
                responseObserver.onNext(KeyStateWithEndorsementsAndValidations_.getDefaultInstance());
                responseObserver.onCompleted();
            }
            if (timer != null) {
                timer.stop();
            }
            var state = response == null ? KeyStateWithEndorsementsAndValidations_.getDefaultInstance() : response;
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
    public void getValidations(EventCoords request, StreamObserver<Validations> responseObserver) {
        Context timer = metrics != null ? metrics.getAttachmentService().time() : null;
        if (metrics != null) {
            final var serializedSize = request.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGetAttachmentRequest().mark(serializedSize);
        }
        routing.evaluate(responseObserver, s -> {
            var response = s.getValidations(request);
            if (response == null) {
                if (timer != null) {
                    timer.stop();
                }
                responseObserver.onNext(Validations.getDefaultInstance());
                responseObserver.onCompleted();
            } else {
                if (timer != null) {
                    timer.stop();
                }
                var attachment = response == null ? Validations.getDefaultInstance() : response;
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
}
