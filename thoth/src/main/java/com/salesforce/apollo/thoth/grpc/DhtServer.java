/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth.grpc;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer.Context;
import com.google.protobuf.Empty;
import com.salesfoce.apollo.stereotomy.event.proto.Attachment;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyState_;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.EventContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.IdentifierContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KERLContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KeyEventWitAttachmentsContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KeyEventsContext;
import com.salesfoce.apollo.thoth.proto.KerlDhtGrpc.KerlDhtImplBase;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLService;
import com.salesforce.apollo.utils.Utils;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class DhtServer extends KerlDhtImplBase {
    private final static Logger log = LoggerFactory.getLogger(DhtServer.class);

    private final StereotomyMetrics                 metrics;
    private final RoutableService<ProtoKERLService> routing;
    private final Executor                          exec;

    public DhtServer(RoutableService<ProtoKERLService> router, Executor exec, StereotomyMetrics metrics) {
        this.metrics = metrics;
        this.routing = router;
        this.exec = exec;
    }

    @Override
    public void append(KeyEventsContext request, StreamObserver<Empty> responseObserver) {
        Context timer = metrics != null ? metrics.appendEventsService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundAppendEventsRequest().mark(request.getSerializedSize());
        }
        exec.execute(Utils.wrapped(() -> routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            CompletableFuture<List<KeyState_>> result = s.append(request.getKeyEventList());
            if (result == null) {
                responseObserver.onError(new StatusRuntimeException(Status.DATA_LOSS));
            } else {
                result.whenComplete((ks, t) -> {
                    if (timer != null) {
                        timer.stop();
                    }
                    if (t != null) {
                        final var description = t.getClass().getSimpleName()
                        + (t.getMessage() == null ? "" : "(" + t.getMessage() + ")");
                        responseObserver.onError(new StatusRuntimeException(Status.DATA_LOSS.withDescription(description)));
                    } else if (ks != null) {
                        responseObserver.onNext(Empty.getDefaultInstance());
                        responseObserver.onCompleted();
                    } else {
                        responseObserver.onError(new StatusRuntimeException(Status.DATA_LOSS));
                    }
                });
            }
        }), log));

    }

    @Override
    public void appendKERL(KERLContext request, StreamObserver<Empty> responseObserver) {
        Context timer = metrics != null ? metrics.appendKERLService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundAppendKERLRequest().mark(request.getSerializedSize());
        }
        exec.execute(Utils.wrapped(() -> routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            CompletableFuture<List<KeyState_>> result = s.append(request.getKerl());
            if (result == null) {
                responseObserver.onError(new StatusRuntimeException(Status.DATA_LOSS));
            } else {
                result.whenComplete((b, t) -> {
                    if (timer != null) {
                        timer.stop();
                    }
                    if (t != null) {
                        final var description = t.getClass().getSimpleName()
                        + (t.getMessage() == null ? "" : "(" + t.getMessage() + ")");
                        responseObserver.onError(new StatusRuntimeException(Status.DATA_LOSS.withDescription(description)));
                    } else if (b != null) {
                        responseObserver.onNext(Empty.getDefaultInstance());
                        responseObserver.onCompleted();
                    } else {
                        responseObserver.onError(new StatusRuntimeException(Status.DATA_LOSS));
                    }
                });
            }
        }), log));
    }

    @Override
    public void appendWithAttachments(KeyEventWitAttachmentsContext request, StreamObserver<Empty> responseObserver) {
        Context timer = metrics != null ? metrics.appendWithAttachmentsService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundAppendWithAttachmentsRequest().mark(request.getSerializedSize());
        }
        exec.execute(Utils.wrapped(() -> routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            CompletableFuture<List<KeyState_>> result = s.append(request.getEventsList(), request.getAttachmentsList());
            if (result == null) {
                responseObserver.onError(new StatusRuntimeException(Status.DATA_LOSS));
            } else {
                result.whenComplete((ks, t) -> {
                    if (timer != null) {
                        timer.stop();
                    }
                    if (t != null) {
                        final var description = t.getClass().getSimpleName()
                        + (t.getMessage() == null ? "" : "(" + t.getMessage() + ")");
                        responseObserver.onError(new StatusRuntimeException(Status.DATA_LOSS.withDescription(description)));
                    } else if (ks != null) {
                        responseObserver.onNext(Empty.getDefaultInstance());
                        responseObserver.onCompleted();
                    } else {
                        responseObserver.onError(new StatusRuntimeException(Status.DATA_LOSS));
                    }
                });
            }
        }), log));

    }

    @Override
    public void getAttachment(EventContext request, StreamObserver<Attachment> responseObserver) {
        Context timer = metrics != null ? metrics.getAttachmentService().time() : null;
        if (metrics != null) {
            final var serializedSize = request.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGetAttachmentRequest().mark(serializedSize);
        }
        exec.execute(Utils.wrapped(() -> routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            CompletableFuture<Attachment> response = s.getAttachment(request.getCoordinates());
            if (response == null) {
                if (timer != null) {
                    timer.stop();
                }
                responseObserver.onNext(Attachment.getDefaultInstance());
                responseObserver.onCompleted();
            } else {
                response.whenComplete((attachment, t) -> {
                    if (timer != null) {
                        timer.stop();
                    }
                    if (t != null) {
                        responseObserver.onError(t);
                    } else {
                        attachment = attachment == null ? Attachment.getDefaultInstance() : attachment;
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
        }), log));
    }

    @Override
    public void getKERL(IdentifierContext request, StreamObserver<KERL_> responseObserver) {
        Context timer = metrics != null ? metrics.getKERLService().time() : null;
        if (metrics != null) {
            final var serializedSize = request.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGetKERLRequest().mark(serializedSize);
        }
        exec.execute(Utils.wrapped(() -> routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            CompletableFuture<KERL_> response = s.getKERL(request.getIdentifier());
            if (response == null) {
                if (timer != null) {
                    timer.stop();
                }
                responseObserver.onNext(KERL_.getDefaultInstance());
                responseObserver.onCompleted();
            } else {
                response.whenComplete((kerl, t) -> {
                    if (timer != null) {
                        timer.stop();
                    }
                    if (t != null) {
                        responseObserver.onError(t);
                    } else {
                        kerl = kerl == null ? KERL_.getDefaultInstance() : kerl;
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
        }), log));
    }

    @Override
    public void getKeyEventCoords(EventContext request, StreamObserver<KeyEvent_> responseObserver) {
        Context timer = metrics != null ? metrics.getKeyEventCoordsService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundGetKeyEventCoordsRequest().mark(request.getSerializedSize());
        }
        exec.execute(Utils.wrapped(() -> routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            CompletableFuture<KeyEvent_> response = s.getKeyEvent(request.getCoordinates());
            if (response == null) {
                if (timer != null) {
                    timer.stop();
                }
                responseObserver.onNext(KeyEvent_.getDefaultInstance());
                responseObserver.onCompleted();
            } else {
                response.whenComplete((event, t) -> {
                    if (timer != null) {
                        timer.stop();
                    }
                    if (t != null) {
                        responseObserver.onError(t);
                    } else {
                        event = event == null ? KeyEvent_.getDefaultInstance() : event;
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
        }), log));
    }

    @Override
    public void getKeyState(IdentifierContext request, StreamObserver<KeyState_> responseObserver) {
        Context timer = metrics != null ? metrics.getKeyStateService().time() : null;
        if (metrics != null) {
            final var serializedSize = request.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGetKeyStateRequest().mark(serializedSize);
        }
        exec.execute(Utils.wrapped(() -> routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            CompletableFuture<KeyState_> response = s.getKeyState(request.getIdentifier());
            if (response == null) {
                if (timer != null) {
                    timer.stop();
                }
                responseObserver.onNext(KeyState_.getDefaultInstance());
                responseObserver.onCompleted();
            } else {
                response.whenComplete((state, t) -> {
                    if (timer != null) {
                        timer.stop();
                    }
                    if (t != null) {
                        responseObserver.onError(t);
                    } else {
                        state = state == null ? KeyState_.getDefaultInstance() : state;
                        responseObserver.onNext(state);
                        responseObserver.onCompleted();
                        if (metrics == null) {
                            metrics.outboundBandwidth().mark(state.getSerializedSize());
                            metrics.outboundGetKeyStateResponse().mark(state.getSerializedSize());
                        }
                    }
                });
            }
        }), log));
    }

    @Override
    public void getKeyStateCoords(EventContext request, StreamObserver<KeyState_> responseObserver) {
        Context timer = metrics != null ? metrics.getKeyStateCoordsService().time() : null;
        if (metrics != null) {
            final var serializedSize = request.getSerializedSize();
            metrics.inboundBandwidth().mark(serializedSize);
            metrics.inboundGetKeyStateCoordsRequest().mark(serializedSize);
        }
        exec.execute(Utils.wrapped(() -> routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            CompletableFuture<KeyState_> response = s.getKeyState(request.getCoordinates());
            if (response == null) {
                if (timer != null) {
                    timer.stop();
                }
                responseObserver.onNext(KeyState_.getDefaultInstance());
                responseObserver.onCompleted();
            }
            response.whenComplete((state, t) -> {
                if (timer != null) {
                    timer.stop();
                }
                if (t != null) {
                    responseObserver.onError(t);
                } else {
                    state = state == null ? KeyState_.getDefaultInstance() : state;
                    responseObserver.onNext(state);
                    responseObserver.onCompleted();
                    if (metrics != null) {
                        final var serializedSize = state.getSerializedSize();
                        metrics.outboundBandwidth().mark(serializedSize);
                        metrics.outboundGetKeyStateCoordsResponse().mark(serializedSize);
                    }
                }
            });
        }), log));
    }
}
