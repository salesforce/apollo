/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc;

import java.util.Optional;

import com.codahale.metrics.Timer.Context;
import com.google.protobuf.Empty;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyState_;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.EventContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.IdentifierContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KERLContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KERLServiceGrpc.KERLServiceImplBase;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KeyEventContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KeyEventWitAttachmentsContext;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KeyStates;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.stereotomy.services.impl.ProtoKERLService;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class KERLServer extends KERLServiceImplBase {
    private final StereotomyMetrics                 metrics;
    private final RoutableService<ProtoKERLService> routing;

    public KERLServer(StereotomyMetrics metrics, RoutableService<ProtoKERLService> router) {
        this.metrics = metrics;
        this.routing = router;
    }

    @Override
    public void append(KeyEventWitAttachmentsContext request, StreamObserver<Empty> responseObserver) {
        Context timer = metrics != null ? metrics.appendService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundAppendRequest().mark(request.getSerializedSize());
        }
        routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            var result = s.append(request.getKeyEvent());
            if (result == null) {
                responseObserver.onNext(Empty.getDefaultInstance());
                responseObserver.onCompleted();
            } else {
                result.whenComplete((b, t) -> {
                    if (timer != null) {
                        timer.stop();
                    }
                    if (t != null) {
                        responseObserver.onError(t);
                    } else {
                        responseObserver.onNext(Empty.getDefaultInstance());
                        responseObserver.onCompleted();
                    }
                });
            }
        });
    }

    @Override
    public void appendWithReturn(KeyEventContext request, StreamObserver<KeyState_> responseObserver) {
        Context timer = metrics != null ? metrics.appendWithReturnService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundAppendWithReturnRequest().mark(request.getSerializedSize());
        }
        routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            var result = s.appendWithReturn(request.getKeyEvent());
            if (result == null) {
                responseObserver.onNext(KeyState_.getDefaultInstance());
                responseObserver.onCompleted();
            } else {
                result.whenComplete((ks, t) -> {
                    if (timer != null) {
                        timer.stop();
                    }
                    if (t != null) {
                        responseObserver.onError(t);
                    } else {
                        responseObserver.onNext(ks);
                        responseObserver.onCompleted();
                        metrics.outboundBandwidth().mark(ks.getSerializedSize());
                        metrics.outboundAppendWithReturnResponse().mark(ks.getSerializedSize());
                    }
                });
            }
        });

    }

    @Override
    public void kerl(IdentifierContext request, StreamObserver<KERL_> responseObserver) {
        Context timer = metrics != null ? metrics.kerlService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundKerlRequest().mark(request.getSerializedSize());
        }
        routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            Optional<KERL_> response = s.kerl(request.getIdentifier());
            if (response.isEmpty()) {
                if (timer != null) {
                    timer.stop();
                }
                responseObserver.onNext(KERL_.getDefaultInstance());
                responseObserver.onCompleted();
            }

            if (timer != null) {
                timer.stop();
                metrics.outboundBandwidth().mark(response.get().getSerializedSize());
                metrics.outboundKerlResponse().mark(response.get().getSerializedSize());
            }
            responseObserver.onNext(response.get());
            responseObserver.onCompleted();
        });
    }

    @Override
    public void publish(KERLContext request, StreamObserver<Empty> responseObserver) {
        Context timer = metrics != null ? metrics.publishService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundPublishRequest().mark(request.getSerializedSize());
        }
        routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            var result = s.publish(request.getKerl());
            if (result == null) {
                responseObserver.onNext(Empty.getDefaultInstance());
                responseObserver.onCompleted();
            } else {
                result.whenComplete((b, t) -> {
                    if (timer != null) {
                        timer.stop();
                    }
                    if (t != null) {
                        responseObserver.onError(t);
                    } else {
                        responseObserver.onNext(Empty.getDefaultInstance());
                        responseObserver.onCompleted();
                    }
                });
            }
        });
    }

    @Override
    public void publishWithReturn(KERLContext request, StreamObserver<KeyStates> responseObserver) {
        Context timer = metrics != null ? metrics.publishWithReturnService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundPublishWithReturnRequest().mark(request.getSerializedSize());
        }
        routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            var result = s.publishWithReturn(request.getKerl());

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
                        var keyStates = KeyStates.newBuilder().addAllKeyStates(ks).build();
                        responseObserver.onNext(keyStates);
                        responseObserver.onCompleted();
                        metrics.outboundBandwidth().mark(keyStates.getSerializedSize());
                        metrics.outboundPublishWithReturnResponse().mark(keyStates.getSerializedSize());
                    }
                });
            }
        });
    }

    @Override
    public void resolve(IdentifierContext request, StreamObserver<KeyState_> responseObserver) {
        Context timer = metrics != null ? metrics.resolveService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundResolveRequest().mark(request.getSerializedSize());
        }
        routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            Optional<KeyState_> response = s.resolve(request.getIdentifier());
            if (response.isEmpty()) {
                if (timer != null) {
                    timer.stop();
                }
                responseObserver.onNext(KeyState_.getDefaultInstance());
                responseObserver.onCompleted();
                return;
            }

            if (timer != null) {
                timer.stop();
                metrics.outboundBandwidth().mark(response.get().getSerializedSize());
                metrics.outboundResolveResponse().mark(response.get().getSerializedSize());
            }
            responseObserver.onNext(response.get());
            responseObserver.onCompleted();
        });

    }

    @Override
    public void resolveCoords(EventContext request, StreamObserver<KeyState_> responseObserver) {
        Context timer = metrics != null ? metrics.resolveCoordsService().time() : null;
        if (metrics != null) {
            metrics.inboundBandwidth().mark(request.getSerializedSize());
            metrics.inboundResolveCoodsRequest().mark(request.getSerializedSize());
        }
        routing.evaluate(responseObserver, Digest.from(request.getContext()), s -> {
            Optional<KeyState_> response = s.resolve(request.getCoordinates());
            if (response.isEmpty()) {
                if (timer != null) {
                    timer.stop();
                }
                responseObserver.onNext(KeyState_.getDefaultInstance());
                responseObserver.onCompleted();
            }

            if (timer != null) {
                timer.stop();
                metrics.outboundBandwidth().mark(response.get().getSerializedSize());
                metrics.outboundResolveCoordsResponse().mark(response.get().getSerializedSize());
            }
            responseObserver.onNext(response.get());
            responseObserver.onCompleted();
        });
    }

}
