/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.pal.daemon.grpc;

import com.google.protobuf.Any;
import com.salesfoce.apollo.pal.proto.PalDGrpc.PalDImplBase;
import com.salesforce.apollo.pal.daemon.PalDaemon;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public class PalDaemonService extends PalDImplBase {

    private final PalDaemon daemon;

    public PalDaemonService(PalDaemon daemon) {
        this.daemon = daemon;
    }

    @Override
    public void ping(Any request, StreamObserver<Any> responseObserver) {
        responseObserver.onNext(Any.getDefaultInstance());
        responseObserver.onCompleted();
    }

}
