/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion.comm.gather;

import com.codahale.metrics.Timer.Context;
import com.salesfoce.apollo.gorgoneion.proto.EndorseNonce;
import com.salesfoce.apollo.stereotomy.event.proto.Validation_;
import com.salesforce.apollo.crypto.Digest;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public interface EndorsementService {

    void endorse(EndorseNonce request, Digest from, StreamObserver<Validation_> responseObserver, Context timer);

}
