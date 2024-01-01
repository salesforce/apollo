/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.comm.entrance;

import com.codahale.metrics.Timer.Context;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.fireflies.proto.*;
import com.salesforce.apollo.stereotomy.event.proto.EventCoords;
import com.salesforce.apollo.stereotomy.event.proto.IdentAndSeq;
import com.salesforce.apollo.stereotomy.event.proto.KeyState_;
import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 */
public interface EntranceService {

    void join(Join request, Digest from, StreamObserver<Gateway> responseObserver, Context timer);

    KeyState_ keyState(IdentAndSeq request, Digest from);

    Redirect seed(Registration request, Digest from);

    Validation validateCoords(EventCoords request, Digest from);
}
