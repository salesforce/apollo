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
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.proto.EventCoords;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import io.grpc.stub.StreamObserver;
import org.joou.ULong;

/**
 * @author hal.hildebrand
 */
public interface EntranceService {

    KeyState getKeyState(Identifier identifier, ULong uLong, Digest from);

    KeyState getKeyState(EventCoordinates coordinates, Digest from);

    void join(Join request, Digest from, StreamObserver<Gateway> responseObserver, Context timer);

    Redirect seed(Registration request, Digest from);

    Validation validateCoords(EventCoords request, Digest from);
}
