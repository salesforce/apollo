/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.comm.entrance;

import com.codahale.metrics.Timer.Context;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.fireflies.proto.Gateway;
import com.salesforce.apollo.fireflies.proto.Join;
import com.salesforce.apollo.fireflies.proto.Redirect;
import com.salesforce.apollo.fireflies.proto.Registration;
import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 */
public interface EntranceService {

    void join(Join request, Digest from, StreamObserver<Gateway> responseObserver, Context timer);

    Redirect seed(Registration request, Digest from);
}
