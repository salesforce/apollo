/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion.comm.admissions;

import com.codahale.metrics.Timer.Context;
import com.salesfoce.apollo.gorgoneion.proto.Application;
import com.salesfoce.apollo.gorgoneion.proto.Credentials;
import com.salesfoce.apollo.gorgoneion.proto.Invitation;
import com.salesfoce.apollo.gorgoneion.proto.SignedNonce;
import com.salesforce.apollo.crypto.Digest;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public interface AdmissionsService {

    void apply(Application request, Digest from, StreamObserver<SignedNonce> responseObserver, Context timer);

    void register(Credentials request, Digest from, StreamObserver<Invitation> responseObserver, Context timer);

}
