/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth.grpc.admission;

import java.util.concurrent.CompletableFuture;

import com.salesfoce.apollo.gorgoneion.proto.Registration;
import com.salesfoce.apollo.gorgoneion.proto.SignedAttestation;
import com.salesfoce.apollo.gorgoneion.proto.SignedNonce;
import com.salesfoce.apollo.stereotomy.event.proto.Validations;
import com.salesforce.apollo.crypto.Digest;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 *
 */
public interface Admission {
    CompletableFuture<SignedNonce> apply(Registration request, Digest from);

    void register(SignedAttestation request, Digest from, StreamObserver<Validations> responseObserver);
}
