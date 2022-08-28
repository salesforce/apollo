/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth.grpc.admission;

import java.util.concurrent.CompletableFuture;

import com.salesfoce.apollo.thoth.proto.Admittance;
import com.salesfoce.apollo.thoth.proto.Registration;
import com.salesfoce.apollo.thoth.proto.SignedAttestation;
import com.salesfoce.apollo.thoth.proto.SignedNonce;
import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
public interface Admission {
    CompletableFuture<SignedNonce> apply(Registration request, Digest from);

    CompletableFuture<Admittance> register(SignedAttestation request, Digest from);
}
