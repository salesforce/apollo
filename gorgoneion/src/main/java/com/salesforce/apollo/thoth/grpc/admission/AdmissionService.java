/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth.grpc.admission;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.gorgoneion.proto.Registration;
import com.salesfoce.apollo.gorgoneion.proto.SignedAttestation;
import com.salesfoce.apollo.gorgoneion.proto.SignedNonce;
import com.salesfoce.apollo.stereotomy.event.proto.Validations;
import com.salesforce.apollo.comm.Link;

/**
 * @author hal.hildebrand
 *
 */
public interface AdmissionService extends Link {

    ListenableFuture<SignedNonce> apply(Registration registration);

    ListenableFuture<Validations> register(SignedAttestation attestation);

}
