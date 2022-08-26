/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth.grpc.admission.gossip;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.salesfoce.apollo.thoth.proto.AdmissionsGossip;
import com.salesfoce.apollo.thoth.proto.AdmissionsUpdate;
import com.salesfoce.apollo.thoth.proto.Commit;
import com.salesforce.apollo.comm.Link;

/**
 * @author hal.hildebrand
 *
 */
public interface AdmissionReplicationService extends Link {

    ListenableFuture<Empty> commit(Commit commit);

    ListenableFuture<AdmissionsUpdate> gossip(AdmissionsGossip gossip);

    ListenableFuture<Empty> register(AdmissionsUpdate update);

}
