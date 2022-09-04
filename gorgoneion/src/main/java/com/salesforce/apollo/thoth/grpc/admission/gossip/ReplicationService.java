/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth.grpc.admission.gossip;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.salesfoce.apollo.gorgoneion.proto.Gossip;
import com.salesfoce.apollo.gorgoneion.proto.Update;
import com.salesforce.apollo.comm.Link;

/**
 * @author hal.hildebrand
 *
 */
public interface ReplicationService extends Link {

    ListenableFuture<Update> gossip(Gossip gossip);

    ListenableFuture<Empty> update(Update update);

}
