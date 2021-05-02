/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.comms;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.consortium.proto.BootstrapSync;
import com.salesfoce.apollo.consortium.proto.CheckpointReplication;
import com.salesfoce.apollo.consortium.proto.CheckpointSegments;
import com.salesfoce.apollo.consortium.proto.CheckpointSync;
import com.salesfoce.apollo.consortium.proto.ViewChain;
import com.salesfoce.apollo.consortium.proto.ViewChainSync;

/**
 * @author hal.hildebrand
 *
 */
public interface BootstrapService {

    ListenableFuture<ViewChain> fetchViewChain(ViewChainSync request);

    ListenableFuture<CheckpointSegments> fetch(CheckpointReplication request);

    ListenableFuture<BootstrapSync> checkpointSync(CheckpointSync sync);

}
