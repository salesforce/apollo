/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.comm;

import com.salesfoce.apollo.choam.proto.BlockReplication;
import com.salesfoce.apollo.choam.proto.Blocks;
import com.salesfoce.apollo.choam.proto.Certification;
import com.salesfoce.apollo.choam.proto.CheckpointReplication;
import com.salesfoce.apollo.choam.proto.CheckpointSegments;
import com.salesfoce.apollo.choam.proto.Initial;
import com.salesfoce.apollo.choam.proto.JoinRequest;
import com.salesfoce.apollo.choam.proto.SubmitResult;
import com.salesfoce.apollo.choam.proto.SubmitTransaction;
import com.salesfoce.apollo.choam.proto.Synchronize;
import com.salesfoce.apollo.choam.proto.ViewMember;
import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
public interface Concierge {

    CheckpointSegments fetch(CheckpointReplication request, Digest from);

    Blocks fetchBlocks(BlockReplication request, Digest from);

    Blocks fetchViewChain(BlockReplication request, Digest from);

    ViewMember join(JoinRequest request, Digest from);

    Certification join2(JoinRequest request, Digest from);

    SubmitResult submit(SubmitTransaction request, Digest from);

    Initial sync(Synchronize request, Digest from);

}
