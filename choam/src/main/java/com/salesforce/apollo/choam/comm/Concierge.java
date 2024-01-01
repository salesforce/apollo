/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.comm;

import com.salesforce.apollo.choam.proto.BlockReplication;
import com.salesforce.apollo.choam.proto.Blocks;
import com.salesforce.apollo.choam.proto.CheckpointReplication;
import com.salesforce.apollo.choam.proto.CheckpointSegments;
import com.salesforce.apollo.choam.proto.Initial;
import com.salesforce.apollo.choam.proto.Synchronize;
import com.salesforce.apollo.choam.proto.ViewMember;
import com.salesforce.apollo.cryptography.Digest;

/**
 * @author hal.hildebrand
 */
public interface Concierge {

    CheckpointSegments fetch(CheckpointReplication request, Digest from);

    Blocks fetchBlocks(BlockReplication request, Digest from);

    Blocks fetchViewChain(BlockReplication request, Digest from);

    ViewMember join(Digest nextView, Digest from);

    Initial sync(Synchronize request, Digest from);

}
