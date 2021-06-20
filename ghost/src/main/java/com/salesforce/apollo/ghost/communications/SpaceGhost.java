/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost.communications;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Any;
import com.salesfoce.apollo.ghost.proto.Entries;
import com.salesfoce.apollo.ghost.proto.Entry;
import com.salesfoce.apollo.ghost.proto.Get;
import com.salesfoce.apollo.ghost.proto.Intervals;
import com.salesforce.apollo.comm.Link;

/**
 * @author hal.hildebrand
 * @since 220
 */
public interface SpaceGhost extends Link {
    ListenableFuture<Any> get(Get key);

    ListenableFuture<Entries> intervals(Intervals intervals);

    void put(Entry value);
}
