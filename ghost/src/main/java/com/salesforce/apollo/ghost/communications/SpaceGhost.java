/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost.communications;

import java.util.List;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Any;
import com.salesfoce.apollo.ghost.proto.Entries;
import com.salesfoce.apollo.ghost.proto.Interval;
import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 * @since 220
 */
public interface SpaceGhost {
    ListenableFuture<Any> get(Digest key);

    ListenableFuture<Entries> intervals(List<Interval> intervals);

    void put(Any entry);
}
