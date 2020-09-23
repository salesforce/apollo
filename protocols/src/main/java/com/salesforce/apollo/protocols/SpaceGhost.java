/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.protocols;

import java.util.List;

import com.salesfoce.apollo.proto.DagEntry;
import com.salesfoce.apollo.proto.Interval;

/**
 * @author hal.hildebrand
 * @since 220
 */
public interface SpaceGhost {
    DagEntry get(HashKey key);

    List<DagEntry> intervals(List<Interval> intervals, List<HashKey> have);

    void put(DagEntry entry);
}
