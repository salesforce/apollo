/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost.communications;

import java.util.List;

import org.apache.avro.AvroRemoteException;

import com.salesforce.apollo.avro.DagEntry;
import com.salesforce.apollo.avro.HASH;
import com.salesforce.apollo.avro.Interval;
import com.salesforce.apollo.ghost.Ghost.Service;
import com.salesforce.apollo.protocols.SpaceGhost;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class GhostServerCommunications implements SpaceGhost {
    private final Service ghost;

    public GhostServerCommunications(Service ghost) {
        this.ghost = ghost;
    }

    @Override
    public DagEntry get(HASH key) throws AvroRemoteException {
        return ghost.get(key);
    }

    @Override
    public List<DagEntry> intervals(List<Interval> intervals, List<HASH> have) {
        return ghost.intervals(intervals, have);
    }

    @Override
    public void put(DagEntry value) {
        ghost.put(value);
    }

}
