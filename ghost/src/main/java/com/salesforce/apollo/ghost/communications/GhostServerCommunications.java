/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost.communications;

import java.util.List;

import org.apache.avro.AvroRemoteException;

import com.salesforce.apollo.avro.Entry;
import com.salesforce.apollo.avro.GhostUpdate;
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
    public Entry get(HASH key) throws AvroRemoteException {
        return ghost.get(key);
    }

    @Override
    public GhostUpdate ghostGossip(List<Interval> intervals, List<HASH> digests) throws AvroRemoteException {
        return ghost.gossip(intervals, digests);
    }

    @Override
    public void gUpdate(List<Entry> update) {
        ghost.update(update);
    }

    @Override
    public void put(Entry value) {
        ghost.put(value);
    }

	@Override
	public List<HASH> join(HASH from, int ring) { 
		return ghost.join(from, ring);
	}

	@Override
	public List<Entry> pull(List<HASH> want) { 
		return ghost.pull(want);
	}
}
