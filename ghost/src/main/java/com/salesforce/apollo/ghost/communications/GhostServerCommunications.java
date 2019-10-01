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
import com.salesforce.apollo.avro.HASH;
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
	public List<HASH> interval(HASH from, int ring) {
		return ghost.interval(from, ring);
	}

	@Override
	public void put(Entry value) {
		ghost.put(value);
	}

	@Override
	public List<Entry> satisfy(List<HASH> want) {
		return ghost.satisfy(want);
	}
}
