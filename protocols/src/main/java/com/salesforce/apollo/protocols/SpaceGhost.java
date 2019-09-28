/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.protocols;

import java.util.List;

import org.apache.avro.AvroRemoteException;

import com.salesforce.apollo.avro.Entry;
import com.salesforce.apollo.avro.GhostUpdate;
import com.salesforce.apollo.avro.Interval;
import com.salesforce.apollo.avro.HASH;

/**
 * @author hal.hildebrand
 * @since 220
 */
public interface SpaceGhost {
	Entry get(HASH key) throws org.apache.avro.AvroRemoteException;

	GhostUpdate ghostGossip(List<Interval> intervals, List<HASH> digests) throws AvroRemoteException;

	void gUpdate(java.util.List<Entry> updates);

	List<HASH> join(HASH from, int ring);

	List<Entry> pull(List<HASH> want);

	void put(Entry entry);
}
