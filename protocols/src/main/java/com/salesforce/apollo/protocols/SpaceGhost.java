/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.protocols;

import java.util.List;

import com.salesforce.apollo.avro.Entry;
import com.salesforce.apollo.avro.HASH;
import com.salesforce.apollo.avro.Interval;

/**
 * @author hal.hildebrand
 * @since 220
 */
public interface SpaceGhost {
	Entry get(HASH key) throws org.apache.avro.AvroRemoteException;

	List<Entry> intervals(List<Interval> intervals, List<HASH> have);

	void put(Entry entry);
}
