/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost;

import java.util.List;

import com.salesfoce.apollo.proto.DagEntry;
import com.salesforce.apollo.protocols.HashKey; 

/**
 * @author hal.hildebrand
 * @since 220
 */
public interface Store {

	public List<DagEntry> entriesIn(CombinedIntervals combinedIntervals, List<HashKey> have);

	public List<HashKey> have(CombinedIntervals keyIntervals);

	public List<HashKey> keySet();

	void add(List<DagEntry> entries, List<HashKey> total);

	DagEntry get(HashKey key);

	List<DagEntry> getUpdates(List<HashKey> want);

	void put(HashKey key, DagEntry value);

}
