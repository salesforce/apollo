/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost;

import java.util.List;

import com.salesforce.apollo.avro.Entry;
import com.salesforce.apollo.avro.HASH;

/**
 * @author hal.hildebrand
 * @since 220
 */
public interface Store {

	public List<Entry> entriesIn(CombinedIntervals combinedIntervals, List<HASH> have);

	public List<HASH> have(CombinedIntervals keyIntervals);

	public List<HASH> keySet();

	void add(List<Entry> entries, List<HASH> total);

	Entry get(HASH key);

	List<Entry> getUpdates(List<HASH> want);

	void put(HASH key, Entry value);

}
